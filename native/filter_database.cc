// Implements filtering for meds_reader databases by subject id.
//
// This file loads a list of subject ids, maps them to offsets in an
// existing database, and copies only the referenced slices of each
// property file into a new database directory. It preserves metadata
// and rebuilds subject id/length tables to match the filtered subset.
#include "filter_database.hh"

#include <algorithm>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <string>
#include <thread>

#include "binary_version.hh"
#include "mmap_file.hh"
#include "pdqsort.h"

namespace {

// Copies selected entries from a byte-offset table into a new file.
void copy_subset(const std::filesystem::path& source_path,
                 const std::filesystem::path& destination_path,
                 const std::vector<size_t>& offsets) {
    MmapFile source(source_path);

    absl::Span<const uint64_t> byte_offsets = source.data<uint64_t>();

    std::string_view data = source.bytes();

    std::ofstream destination(destination_path, std::ios_base::out |
                                                    std::ios_base::binary |
                                                    std::ios_base::trunc);

    std::vector<const char*> result_pointers;
    result_pointers.reserve(offsets.size());

    std::vector<uint64_t> result_byte_offsets;
    result_byte_offsets.reserve(offsets.size() + 1);

    uint64_t current_offset = sizeof(uint64_t) * (offsets.size() + 1);

    for (size_t offset : offsets) {
        uint64_t start = byte_offsets[offset];
        uint64_t end = byte_offsets[offset + 1];

        result_byte_offsets.push_back(current_offset);
        result_pointers.push_back(data.data() + start);

        current_offset += (end - start);
    }

    result_byte_offsets.push_back(current_offset);

    destination.write((const char*)result_byte_offsets.data(),
                      sizeof(uint64_t) * result_byte_offsets.size());

    for (size_t i = 0; i < result_pointers.size(); i++) {
        uint64_t length = result_byte_offsets[i + 1] - result_byte_offsets[i];
        destination.write(result_pointers[i], length);
    }
}

// Copies a property directory for selected subject offsets.
void filter_database_property(const std::filesystem::path& source_path,
                              const std::filesystem::path& destination_path,
                              const std::vector<size_t>& offsets,
                              const std::string& property_name) {
    std::filesystem::path source_property_path = source_path / property_name;
    std::filesystem::path destination_property_path =
        destination_path / property_name;

    std::filesystem::create_directory(destination_property_path);

    std::filesystem::path source_dictionary_path =
        source_property_path / "dictionary";

    if (std::filesystem::exists(source_dictionary_path)) {
        std::filesystem::copy(source_dictionary_path,
                              destination_property_path / "dictionary");
    }

    std::filesystem::copy(source_property_path / "zdict",
                          destination_property_path / "zdict");

    copy_subset(source_property_path / "data",
                destination_property_path / "data", offsets);
}

}  // namespace

// Filters a meds_reader database to the subject ids in a file.
void filter_database(const char* source, const char* destination,
                     const char* subject_ids_file, int num_threads) {
    std::filesystem::path source_path(source);
    std::filesystem::path destination_path(destination);

    MmapFile subject_ids_data{std::string(subject_ids_file)};

    absl::Span<const int64_t> unsorted_subject_ids =
        subject_ids_data.data<int64_t>();

    std::vector<int64_t> sorted_subject_ids(std::begin(unsorted_subject_ids),
                                            std::end(unsorted_subject_ids));

    pdqsort(std::begin(sorted_subject_ids), std::end(sorted_subject_ids));
    sorted_subject_ids.erase(
        std::unique(std::begin(sorted_subject_ids),
                    std::end(sorted_subject_ids)),
        std::end(sorted_subject_ids));

    absl::Span<const int64_t> subject_ids(sorted_subject_ids.data(),
                                          sorted_subject_ids.size());

    std::filesystem::create_directory(destination_path);

    std::filesystem::copy(source_path / "metadata",
                          destination_path / "metadata");

    if (subject_ids.empty()) {
        std::ofstream empty_marker(destination_path / "meds_reader.empty");
        empty_marker.exceptions(std::ofstream::badbit | std::ofstream::failbit);
        empty_marker << "empty\n";
        empty_marker.close();
    } else {
        {
            std::ofstream subject_ids_file(
                destination_path / "subject_id",
                std::ios_base::out | std::ios_base::binary |
                    std::ios_base::trunc);
            subject_ids_file.write((const char*)subject_ids.data(),
                                   sizeof(int64_t) * subject_ids.size());
        }
    }

    std::vector<size_t> offsets;
    offsets.reserve(subject_ids.size());

    {
        MmapFile source_subject_ids_file(source_path / "subject_id");
        absl::Span<const int64_t> source_subject_ids =
            source_subject_ids_file.data<int64_t>();

        if (!subject_ids.empty()) {
            auto first = std::lower_bound(
                std::begin(source_subject_ids), std::end(source_subject_ids),
                subject_ids.front());
            auto last = std::upper_bound(
                std::begin(source_subject_ids), std::end(source_subject_ids),
                subject_ids.back());

            for (int64_t subject_id : subject_ids) {
                auto iter = std::lower_bound(first, last, subject_id);
                if (iter == last || *iter != subject_id) {
                    throw std::runtime_error(
                        std::string("Could not find subject_id ") +
                        std::to_string(subject_id) + " in database");
                }

                offsets.push_back(iter - std::begin(source_subject_ids));

                first = ++iter;
            }
        }
    }

    {
        MmapFile source_subject_lengths_file(source_path /
                                             "meds_reader.length");
        absl::Span<const uint32_t> source_subject_lengths =
            source_subject_lengths_file.data<uint32_t>();
        std::vector<uint32_t> subject_lengths;
        subject_lengths.reserve(subject_ids.size());

        for (size_t offset : offsets) {
            subject_lengths.push_back(source_subject_lengths[offset]);
        }

        if (!subject_lengths.empty()) {
            std::ofstream subject_lengths_file(
                destination_path / "meds_reader.length",
                std::ios_base::out | std::ios_base::binary |
                    std::ios_base::trunc);
            subject_lengths_file.write(
                (const char*)subject_lengths.data(),
                sizeof(uint32_t) * subject_lengths.size());
        }
    }

    std::vector<std::string> properties;

    {
        MmapFile property_file(source_path / "meds_reader.properties");

        const char* data = property_file.bytes().data();
        const char* end =
            property_file.bytes().data() + property_file.bytes().size();

        while (data != end) {
            size_t name_length = *(const size_t*)data;
            data += sizeof(size_t);
            properties.push_back(std::string(data, name_length));
            data += name_length;
            data += sizeof(int64_t);

            std::cout << "Got property " << properties.back() << std::endl;
        }
    }

    properties.push_back("meds_reader.null_map");

    for (const auto& property : properties) {
        filter_database_property(source_path, destination_path, offsets,
                                 property);
    }

    std::filesystem::copy(source_path / "meds_reader.properties",
                          destination_path / "meds_reader.properties");

    {
        std::ofstream version_file(destination_path / "meds_reader.version");
        version_file.exceptions(std::ofstream::badbit |
                                std::ofstream::failbit);
        version_file << CURRENT_BINARY_VERSION << std::endl;
        version_file.close();
    }
}
