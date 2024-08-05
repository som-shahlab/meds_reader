#include "create_database.hh"

#define ZSTD_STATIC_LINKING_ONLY

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include <bitset>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <queue>
#include <set>
#include <string>
#include <thread>

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/types/optional.h"
#include "arrow/array/array_binary.h"
#include "arrow/array/array_primitive.h"
#include "arrow/array/builder_binary.h"
#include "arrow/array/builder_nested.h"
#include "arrow/array/builder_primitive.h"
#include "arrow/io/file.h"
#include "arrow/memory_pool.h"
#include "arrow/record_batch.h"
#include "arrow/table.h"
#include "arrow/util/type_fwd.h"
#include "binary_version.hh"
#include "blockingconcurrentqueue.h"
#include "dtype.hh"
#include "lightweightsemaphore.h"
#include "mmap_file.hh"
#include "parquet/arrow/reader.h"
#include "parquet/arrow/schema.h"
#include "parquet/arrow/writer.h"
#include "pdqsort.h"
#include "streamvbyte.h"
#include "zdict.h"
#include "zstd.h"

namespace {

constexpr size_t COMPRESSION_BUFFER_SIZE =
    1 * 1000 * 1000;                                   // Roughly 1 megabyte
constexpr size_t PIECE_SIZE = 1 * 1000 * 1000 * 1000;  // Roughly 1 gigabyte
constexpr int QUEUE_SIZE = 1000;
constexpr ssize_t SEMAPHORE_BLOCK_SIZE = 100;

template <typename T>
void add_literal_to_vector(std::vector<char>& data, T to_add) {
    const char* bytes = reinterpret_cast<const char*>(&to_add);
    data.insert(std::end(data), bytes, bytes + sizeof(T));
}

void add_string_to_vector(std::vector<char>& data, std::string_view to_add) {
    add_literal_to_vector(data, to_add.size());
    data.insert(std::end(data), std::begin(to_add), std::end(to_add));
}

class ZstdRowWriter {
   public:
    ZstdRowWriter(const std::string& path, ZSTD_CCtx* ctx)
        : fname(path),
          fstream(path, std::ifstream::out | std::ifstream::binary),
          context(ctx) {}

    void add_next(std::string_view data, uint64_t count) {
        add_literal_to_vector(uncompressed_buffer, count);
        add_string_to_vector(uncompressed_buffer,
                             std::string_view(data.data(), data.size()));

        if (uncompressed_buffer.size() > COMPRESSION_BUFFER_SIZE) {
            flush_compressed();
        }
    }

    ~ZstdRowWriter() {
        if (uncompressed_buffer.size() > 0) {
            flush_compressed();
        }
    }

    const std::string fname;

   private:
    void flush_compressed() {
        size_t needed_size = ZSTD_compressBound(uncompressed_buffer.size());

        if (compressed_buffer.size() < needed_size) {
            compressed_buffer.resize(needed_size * 2);
        }

        size_t compressed_length = ZSTD_compressCCtx(
            context, compressed_buffer.data(), compressed_buffer.size(),
            uncompressed_buffer.data(), uncompressed_buffer.size(), 1);

        if (ZSTD_isError(compressed_length)) {
            throw std::runtime_error("Could not compress using zstd?");
        }

        fstream.write(reinterpret_cast<char*>(&compressed_length),
                      sizeof(compressed_length));
        fstream.write(compressed_buffer.data(), compressed_length);

        uncompressed_buffer.clear();
    }

    std::ofstream fstream;

    ZSTD_CCtx* context;

    std::vector<char> compressed_buffer;
    std::vector<char> uncompressed_buffer;
};

class ZstdRowReader {
   public:
    ZstdRowReader(const std::string& path, ZSTD_DCtx* ctx)
        : fname(path),
          fstream(path, std::ifstream::in | std::ifstream::binary),
          context(ctx),
          current_offset(0),
          uncompressed_size(0) {}

    absl::optional<std::tuple<std::string_view, uint64_t>> get_next() {
        if (current_offset == uncompressed_size) {
            bool could_load_more = try_to_load_more_data();

            if (!could_load_more) {
                return {};
            }

            assert(current_offset < uncompressed_size);
        }

        assert(compressed_buffer.size() >= sizeof(size_t));

        uint64_t count = *reinterpret_cast<const uint64_t*>(
            uncompressed_buffer.data() + current_offset);
        current_offset += sizeof(count);

        size_t size = *reinterpret_cast<const size_t*>(
            uncompressed_buffer.data() + current_offset);
        current_offset += sizeof(size);

        std::string_view data(uncompressed_buffer.data() + current_offset,
                              size);
        current_offset += size;

        assert(data.size() >= 0);
        assert(data.data() != nullptr);

        return std::make_tuple(data, count);
    }

   private:
    bool try_to_load_more_data() {
        if (fstream.eof()) {
            return false;
        }

        size_t size;
        fstream.read(reinterpret_cast<char*>(&size), sizeof(size));

        if (fstream.eof()) {
            return false;
        }

        if (compressed_buffer.size() < size) {
            compressed_buffer.resize(size * 2);
        }

        fstream.read(compressed_buffer.data(), size);

        uncompressed_size =
            ZSTD_getFrameContentSize(compressed_buffer.data(), size);

        if (uncompressed_size == ZSTD_CONTENTSIZE_ERROR ||
            uncompressed_size == ZSTD_CONTENTSIZE_UNKNOWN) {
            throw std::runtime_error(
                "Could not get the size of the zstd compressed stream?");
        }

        if (uncompressed_buffer.size() < uncompressed_size) {
            uncompressed_buffer.resize(uncompressed_size * 2);
        }

        size_t read_size = ZSTD_decompressDCtx(
            context, uncompressed_buffer.data(), uncompressed_size,
            compressed_buffer.data(), size);

        if (ZSTD_isError(read_size) || read_size != uncompressed_size) {
            throw std::runtime_error("Could not decompress zstd data?");
        }

        current_offset = 0;
        return true;
    }

    const std::string fname;
    std::ifstream fstream;

    ZSTD_DCtx* context;

    std::vector<char> compressed_buffer;
    std::vector<char> uncompressed_buffer;
    size_t current_offset;
    size_t uncompressed_size;
};

std::map<std::string, std::pair<std::shared_ptr<arrow::DataType>, int64_t>>
get_properties(const parquet::arrow::SchemaManifest& manifest) {
    std::map<std::string, std::pair<std::shared_ptr<arrow::DataType>, int64_t>>
        result;

    std::queue<parquet::arrow::SchemaField> to_process;
    for (const auto& field : manifest.schema_fields) {
        to_process.emplace(std::move(field));
    }

    auto helper = [&](parquet::arrow::SchemaField& field) {
        if (!field.is_leaf()) {
            throw std::runtime_error(
                "meds_reader only supports leaf properties");
        }
        result[field.field->name()] =
            std::make_pair(field.field->type(), field.column_index);
    };

    while (!to_process.empty()) {
        parquet::arrow::SchemaField next = std::move(to_process.front());
        to_process.pop();
        helper(next);
    }

    return result;
}

std::map<std::string, std::shared_ptr<arrow::DataType>> get_property_types(
    const parquet::arrow::SchemaManifest& manifest) {
    std::map<std::string, std::shared_ptr<arrow::DataType>> result;

    for (const auto& entry : get_properties(manifest)) {
        result[entry.first] = entry.second.first;
    }

    return result;
}

bool are_maps_equivalent(
    const std::map<std::string, std::shared_ptr<arrow::DataType>>& a,
    const std::map<std::string, std::shared_ptr<arrow::DataType>>& b) {
    if (a.size() != b.size()) {
        return false;
    }

    for (const auto& entry : a) {
        auto item = b.find(entry.first);
        if (item == std::end(b)) {
            return false;
        }

        if (!entry.second->Equals(item->second)) {
            return false;
        }
    }

    return true;
}

typedef std::pair<std::filesystem::path, size_t> WorkEntry;

std::pair<std::vector<std::pair<std::string, std::shared_ptr<arrow::DataType>>>,
          std::vector<WorkEntry>>
read_files(std::filesystem::path root_directory, int num_threads) {
    std::map<std::string, std::shared_ptr<arrow::DataType>> properties;

    std::vector<std::pair<std::filesystem::path, size_t>> work_entries;

    for (const auto& dir_entry :
         std::filesystem::recursive_directory_iterator(root_directory)) {
        if (dir_entry.path().extension() != ".parquet") {
            continue;
        }
        arrow::MemoryPool* pool = arrow::default_memory_pool();

        // Configure general Parquet reader settings
        auto reader_properties = parquet::ReaderProperties(pool);
        reader_properties.set_buffer_size(1024 * 1024);
        reader_properties.enable_buffered_stream();

        // Configure Arrow-specific Parquet reader settings
        auto arrow_reader_props = parquet::ArrowReaderProperties();
        arrow_reader_props.set_batch_size(128 * 1024);  // default 64 * 1024

        parquet::arrow::FileReaderBuilder reader_builder;
        PARQUET_THROW_NOT_OK(reader_builder.OpenFile(dir_entry.path().string(),
                                                     /*memory_map=*/false,
                                                     reader_properties));
        reader_builder.memory_pool(pool);
        reader_builder.properties(arrow_reader_props);

        std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
        PARQUET_ASSIGN_OR_THROW(arrow_reader, reader_builder.Build());

        if (properties.empty()) {
            properties = get_property_types(arrow_reader->manifest());
        } else {
            auto temp_properties = get_property_types(arrow_reader->manifest());
            if (!are_maps_equivalent(properties, temp_properties)) {
                throw std::runtime_error("Properties did not match up");
            }
        }

        size_t index = work_entries.size();
        work_entries.push_back(std::make_pair(dir_entry.path(), index));
    }

    properties.erase("patient_id");

    std::vector<std::pair<std::string, std::shared_ptr<arrow::DataType>>>
        properties_list(std::begin(properties), std::end(properties));

    return {properties_list, work_entries};
}

std::set<std::string> known_properties = {"code", "numeric_value"};

template <typename F, typename A>
void iterate_strings_helper(
    const std::filesystem::path& filename, const std::string& property_name,
    const std::vector<uint32_t>& patient_lengths,
    const absl::flat_hash_map<std::string, size_t>& dictionary_entries,
    F func) {
    arrow::MemoryPool* pool = arrow::default_memory_pool();

    // Configure general Parquet reader settings
    auto reader_properties = parquet::ReaderProperties(pool);
    reader_properties.set_buffer_size(1024 * 1024);
    reader_properties.enable_buffered_stream();

    // Configure Arrow-specific Parquet reader settings
    auto arrow_reader_props = parquet::ArrowReaderProperties();
    arrow_reader_props.set_batch_size(128 * 1024);  // default 64 * 1024

    parquet::arrow::FileReaderBuilder reader_builder;
    PARQUET_THROW_NOT_OK(reader_builder.OpenFile(filename, /*memory_map=*/false,
                                                 reader_properties));
    reader_builder.memory_pool(pool);
    reader_builder.properties(arrow_reader_props);

    std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
    PARQUET_ASSIGN_OR_THROW(arrow_reader, reader_builder.Build());

    auto properties = get_properties(arrow_reader->manifest());

    int64_t column = properties.find(property_name)->second.second;

    std::vector<int> columns = {(int)column};

    size_t next_patient_index = 0;
    size_t remaining_events;
    bool has_event = false;

    absl::flat_hash_map<std::string, uint32_t> per_patient_values;

    std::vector<char> null_bytes;
    std::vector<uint32_t> text_value_lengths;
    std::vector<char> text_value_bytes;
    std::vector<uint32_t> values;
    size_t offset_in_bitset;
    std::bitset<sizeof(uint64_t) * 8> bitset;
    uint64_t* nullmap_offset;

    auto flush = [&]() {
        *nullmap_offset++ = bitset.to_ulong();
        offset_in_bitset = 0;
        bitset.reset();
    };

    auto flush_patient = [&]() {
        if (offset_in_bitset != 0) {
            flush();
        }

        text_value_lengths[0] = text_value_lengths.size() - 1;
        text_value_lengths.insert(std::end(text_value_lengths),
                                  std::begin(values), std::end(values));

        std::vector<char> helper(
            sizeof(uint32_t) +
            streamvbyte_max_compressedbytes(text_value_lengths.size()));

        size_t count = streamvbyte_encode(
            text_value_lengths.data(), text_value_lengths.size(),
            (uint8_t*)helper.data() + sizeof(uint32_t));

        uint32_t* length_pointer = (uint32_t*)helper.data();
        *length_pointer = text_value_lengths.size();

        helper.resize(sizeof(uint32_t) + count);

        helper.insert(std::end(helper), std::begin(text_value_bytes),
                      std::end(text_value_bytes));

        null_bytes.insert(std::end(null_bytes), std::begin(helper),
                          std::end(helper));

        func(null_bytes);
    };

    auto write_null = [&]() {
        offset_in_bitset++;
        if (offset_in_bitset == bitset.size()) {
            flush();
        }
    };

    auto write_value = [&](std::string_view text) {
        auto iter = dictionary_entries.find(text);

        if (iter != std::end(dictionary_entries)) {
            values.push_back(iter->second);
        } else {
            auto custom = per_patient_values.try_emplace(
                text, per_patient_values.size() + dictionary_entries.size());

            if (custom.second) {
                text_value_lengths.push_back(text.size());
                text_value_bytes.insert(std::end(text_value_bytes),
                                        std::begin(text), std::end(text));
            }
            values.push_back(custom.first->second);
        }

        bitset.set(offset_in_bitset);
        offset_in_bitset++;
        if (offset_in_bitset == bitset.size()) {
            flush();
        }
    };

    auto add_value = [&](std::string_view value) {
        if (!has_event || (remaining_events == 0)) {
            if (has_event) {
                flush_patient();
            } else {
                has_event = true;
            }

            remaining_events = patient_lengths[next_patient_index++];

            per_patient_values.clear();
            null_bytes.clear();
            text_value_lengths.clear();
            text_value_lengths.push_back(0);

            text_value_bytes.clear();
            values.clear();
            offset_in_bitset = 0;
            bitset.reset();

            auto num_null_bytes =
                (remaining_events + bitset.size() - 1) / bitset.size();

            null_bytes.resize(num_null_bytes * sizeof(uint64_t));

            nullmap_offset = (uint64_t*)null_bytes.data();
        }

        if (value.empty()) {
            write_null();
        } else {
            write_value(value);
        }

        remaining_events--;
    };

    for (int64_t row_group = 0; row_group < arrow_reader->num_row_groups();
         row_group++) {
        std::shared_ptr<arrow::Table> table;
        PARQUET_THROW_NOT_OK(
            arrow_reader->ReadRowGroup(row_group, columns, &table));

        auto chunked_values = table->GetColumnByName(property_name);

        for (const auto& array : chunked_values->chunks()) {
            auto string_array = std::dynamic_pointer_cast<A>(array);
            if (string_array == nullptr) {
                throw std::runtime_error("Could not cast property");
            }

            for (int64_t i = 0; i < string_array->length(); i++) {
                if (string_array->IsNull(i)) {
                    add_value(std::string_view());
                } else {
                    std::string_view item = string_array->GetView(i);
                    add_value(item);
                }
            }
        }
    }

    if (has_event) {
        flush_patient();
    }
}

template <typename F>
void iterate_strings(
    const std::filesystem::path& filename, const std::string& property_name,

    const std::shared_ptr<arrow::DataType>& type,

    const std::vector<uint32_t>& patient_lengths,
    const absl::flat_hash_map<std::string, size_t>& dictionary_entries,
    F func) {
    switch (type->id()) {
        case arrow::Type::STRING:
            iterate_strings_helper<F, arrow::StringArray>(
                filename, property_name, patient_lengths, dictionary_entries,
                func);
            break;

        case arrow::Type::LARGE_STRING:
            iterate_strings_helper<F, arrow::LargeStringArray>(
                filename, property_name, patient_lengths, dictionary_entries,
                func);
            break;

        default:
            throw std::runtime_error("Unsupported type " + type->ToString());
    };
}

std::vector<std::vector<char>> get_samples(
    std::filesystem::path filename, std::string property_name,
    const std::shared_ptr<arrow::DataType>& type,
    const std::vector<uint32_t>& patient_lengths,
    const absl::flat_hash_map<std::string, size_t>& dictionary_entries,
    size_t num_samples) {
    size_t sample_count = 0;
    std::vector<std::vector<char>> samples;

    iterate_strings(filename, property_name, type, patient_lengths,
                    dictionary_entries, [&](std::vector<char> bytes) {
                        sample_count++;

                        if (samples.size() < num_samples) {
                            samples.emplace_back(std::move(bytes));
                        } else {
                            size_t j = (size_t)(rand() % sample_count);
                            if (j < num_samples) {
                                samples[j] = std::move(bytes);
                            }
                        }
                    });

    return samples;
}

std::pair<size_t, std::vector<uint64_t>> write_files(
    std::filesystem::path filename, std::string property_name,
    const std::shared_ptr<arrow::DataType>& type,
    const std::vector<uint32_t>& patient_lengths,
    const absl::flat_hash_map<std::string, size_t>& dictionary_entries,
    const std::vector<char>& dictionary,
    std::filesystem::path target_filename) {
    std::vector<uint64_t> offsets;

    std::ofstream output_file(target_filename, std::ios_base::out |
                                                   std::ios_base::binary |
                                                   std::ios_base::trunc);

    uint64_t num_bytes = 0;

    auto context_deleter = [](ZSTD_CCtx* context) { ZSTD_freeCCtx(context); };

    std::unique_ptr<ZSTD_CCtx, decltype(context_deleter)> context2{
        ZSTD_createCCtx(), context_deleter};

    size_t res =
        ZSTD_CCtx_setParameter(context2.get(), ZSTD_c_compressionLevel, 22);

    if (ZSTD_isError(res)) {
        throw std::runtime_error("Could not set the compression level");
    }

    res = ZSTD_CCtx_loadDictionary(context2.get(), dictionary.data(),
                                   dictionary.size());
    if (ZSTD_isError(res)) {
        throw std::runtime_error("Could not load the dictionary");
    }

    iterate_strings(
        filename, property_name, type, patient_lengths, dictionary_entries,
        [&](std::vector<char> bytes) {
            std::vector<char> final_bytes(sizeof(uint32_t) +
                                          ZSTD_compressBound(bytes.size()));
            res = ZSTD_compress2(
                context2.get(), final_bytes.data() + sizeof(uint32_t),
                final_bytes.size(), bytes.data(), bytes.size());

            if (ZSTD_isError(res)) {
                throw std::runtime_error("Unable to compress");
            }

            final_bytes.resize(res + sizeof(uint32_t));

            uint32_t* length_pointer = (uint32_t*)final_bytes.data();
            *length_pointer = bytes.size();

            offsets.push_back(num_bytes);

            output_file.write(final_bytes.data(), final_bytes.size());

            num_bytes += final_bytes.size();
        });

    return std::make_pair(num_bytes, std::move(offsets));
}

template <typename A>
void string_reader_thread_helper(
    const std::filesystem::path& filename, const std::string& property_name,
    const std::vector<uint32_t>& patient_lengths,
    std::vector<
        moodycamel::BlockingConcurrentQueue<std::pair<std::string, uint64_t>>>&
        all_write_queues,
    moodycamel::LightweightSemaphore& queue_semaphore) {
    arrow::MemoryPool* pool = arrow::default_memory_pool();

    // Configure general Parquet reader settings
    auto reader_properties = parquet::ReaderProperties(pool);
    reader_properties.set_buffer_size(1024 * 1024);
    reader_properties.enable_buffered_stream();

    // Configure Arrow-specific Parquet reader settings
    auto arrow_reader_props = parquet::ArrowReaderProperties();
    arrow_reader_props.set_batch_size(128 * 1024);  // default 64 * 1024

    parquet::arrow::FileReaderBuilder reader_builder;
    PARQUET_THROW_NOT_OK(reader_builder.OpenFile(filename, /*memory_map=*/false,
                                                 reader_properties));
    reader_builder.memory_pool(pool);
    reader_builder.properties(arrow_reader_props);

    std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
    PARQUET_ASSIGN_OR_THROW(arrow_reader, reader_builder.Build());

    auto properties = get_properties(arrow_reader->manifest());

    int64_t column = properties.find(property_name)->second.second;
    std::vector<int> columns = {(int)column};

    ssize_t slots_to_write = queue_semaphore.waitMany(SEMAPHORE_BLOCK_SIZE);

    absl::flat_hash_map<std::string, uint64_t> items;

    size_t next_patient_index = 0;
    size_t remaining_events;
    bool has_event = false;

    auto flush_patient = [&]() {
        for (auto& item : items) {
            size_t h = std::hash<std::string>{}(item.first);
            size_t partition = h % all_write_queues.size();

            item.second += (((uint64_t)1) << 32);

            if (slots_to_write == 0) {
                slots_to_write = queue_semaphore.waitMany(SEMAPHORE_BLOCK_SIZE);
            }
            slots_to_write--;

            all_write_queues[partition].enqueue(std::move(item));
        }
    };

    auto add_value = [&](std::string_view item) {
        if (!has_event || (remaining_events == 0)) {
            if (has_event) {
                flush_patient();
            } else {
                has_event = true;
            }

            items.clear();
            remaining_events = patient_lengths[next_patient_index++];
        }

        if (!item.empty()) {
            items[item] += 1;
        }

        remaining_events--;
    };

    for (int64_t row_group = 0; row_group < arrow_reader->num_row_groups();
         row_group++) {
        std::shared_ptr<arrow::Table> table;
        PARQUET_THROW_NOT_OK(
            arrow_reader->ReadRowGroup(row_group, columns, &table));

        auto chunked_values = table->GetColumnByName(property_name);

        for (const auto& array : chunked_values->chunks()) {
            auto string_array = std::dynamic_pointer_cast<A>(array);
            if (string_array == nullptr) {
                throw std::runtime_error("Could not cast property");
            }

            for (int64_t i = 0; i < string_array->length(); i++) {
                if (string_array->IsNull(i)) {
                    add_value(std::string_view());
                } else {
                    add_value(string_array->GetView(i));
                }
            }
        }
    }

    if (items.size() != 0) {
        flush_patient();
    }

    queue_semaphore.signal(slots_to_write);
}

void string_reader_thread(
    const std::filesystem::path& filename, const std::string& property_name,
    const std::shared_ptr<arrow::DataType>& type,
    const std::vector<uint32_t>& patient_lengths,
    std::vector<
        moodycamel::BlockingConcurrentQueue<std::pair<std::string, uint64_t>>>&
        all_write_queues,
    moodycamel::LightweightSemaphore& queue_semaphore) {
    switch (type->id()) {
        case arrow::Type::STRING:
            string_reader_thread_helper<arrow::StringArray>(
                filename, property_name, patient_lengths, all_write_queues,
                queue_semaphore);
            break;

        case arrow::Type::LARGE_STRING:
            string_reader_thread_helper<arrow::LargeStringArray>(
                filename, property_name, patient_lengths, all_write_queues,
                queue_semaphore);
            break;

        default:
            throw std::runtime_error("Unsupported type " + type->ToString());
    };
}

void string_writer_thread(
    std::filesystem::path folder_to_write_to,
    moodycamel::BlockingConcurrentQueue<std::pair<std::string, uint64_t>>&
        queue,
    moodycamel::LightweightSemaphore& queue_semaphore, int num_threads) {
    moodycamel::ConsumerToken c_tok(queue);

    int next_index = 0;

    auto context_deleter = [](ZSTD_CCtx* context) { ZSTD_freeCCtx(context); };

    std::unique_ptr<ZSTD_CCtx, decltype(context_deleter)> context{
        ZSTD_createCCtx(), context_deleter};

    size_t bytes_written = 0;

    absl::flat_hash_map<std::string, uint64_t> items;

    auto flush = [&]() {
        std::vector<std::pair<std::string_view, uint64_t>> vector;
        vector.reserve(items.size());

        for (auto& entry : items) {
            vector.emplace_back(std::move(entry.first), entry.second);
        }

        pdqsort(std::begin(vector), std::end(vector));

        ZstdRowWriter writer(folder_to_write_to / std::to_string(next_index),
                             context.get());
        next_index++;

        for (const auto& item : vector) {
            writer.add_next(item.first, item.second);
        }

        items.clear();
        bytes_written = 0;
    };

    ssize_t num_read = 0;

    int num_remaining_threads = num_threads;

    std::pair<std::string, size_t> entry;
    while (true) {
        queue.wait_dequeue(c_tok, entry);

        if (entry.first.empty()) {
            num_remaining_threads--;
            if (num_remaining_threads == 0) {
                if (items.size() != 0) {
                    flush();
                }
                queue_semaphore.signal(num_read);
                return;
            } else {
                continue;
            }
        }

        num_read++;

        if (num_read == SEMAPHORE_BLOCK_SIZE) {
            queue_semaphore.signal(SEMAPHORE_BLOCK_SIZE);
            num_read = 0;
        }

        size_t item_size = entry.first.size();

        auto attempt = items.try_emplace(std::move(entry.first), entry.second);

        if (attempt.second) {
            // Inserted, increase size
            bytes_written += (item_size + sizeof(std::string) + sizeof(size_t) +
                              sizeof(size_t));
        } else {
            attempt.first->second += entry.second;
        }

        if (bytes_written > PIECE_SIZE) {
            flush();
        }
    }
}

std::vector<std::pair<uint64_t, std::string>> merger_thread(
    std::filesystem::path folder_to_merge) {
    auto context_deleter = [](ZSTD_DCtx* context) { ZSTD_freeDCtx(context); };

    std::unique_ptr<ZSTD_DCtx, decltype(context_deleter)> context{
        ZSTD_createDCtx(), context_deleter};

    std::vector<ZstdRowReader> readers;

    typedef std::tuple<std::string_view, uint64_t, size_t> PriorityQueueItem;

    std::priority_queue<PriorityQueueItem, std::vector<PriorityQueueItem>,
                        std::greater<PriorityQueueItem>>
        queue;

    for (const auto& dir_entry :
         std::filesystem::directory_iterator(folder_to_merge)) {
        readers.emplace_back(
            ZstdRowReader(dir_entry.path().string(), context.get()));
        auto next = readers.back().get_next();
        if (next) {
            queue.push(std::make_tuple(std::get<0>(*next), std::get<1>(*next),
                                       readers.size() - 1));
        }
    }

    std::vector<std::pair<uint64_t, std::string>> entries;

    std::string current_value = "";
    uint64_t current_count = 0;

    auto flush = [&]() {
        uint64_t num_patients = current_count >> 32;
        uint64_t num_times = current_count % (((uint64_t)1) << 32);
        if (num_patients > 1) {
            entries.emplace_back(num_times, std::move(current_value));
        }
    };

    while (!queue.empty()) {
        auto next = std::move(queue.top());
        queue.pop();

        if (current_value == "") {
            current_value = std::string(std::get<0>(next));
        }

        if (current_value == std::get<0>(next)) {
            current_count += std::get<1>(next);
        } else {
            if (current_value >= std::get<0>(next)) {
                std::cout << "Should not happen " << current_value << " "
                          << std::get<0>(next) << " "
                          << (current_value < std::get<0>(next)) << std::endl;
                abort();
            }
            flush();
            current_value = std::string(std::get<0>(next));
            current_count = std::get<1>(next);
        }

        auto future = readers[std::get<2>(next)].get_next();
        if (future) {
            queue.push(std::make_tuple(std::move(std::get<0>(*future)),
                                       std::get<1>(*future),
                                       std::get<2>(next)));
        }
    }

    flush();

    return entries;
}

template <typename F, typename H, typename G>
void run_all(const std::vector<WorkEntry>& work_entries, int num_threads,
             F func, H end, G other) {
    std::vector<std::thread> threads;
    moodycamel::BlockingConcurrentQueue<std::optional<WorkEntry>> work_queue;

    for (const auto& entry : work_entries) {
        work_queue.enqueue({entry});
    }

    for (int i = 0; i < num_threads; i++) {
        work_queue.enqueue(std::nullopt);

        threads.emplace_back([&work_queue, &func, &end]() {
            std::optional<WorkEntry> next_entry;
            while (true) {
                work_queue.wait_dequeue(next_entry);

                if (!next_entry) {
                    end();

                    return;
                }

                std::apply(func, *next_entry);
            }
        });
    }

    other(threads);

    for (auto& thread : threads) {
        thread.join();
    }
}

template <typename F>
void run_all(const std::vector<WorkEntry>& work_entries, int num_threads,
             F func) {
    run_all(
        work_entries, num_threads, func, []() {},
        [](std::vector<std::thread>& threads) {});
}

void process_string_property(
    const std::string& property_name,

    const std::shared_ptr<arrow::DataType>& type,
    const std::vector<std::vector<uint32_t>>& patient_lengths,
    std::filesystem::path temp_path, const std::vector<WorkEntry>& work_entries,
    int num_threads) {
    std::filesystem::path string_path = temp_path / property_name;
    std::filesystem::create_directories(string_path);

    // We have to start by reading everything in

    std::vector<
        moodycamel::BlockingConcurrentQueue<std::pair<std::string, uint64_t>>>
        queues(num_threads);

    moodycamel::LightweightSemaphore write_semaphore(QUEUE_SIZE * num_threads);

    auto reader = [&property_name, &type, &patient_lengths, &queues,
                   &write_semaphore](const std::filesystem::path fname,
                                     size_t index) {
        string_reader_thread(fname, property_name, type, patient_lengths[index],
                             queues, write_semaphore);
    };

    auto writer = [num_threads, &string_path, &queues,
                   &write_semaphore](std::vector<std::thread>& threads) {
        for (int i = 0; i < num_threads; i++) {
            threads.emplace_back(
                [i, &string_path, &queues, &write_semaphore, num_threads]() {
                    std::filesystem::path writer_path =
                        string_path / std::to_string(i);
                    std::filesystem::create_directories(writer_path);
                    string_writer_thread(writer_path, queues[i],
                                         write_semaphore, num_threads);
                });
        }
    };

    auto end_reader = [&queues]() {
        for (auto& queue : queues) {
            queue.enqueue(std::make_pair("", 0));
        }
    };
    run_all(work_entries, num_threads, reader, end_reader, writer);

    std::vector<std::vector<std::pair<uint64_t, std::string>>> all_entries(
        num_threads);

    {
        std::vector<std::thread> threads;

        for (int i = 0; i < num_threads; i++) {
            threads.emplace_back([i, &string_path, &all_entries]() {
                std::filesystem::path writer_path =
                    string_path / std::to_string(i);
                all_entries[i] = merger_thread(writer_path);
                std::filesystem::remove_all(writer_path);
            });
        }

        for (auto& thread : threads) {
            thread.join();
        }
    }

    absl::flat_hash_map<std::string, std::pair<size_t, size_t>> found;

    std::vector<std::pair<uint64_t, std::string>> entries;
    size_t e_index = 0;
    for (auto& e : all_entries) {
        size_t ei_index = 0;
        for (auto& ei : e) {
#ifndef NDEBUG
            if (found.count(ei.second) != 0) {
                std::cout << "Got duplicate! " << ei.second << " " << ei.first
                          << " " << e_index << " " << ei_index << " "
                          << found[ei.second].first << " "
                          << found[ei.second].second << std::endl;
                abort();
            }
            found[ei.second].first = e_index;
            found[ei.second].second = ei_index;
#endif

            entries.emplace_back(std::move(ei));
            ei_index++;
        }
        e_index++;
    }

    auto context_deleter = [](ZSTD_CCtx* context) { ZSTD_freeCCtx(context); };

    pdqsort(std::begin(entries), std::end(entries),
            std::greater<std::pair<uint64_t, std::string>>());

    absl::flat_hash_map<std::string, size_t> dictionary_entries;

    std::unique_ptr<ZSTD_CCtx, decltype(context_deleter)> context{
        ZSTD_createCCtx(), context_deleter};

    {
        ZstdRowWriter writer((string_path / "dictionary").string(),
                             context.get());

        for (size_t i = 0; i < entries.size(); i++) {
            const auto& e = entries[i];
            auto iter = dictionary_entries.try_emplace(std::move(e.second), i);
            if (!iter.second) {
                throw std::runtime_error("Already inserted? " + e.second);
            }
            writer.add_next(e.second, e.first);
        }
    }

    std::vector<std::vector<std::vector<char>>> all_samples(
        work_entries.size());

    size_t num_samples_per_entry =
        (10 * 1000 + work_entries.size() - 1) / work_entries.size();

    run_all(work_entries, num_threads,
            [&all_samples, &property_name, &type, &patient_lengths,
             &dictionary_entries,
             num_samples_per_entry](std::filesystem::path path, size_t index) {
                all_samples[index] = get_samples(
                    path, property_name, type, patient_lengths[index],
                    dictionary_entries, num_samples_per_entry);
            });

    std::vector<size_t> sample_sizes;
    std::vector<char> sample_buffer;

    for (const auto& samples : all_samples) {
        for (const auto& sample : samples) {
            sample_sizes.push_back(sample.size());
            sample_buffer.insert(std::end(sample_buffer), std::begin(sample),
                                 std::end(sample));
        }
    }

    size_t dictionary_size = 100 * 1000;  // 100 kilobytes
    std::vector<char> dictionary(dictionary_size);

    size_t dict_size = ZDICT_trainFromBuffer(
        dictionary.data(), dictionary.size(), sample_buffer.data(),
        sample_sizes.data(), sample_sizes.size());

    if (ZDICT_isError(dict_size)) {
        dict_size = 0;
    }

    dictionary.resize(dict_size);

    {
        std::ofstream zdict(
            string_path / std::string("zdict"),
            std::ios_base::out | std::ios_base::binary | std::ios_base::trunc);
        zdict.write(dictionary.data(), dictionary.size());
    }

    std::vector<std::pair<size_t, std::vector<uint64_t>>> all_lengths(
        work_entries.size());

    run_all(work_entries, num_threads,
            [&all_lengths, &string_path, &property_name, &type,
             &patient_lengths, &dictionary_entries,
             &dictionary](std::filesystem::path path, size_t index) {
                std::filesystem::path target_path =
                    string_path / (std::to_string(index) + ".data");
                all_lengths[index] = write_files(
                    path, property_name, type, patient_lengths[index],
                    dictionary_entries, dictionary, target_path);
            });

    size_t num_patients = 0;

    for (const auto& length : all_lengths) {
        num_patients += length.second.size();
    }

    uint64_t current_offset = (num_patients + 1) * sizeof(uint64_t);

    for (auto& length : all_lengths) {
        size_t temp = length.first;

        length.first = current_offset;

        current_offset += temp;
    }

    run_all(work_entries, num_threads,
            [&all_lengths](std::filesystem::path path, size_t index) {
                auto& item = all_lengths[index];
                for (auto& val : item.second) {
                    val += item.first;
                }
            });

    std::ofstream data_file((string_path / "data"), std::ios_base::out |
                                                        std::ios_base::binary |
                                                        std::ios_base::trunc);
    data_file.exceptions(std::ifstream::failbit | std::ifstream::badbit);

    for (const auto& entry : work_entries) {
        auto& item = all_lengths[entry.second];
        ssize_t num_to_write = item.second.size() * sizeof(uint64_t);
        const char* buffer = (const char*)item.second.data();
        data_file.write(buffer, num_to_write);
    }

    data_file.write((const char*) &current_offset, sizeof(current_offset));

    for (const auto& entry : work_entries) {
        std::filesystem::path entry_path =
            string_path / (std::to_string(entry.second) + ".data");

        if (all_lengths[entry.second].second.size() > 0) {
            std::ifstream entry_file(entry_path,
                                     std::ios_base::in | std::ios_base::binary);

            data_file << entry_file.rdbuf();
        }

        std::filesystem::remove(entry_path);
    }
}

template <typename F>
void iterate_primitive(std::filesystem::path filename,
                       std::string property_name,
                       const std::vector<uint32_t>& patient_lengths, F func) {
    arrow::MemoryPool* pool = arrow::default_memory_pool();

    // Configure general Parquet reader settings
    auto reader_properties = parquet::ReaderProperties(pool);
    reader_properties.set_buffer_size(1024 * 1024);
    reader_properties.enable_buffered_stream();

    // Configure Arrow-specific Parquet reader settings
    auto arrow_reader_props = parquet::ArrowReaderProperties();
    arrow_reader_props.set_batch_size(128 * 1024);  // default 64 * 1024

    parquet::arrow::FileReaderBuilder reader_builder;
    PARQUET_THROW_NOT_OK(reader_builder.OpenFile(filename, /*memory_map=*/false,
                                                 reader_properties));
    reader_builder.memory_pool(pool);
    reader_builder.properties(arrow_reader_props);

    std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
    PARQUET_ASSIGN_OR_THROW(arrow_reader, reader_builder.Build());

    auto properties = get_properties(arrow_reader->manifest());

    int64_t column = properties.find(property_name)->second.second;
    std::vector<int> columns = {(int)column};

    size_t next_patient_index = 0;
    size_t remaining_events;
    bool has_event = false;

    std::vector<char> null_bytes;
    std::vector<char> value_bytes;

    size_t offset_in_bitset;
    std::bitset<sizeof(uint64_t) * 8> bitset;
    uint64_t* nullmap_offset;

    auto flush = [&]() {
        *nullmap_offset++ = bitset.to_ulong();
        offset_in_bitset = 0;
        bitset.reset();
    };

    auto flush_patient = [&]() {
        if (offset_in_bitset != 0) {
            flush();
        }

        null_bytes.insert(std::end(null_bytes), std::begin(value_bytes),
                          std::end(value_bytes));

        func(std::move(null_bytes));
    };

    auto write_null = [&]() {
        offset_in_bitset++;
        if (offset_in_bitset == bitset.size()) {
            flush();
        }
    };

    auto write_value = [&](std::string_view text) {
        value_bytes.insert(std::end(value_bytes), std::begin(text),
                           std::end(text));

        bitset.set(offset_in_bitset);
        offset_in_bitset++;
        if (offset_in_bitset == bitset.size()) {
            flush();
        }
    };

    auto add_value = [&](std::string_view value) {
        if (!has_event || (remaining_events == 0)) {
            if (has_event) {
                flush_patient();
            } else {
                has_event = true;
            }

            remaining_events = patient_lengths[next_patient_index++];

            null_bytes.clear();
            value_bytes.clear();
            offset_in_bitset = 0;
            bitset.reset();

            auto num_null_bytes =
                (remaining_events + bitset.size() - 1) / bitset.size();

            null_bytes.resize(num_null_bytes * sizeof(uint64_t));

            nullmap_offset = (uint64_t*)null_bytes.data();
        }

        if (value.empty()) {
            write_null();
        } else {
            write_value(value);
        }

        remaining_events--;
    };

    for (int64_t row_group = 0; row_group < arrow_reader->num_row_groups();
         row_group++) {
        std::shared_ptr<arrow::Table> table;
        PARQUET_THROW_NOT_OK(
            arrow_reader->ReadRowGroup(row_group, columns, &table));

        auto chunked_values = table->GetColumnByName(property_name);

        for (const auto& array : chunked_values->chunks()) {
            auto primitive_array =
                std::dynamic_pointer_cast<arrow::PrimitiveArray>(array);
            if (primitive_array == nullptr) {
                throw std::runtime_error("Could not cast property");
            }

            int32_t type_bytes = primitive_array->type()->byte_width();

            for (int64_t i = 0; i < primitive_array->length(); i++) {
                if (primitive_array->IsNull(i)) {
                    add_value(std::string_view());
                } else {
                    std::string_view item(
                        (const char*)primitive_array->values()->data() +
                            (primitive_array->offset() + i) * type_bytes,
                        (size_t)type_bytes);
                    add_value(item);
                }
            }
        }
    }

    if (has_event) {
        flush_patient();
    }
}

std::vector<std::vector<char>> get_primitive_samples(
    std::filesystem::path filename, std::string property_name,
    const std::vector<uint32_t>& patient_lengths, size_t num_samples) {
    std::vector<std::vector<char>> samples;
    size_t sample_count = 0;

    iterate_primitive(filename, property_name, patient_lengths,
                      [&](std::vector<char> bytes) {
                          sample_count++;

                          if (samples.size() < num_samples) {
                              samples.emplace_back(std::move(bytes));
                          } else {
                              size_t j = (size_t)(rand() % sample_count);
                              if (j < num_samples) {
                                  samples[j] = std::move(bytes);
                              }
                          }
                      });

    return samples;
}

std::pair<size_t, std::vector<uint64_t>> write_primitive_files(
    std::filesystem::path filename, std::string property_name,
    const std::vector<uint32_t>& patient_lengths,
    const std::vector<char>& dictionary,
    std::filesystem::path target_filename) {
    std::vector<uint64_t> offsets;

    std::ofstream output_file(target_filename, std::ios_base::out |
                                                   std::ios_base::binary |
                                                   std::ios_base::trunc);
    output_file.exceptions(std::ifstream::failbit | std::ifstream::badbit);

    uint64_t num_bytes = 0;

    auto context_deleter = [](ZSTD_CCtx* context) { ZSTD_freeCCtx(context); };

    std::unique_ptr<ZSTD_CCtx, decltype(context_deleter)> context2{
        ZSTD_createCCtx(), context_deleter};

    size_t res =
        ZSTD_CCtx_setParameter(context2.get(), ZSTD_c_compressionLevel, 22);

    if (ZSTD_isError(res)) {
        throw std::runtime_error("Could not set the compression level");
    }

    res = ZSTD_CCtx_loadDictionary(context2.get(), dictionary.data(),
                                   dictionary.size());
    if (ZSTD_isError(res)) {
        throw std::runtime_error("Could not load the dictionary");
    }

    iterate_primitive(
        filename, property_name, patient_lengths, [&](std::vector<char> bytes) {
            std::vector<char> final_bytes(sizeof(uint32_t) +
                                          ZSTD_compressBound(bytes.size()));
            res = ZSTD_compress2(
                context2.get(), final_bytes.data() + sizeof(uint32_t),
                final_bytes.size(), bytes.data(), bytes.size());

            if (ZSTD_isError(res)) {
                throw std::runtime_error("Unable to compress");
            }

            final_bytes.resize(res + sizeof(uint32_t));

            uint32_t* length_pointer = (uint32_t*)final_bytes.data();
            *length_pointer = bytes.size();

            offsets.push_back(num_bytes);

            output_file.write(final_bytes.data(), final_bytes.size());

            num_bytes += final_bytes.size();
        });

    return std::make_pair(num_bytes, std::move(offsets));
}

void process_primitive_property(
    const std::string& property_name,
    const std::vector<std::vector<uint32_t>>& patient_lengths,
    std::filesystem::path temp_path, const std::vector<WorkEntry>& work_entries,
    int num_threads) {
    std::filesystem::path string_path = temp_path / property_name;
    std::filesystem::create_directories(string_path);

    // We have to start by reading everything in

    std::vector<std::vector<std::vector<char>>> all_samples(
        work_entries.size());

    size_t num_samples_per_entry =
        (10 * 1000 + work_entries.size() - 1) / work_entries.size();

    run_all(work_entries, num_threads,
            [&all_samples, &property_name, &patient_lengths,
             num_samples_per_entry](std::filesystem::path path, size_t index) {
                all_samples[index] = get_primitive_samples(
                    path, property_name, patient_lengths[index],
                    num_samples_per_entry);
            });

    std::vector<size_t> sample_sizes;
    std::vector<char> sample_buffer;

    for (const auto& samples : all_samples) {
        for (const auto& sample : samples) {
            sample_sizes.push_back(sample.size());
            sample_buffer.insert(std::end(sample_buffer), std::begin(sample),
                                 std::end(sample));
        }
    }

    size_t dictionary_size = 100 * 1000;  // 100 kilobytes
    std::vector<char> dictionary(dictionary_size);

    size_t dict_size = ZDICT_trainFromBuffer(
        dictionary.data(), dictionary.size(), sample_buffer.data(),
        sample_sizes.data(), sample_sizes.size());

    if (ZDICT_isError(dict_size)) {
        dict_size = 0;
    }

    dictionary.resize(dict_size);

    {
        std::ofstream zdict(
            string_path / std::string("zdict"),
            std::ios_base::out | std::ios_base::binary | std::ios_base::trunc);
        zdict.write(dictionary.data(), dictionary.size());
    }

    std::vector<std::pair<size_t, std::vector<uint64_t>>> all_lengths(
        work_entries.size());

    run_all(work_entries, num_threads,
            [&all_lengths, &string_path, &property_name, &patient_lengths,
             &dictionary](std::filesystem::path path, size_t index) {
                std::filesystem::path target_path =
                    string_path / (std::to_string(index) + ".data");
                all_lengths[index] = write_primitive_files(
                    path, property_name, patient_lengths[index], dictionary,
                    target_path);
            });

    uint64_t num_patients = 0;

    for (const auto& length : all_lengths) {
        num_patients += length.second.size();
    }

    uint64_t current_offset = (num_patients + 1) * sizeof(uint64_t);

    for (auto& length : all_lengths) {
        size_t temp = length.first;

        length.first = current_offset;

        current_offset += temp;
    }

    run_all(work_entries, num_threads,
            [&all_lengths](std::filesystem::path path, size_t index) {
                auto& item = all_lengths[index];
                for (auto& val : item.second) {
                    val += item.first;
                }
            });

    std::ofstream data_file((string_path / "data"), std::ios_base::out |
                                                        std::ios_base::binary |
                                                        std::ios_base::trunc);

    for (const auto& entry : work_entries) {
        auto& item = all_lengths[entry.second];
        ssize_t num_to_write = item.second.size() * sizeof(uint64_t);
        const char* buffer = (const char*)item.second.data();
        data_file.write(buffer, num_to_write);
    }

    data_file.write((const char*) &current_offset, sizeof(current_offset));

    for (const auto& entry : work_entries) {
        std::filesystem::path entry_path =
            string_path / (std::to_string(entry.second) + ".data");

        if (all_lengths[entry.second].second.size() > 0) {
            std::ifstream entry_file(entry_path,
                                     std::ios_base::in | std::ios_base::binary);

            data_file << entry_file.rdbuf();
        }

        std::filesystem::remove(entry_path);
    }
}

template <typename F>
void iterate_time(std::filesystem::path filename, std::string property_name,
                  const std::vector<uint32_t>& patient_lengths, F func) {
    arrow::MemoryPool* pool = arrow::default_memory_pool();

    // Configure general Parquet reader settings
    auto reader_properties = parquet::ReaderProperties(pool);
    reader_properties.set_buffer_size(1024 * 1024);
    reader_properties.enable_buffered_stream();

    // Configure Arrow-specific Parquet reader settings
    auto arrow_reader_props = parquet::ArrowReaderProperties();
    arrow_reader_props.set_batch_size(128 * 1024);  // default 64 * 1024

    parquet::arrow::FileReaderBuilder reader_builder;
    PARQUET_THROW_NOT_OK(reader_builder.OpenFile(filename, /*memory_map=*/false,
                                                 reader_properties));
    reader_builder.memory_pool(pool);
    reader_builder.properties(arrow_reader_props);

    std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
    PARQUET_ASSIGN_OR_THROW(arrow_reader, reader_builder.Build());

    auto properties = get_properties(arrow_reader->manifest());

    int64_t column = properties.find(property_name)->second.second;
    std::vector<int> columns = {(int)column};

    size_t next_patient_index = 0;
    size_t remaining_events;
    bool has_event = false;

    constexpr int64_t micros_per_second = ((int64_t)1000) * 1000;
    constexpr int64_t micros_per_day = micros_per_second * 24 * 60 * 60;

    std::optional<int64_t> starting_timestamp = std::nullopt;

    int64_t last_timestamp;

    int64_t current_timestamp;
    int64_t current_timestamp_count;

    int64_t num_null_timestamps;

    std::vector<uint32_t> values;

    auto flush_timestamp = [&]() {
        while (current_timestamp_count != 0) {
            int64_t delta = current_timestamp - last_timestamp;

            int64_t delta_days = delta / micros_per_day;
            int64_t delta_seconds =
                (delta % micros_per_day) / micros_per_second;
            int64_t delta_micros = delta % micros_per_second;

            if (delta_days < 0 || delta_seconds < 0 || delta_micros < 0) {
                throw std::runtime_error("Times are not in order");
            }

            int64_t num_to_emit =
                std::min(current_timestamp_count, (int64_t)15);

            if (delta_seconds == 0 && delta_micros == 0) {
                values.push_back((delta_days << 4) + num_to_emit);
            } else if (delta_micros == 0) {
                values.push_back((delta_days << 4));
                values.push_back((delta_seconds << 4) + num_to_emit);
            } else {
                values.push_back((delta_days << 4));
                values.push_back((delta_seconds << 4));
                values.push_back((delta_micros << 4) + num_to_emit);
            }

            current_timestamp_count -= num_to_emit;

            last_timestamp = current_timestamp;
        }
    };

    auto flush_patient = [&]() {
        flush_timestamp();

        values[0] = num_null_timestamps;

        std::vector<char> helper(
            sizeof(int64_t) + sizeof(uint32_t) +
            streamvbyte_max_compressedbytes(values.size()));
        size_t count = streamvbyte_encode_0124(
            values.data(), values.size(),
            (uint8_t*)helper.data() + sizeof(uint32_t) + sizeof(int64_t));

        int64_t* start_pointer = (int64_t*)(helper.data());
        *start_pointer = *starting_timestamp;

        uint32_t* length_pointer = (uint32_t*)(helper.data() + sizeof(int64_t));
        *length_pointer = values.size();

        helper.resize(sizeof(int64_t) + sizeof(uint32_t) + count);

        func(std::move(helper));
    };

    auto add_time = [&](std::optional<int64_t> time) {
        if (!has_event || (remaining_events == 0)) {
            if (has_event) {
                flush_patient();
            } else {
                has_event = true;
            }

            remaining_events = patient_lengths[next_patient_index++];

            values.clear();
            values.push_back(0);
            starting_timestamp = std::nullopt;
            num_null_timestamps = 0;
        }

        if (time == std::nullopt) {
            if (starting_timestamp != std::nullopt) {
                throw std::runtime_error(
                    "Should only get null times at the start of a patient");
            }
            num_null_timestamps++;
        } else {
            if (starting_timestamp == std::nullopt) {
                starting_timestamp = *time;
                current_timestamp = *time;
                current_timestamp_count = 1;
                last_timestamp = *time;
            } else if (*time == current_timestamp) {
                current_timestamp_count++;
            } else {
                flush_timestamp();
                current_timestamp = *time;
                current_timestamp_count = 1;
            }
        }

        remaining_events--;
    };

    for (int64_t row_group = 0; row_group < arrow_reader->num_row_groups();
         row_group++) {
        std::shared_ptr<arrow::Table> table;
        PARQUET_THROW_NOT_OK(
            arrow_reader->ReadRowGroup(row_group, columns, &table));

        auto chunked_values = table->GetColumnByName(property_name);

        for (const auto& array : chunked_values->chunks()) {
            auto time_array =
                std::dynamic_pointer_cast<arrow::TimestampArray>(array);
            if (time_array == nullptr) {
                throw std::runtime_error("Could not find property");
            }

            auto time_type = std::dynamic_pointer_cast<arrow::TimestampType>(
                time_array->type());

            if (time_type->unit() != arrow::TimeUnit::MICRO) {
                throw std::runtime_error("Wrong units for timestamp");
            }

            if (!time_type->timezone().empty()) {
                throw std::runtime_error("Need an empty timezone");
            }
            for (int64_t i = 0; i < time_array->length(); i++) {
                if (time_array->IsNull(i)) {
                    add_time(std::nullopt);
                } else {
                    add_time(time_array->Value(i));
                }
            }
        }
    }

    if (has_event) {
        flush_patient();
    }
}

std::vector<std::vector<char>> get_time_samples(
    std::filesystem::path filename, std::string property_name,
    const std::vector<uint32_t>& patient_lengths, size_t num_samples) {
    std::vector<std::vector<char>> samples;
    size_t sample_count = 0;

    iterate_time(filename, property_name, patient_lengths,
                 [&](std::vector<char> bytes) {
                     sample_count++;

                     if (samples.size() < num_samples) {
                         samples.emplace_back(std::move(bytes));
                     } else {
                         size_t j = (size_t)(rand() % sample_count);
                         if (j < num_samples) {
                             samples[j] = std::move(bytes);
                         }
                     }
                 });

    return samples;
}

std::pair<size_t, std::vector<uint64_t>> write_time_files(
    std::filesystem::path filename, std::string property_name,
    const std::vector<uint32_t>& patient_lengths,
    const std::vector<char>& dictionary,
    std::filesystem::path target_filename) {
    std::vector<uint64_t> offsets;

    std::ofstream output_file(target_filename, std::ios_base::out |
                                                   std::ios_base::binary |
                                                   std::ios_base::trunc);
    output_file.exceptions(std::ifstream::failbit | std::ifstream::badbit);

    uint64_t num_bytes = 0;

    auto context_deleter = [](ZSTD_CCtx* context) { ZSTD_freeCCtx(context); };

    std::unique_ptr<ZSTD_CCtx, decltype(context_deleter)> context2{
        ZSTD_createCCtx(), context_deleter};

    size_t res =
        ZSTD_CCtx_setParameter(context2.get(), ZSTD_c_compressionLevel, 22);

    if (ZSTD_isError(res)) {
        throw std::runtime_error("Could not set the compression level");
    }

    res = ZSTD_CCtx_loadDictionary(context2.get(), dictionary.data(),
                                   dictionary.size());
    if (ZSTD_isError(res)) {
        throw std::runtime_error("Could not load the dictionary");
    }

    iterate_time(
        filename, property_name, patient_lengths, [&](std::vector<char> bytes) {
            std::vector<char> final_bytes(sizeof(uint32_t) +
                                          ZSTD_compressBound(bytes.size()));
            res = ZSTD_compress2(
                context2.get(), final_bytes.data() + sizeof(uint32_t),
                final_bytes.size(), bytes.data(), bytes.size());

            if (ZSTD_isError(res)) {
                throw std::runtime_error("Unable to compress");
            }

            final_bytes.resize(res + sizeof(uint32_t));

            uint32_t* length_pointer = (uint32_t*)final_bytes.data();
            *length_pointer = bytes.size();

            offsets.push_back(num_bytes);

            output_file.write(final_bytes.data(), final_bytes.size());

            num_bytes += final_bytes.size();
        });

    return std::make_pair(num_bytes, std::move(offsets));
}

void process_time_property(
    const std::string& property_name,
    const std::vector<std::vector<uint32_t>>& patient_lengths,
    std::filesystem::path temp_path, const std::vector<WorkEntry>& work_entries,
    int num_threads) {
    std::filesystem::path string_path = temp_path / property_name;
    std::filesystem::create_directories(string_path);

    // We have to start by reading everything in

    std::vector<std::vector<std::vector<char>>> all_samples(
        work_entries.size());

    size_t num_samples_per_entry =
        (10 * 1000 + work_entries.size() - 1) / work_entries.size();

    run_all(work_entries, num_threads,
            [&all_samples, &property_name, &patient_lengths,
             num_samples_per_entry](std::filesystem::path path, size_t index) {
                all_samples[index] = get_time_samples(path, property_name,
                                                      patient_lengths[index],
                                                      num_samples_per_entry);
            });

    std::vector<size_t> sample_sizes;
    std::vector<char> sample_buffer;

    for (const auto& samples : all_samples) {
        for (const auto& sample : samples) {
            sample_sizes.push_back(sample.size());
            sample_buffer.insert(std::end(sample_buffer), std::begin(sample),
                                 std::end(sample));
        }
    }

    size_t dictionary_size = 100 * 1000;  // 100 kilobytes
    std::vector<char> dictionary(dictionary_size);

    size_t dict_size = ZDICT_trainFromBuffer(
        dictionary.data(), dictionary.size(), sample_buffer.data(),
        sample_sizes.data(), sample_sizes.size());

    if (ZDICT_isError(dict_size)) {
        dict_size = 0;
    }

    dictionary.resize(dict_size);

    {
        std::ofstream zdict(
            string_path / std::string("zdict"),
            std::ios_base::out | std::ios_base::binary | std::ios_base::trunc);
        zdict.write(dictionary.data(), dictionary.size());
    }

    std::vector<std::pair<size_t, std::vector<uint64_t>>> all_lengths(
        work_entries.size());

    run_all(work_entries, num_threads,
            [&all_lengths, &string_path, &property_name, &patient_lengths,
             &dictionary](std::filesystem::path path, size_t index) {
                std::filesystem::path target_path =
                    string_path / (std::to_string(index) + ".data");
                all_lengths[index] = write_time_files(path, property_name,
                                                      patient_lengths[index],
                                                      dictionary, target_path);
            });

    uint64_t num_patients = 0;

    for (const auto& length : all_lengths) {
        num_patients += length.second.size();
    }

    uint64_t current_offset = (num_patients + 1) * sizeof(uint64_t);

    for (auto& length : all_lengths) {
        size_t temp = length.first;

        length.first = current_offset;

        current_offset += temp;
    }

    run_all(work_entries, num_threads,
            [&all_lengths](std::filesystem::path path, size_t index) {
                auto& item = all_lengths[index];
                for (auto& val : item.second) {
                    val += item.first;
                }
            });

    std::ofstream data_file((string_path / "data"), std::ios_base::out |
                                                        std::ios_base::binary |
                                                        std::ios_base::trunc);

    for (const auto& entry : work_entries) {
        auto& item = all_lengths[entry.second];
        ssize_t num_to_write = item.second.size() * sizeof(uint64_t);
        const char* buffer = (const char*)item.second.data();
        data_file.write(buffer, num_to_write);
    }

    data_file.write((const char*) &current_offset, sizeof(current_offset));

    for (const auto& entry : work_entries) {
        std::filesystem::path entry_path =
            string_path / (std::to_string(entry.second) + ".data");

        if (all_lengths[entry.second].second.size() > 0) {
            std::ifstream entry_file(entry_path,
                                     std::ios_base::in | std::ios_base::binary);

            data_file << entry_file.rdbuf();
        }

        std::filesystem::remove(entry_path);
    }
}

std::pair<std::vector<int64_t>, std::vector<uint32_t>> get_patient_ids(
    std::filesystem::path filename) {
    arrow::MemoryPool* pool = arrow::default_memory_pool();

    // Configure general Parquet reader settings
    auto reader_properties = parquet::ReaderProperties(pool);
    reader_properties.set_buffer_size(1024 * 1024);
    reader_properties.enable_buffered_stream();

    // Configure Arrow-specific Parquet reader settings
    auto arrow_reader_props = parquet::ArrowReaderProperties();
    arrow_reader_props.set_batch_size(128 * 1024);  // default 64 * 1024

    parquet::arrow::FileReaderBuilder reader_builder;
    PARQUET_THROW_NOT_OK(reader_builder.OpenFile(filename, /*memory_map=*/false,
                                                 reader_properties));
    reader_builder.memory_pool(pool);
    reader_builder.properties(arrow_reader_props);

    std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
    PARQUET_ASSIGN_OR_THROW(arrow_reader, reader_builder.Build());

    auto properties = get_properties(arrow_reader->manifest());

    int64_t patient_id_column = properties.find("patient_id")->second.second;
    std::vector<int> columns = {(int)patient_id_column};

    std::vector<int64_t> patient_ids;

    std::vector<int64_t> unique_patient_ids;
    std::vector<uint32_t> lengths;

    bool has_event = false;
    int64_t current_patient_id = 0;
    size_t current_patient_length = 0;

    auto flush_patient = [&]() {
        unique_patient_ids.push_back(current_patient_id);
        lengths.push_back(current_patient_length);
    };

    auto add_patient_id = [&](int64_t patient_id) {
        if (!has_event || (patient_id != current_patient_id)) {
            if (has_event) {
                flush_patient();
            } else {
                has_event = true;
            }
            current_patient_id = patient_id;
            current_patient_length = 0;
        }

        current_patient_length++;
    };

    for (int64_t row_group = 0; row_group < arrow_reader->num_row_groups();
         row_group++) {
        std::shared_ptr<arrow::Table> table;
        PARQUET_THROW_NOT_OK(
            arrow_reader->ReadRowGroup(row_group, columns, &table));

        auto chunked_patient_id = table->GetColumnByName("patient_id");

        if (chunked_patient_id == nullptr) {
            throw std::runtime_error("Could not get column");
        }

        for (const auto& patient_id_chunk : chunked_patient_id->chunks()) {
            auto patient_id =
                std::dynamic_pointer_cast<arrow::Int64Array>(patient_id_chunk);
            if (patient_id == nullptr) {
                throw std::runtime_error("Could not cast patient_id");
            }

            for (int64_t i = 0; i < patient_id->length(); i++) {
                if (!patient_id->IsValid(i)) {
                    throw std::runtime_error("Invalid timestamp");
                }
                add_patient_id(patient_id->Value(i));
            }
        }
    }

    if (has_event) {
        flush_patient();
    }

    return {std::move(unique_patient_ids), std::move(lengths)};
}

std::pair<std::vector<std::vector<uint32_t>>, uint32_t> process_patient_id(
    std::filesystem::path temp_path, const std::vector<WorkEntry>& work_entries,
    int num_threads) {
    std::vector<std::pair<std::vector<int64_t>, std::vector<uint32_t>>>
        all_lengths(work_entries.size());

    std::vector<std::vector<uint32_t>> result_lengths(work_entries.size());

    run_all(
        work_entries, num_threads,
        [&temp_path, &all_lengths](std::filesystem::path path, size_t index) {
            all_lengths[index] = get_patient_ids(path);
        });

    size_t num_patients = 0;

    for (const auto& length : all_lengths) {
        num_patients += length.first.size();
    }

    {
        std::ofstream pids_file(
            temp_path / "patient_id",
            std::ios_base::out | std::ios_base::binary | std::ios_base::trunc);
        std::ofstream lengths_file(
            temp_path / "meds_reader.length",
            std::ios_base::out | std::ios_base::binary | std::ios_base::trunc);

        absl::flat_hash_set<int64_t> seen_patient_ids;

        for (const auto& entry : work_entries) {
            auto& item = all_lengths[entry.second];
            pids_file.write((const char*)item.first.data(),
                            sizeof(int64_t) * item.first.size());
            for (int64_t pid : item.first) {
                if (seen_patient_ids.count(pid) != 0) {
                    throw std::runtime_error("Had duplicate patient ids " +
                                             std::to_string(pid));
                }
                seen_patient_ids.insert(pid);
            }
            lengths_file.write((const char*)item.second.data(),
                               sizeof(uint32_t) * item.second.size());

            result_lengths[entry.second] = std::move(item.second);
        }
    }

    return {result_lengths, num_patients};
}

DataType convert_to_datatype(const std::shared_ptr<arrow::DataType>& type) {
    switch (type->id()) {
        case arrow::Type::STRING:
            return DataType::STRING;

        case arrow::Type::LARGE_STRING:
            return DataType::LARGE_STRING;

        case arrow::Type::TIMESTAMP:
            return DataType::TIMESTAMP;

        case arrow::Type::FLOAT:
            return DataType::FLOAT32;

        case arrow::Type::DOUBLE:
            return DataType::FLOAT64;

        case arrow::Type::INT8:
            return DataType::INT8;
        case arrow::Type::INT16:
            return DataType::INT16;
        case arrow::Type::INT32:
            return DataType::INT32;
        case arrow::Type::INT64:
            return DataType::INT64;

        case arrow::Type::UINT8:
            return DataType::UINT8;
        case arrow::Type::UINT16:
            return DataType::UINT16;
        case arrow::Type::UINT32:
            return DataType::UINT32;
        case arrow::Type::UINT64:
            return DataType::UINT64;

        default:
            throw std::runtime_error("Unsupported type " + type->ToString());
    };

    // Unreachable
    abort();
}

void process_property(const std::string& property_name,
                      const std::shared_ptr<arrow::DataType>& type,
                      const std::vector<std::vector<uint32_t>>& patient_lengths,
                      const std::filesystem::path& temp_path,
                      const std::vector<WorkEntry>& work_entries,
                      int num_threads) {
    if (property_name == "time") {
        process_time_property(property_name, patient_lengths, temp_path,
                              work_entries, num_threads);
        return;
    }

    switch (type->id()) {
        case arrow::Type::STRING:
        case arrow::Type::LARGE_STRING:
            process_string_property(property_name, type, patient_lengths,
                                    temp_path, work_entries, num_threads);
            return;

        case arrow::Type::TIMESTAMP:
        case arrow::Type::FLOAT:
        case arrow::Type::DOUBLE:
        case arrow::Type::INT8:
        case arrow::Type::INT16:
        case arrow::Type::INT32:
        case arrow::Type::INT64:
        case arrow::Type::UINT8:
        case arrow::Type::UINT16:
        case arrow::Type::UINT32:
        case arrow::Type::UINT64:
            process_primitive_property(property_name, patient_lengths,
                                       temp_path, work_entries, num_threads);
            return;

        default:
            throw std::runtime_error("Unsupported type " + type->ToString());
    };

    // Unreachable
    abort();
}

template <typename F>
void run_all_simple(int num_patients, int num_threads, F func) {
    std::vector<std::thread> threads;

    int patients_per_thread = (num_patients + num_threads - 1) / num_threads;

    for (int i = 0; i < num_threads; i++) {
        threads.emplace_back([&func, i, num_patients, patients_per_thread]() {
            int start = std::min(num_patients, patients_per_thread * i);
            int end = std::min(num_patients, patients_per_thread * (i + 1));
            func(i, start, end);
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }
}

auto context_deleter = [](ZSTD_DCtx* context) { ZSTD_freeDCtx(context); };

struct PropertyNullReader {
    PropertyNullReader(const std::filesystem::path& property_path,
                       std::string property_name)
        : zdict_file(property_path / "zdict"),
          data_file(property_path / "data"),
          context(ZSTD_createDCtx(), context_deleter),
          is_time(property_name == "time") {
        if (zdict_file.bytes().size() != 0) {
            size_t ret = ZSTD_DCtx_loadDictionary_byReference(
                context.get(), (void*)zdict_file.bytes().data(),
                zdict_file.bytes().size());
            if (ZSTD_isError(ret)) {
                throw std::runtime_error("Unable to load dictionary");
            }
        }
    }

    std::vector<char> decompressed;
    std::vector<uint32_t> values;

    std::vector<uint64_t> get_null_bytes(int32_t patient_offset,
                                         int32_t length) {
        uint64_t offset = data_file.data<uint64_t>()[patient_offset];
        uint64_t num_bytes =
            data_file.data<uint64_t>()[patient_offset + 1] - offset;

        uint32_t* length_pointer =
            (uint32_t*)(data_file.bytes().data() + offset);
        uint32_t decompressed_size = *length_pointer;
        if (decompressed.size() < decompressed_size) {
            decompressed.resize(2 * decompressed_size);
        }

        size_t ret = ZSTD_decompressDCtx(
            context.get(), decompressed.data(), decompressed.size(),
            data_file.bytes().data() + offset + sizeof(uint32_t),
            num_bytes - sizeof(uint32_t));

        if (ZSTD_isError(ret)) {
            throw std::runtime_error("Unable to decompress " +
                                     std::string(ZSTD_getErrorName(ret)));
        }

        if (ret != decompressed_size) {
            throw std::runtime_error("Decompressed the wrong amount of data " +
                                     std::to_string(ret) + " " +
                                     std::to_string(decompressed_size));
        }

        auto num_null_bytes =
            (length + sizeof(uint64_t) * 8 - 1) / (sizeof(uint64_t) * 8);
        if (is_time) {
            std::vector<uint64_t> result(num_null_bytes,
                                         std::numeric_limits<uint64_t>::max());

            length_pointer = (uint32_t*)(decompressed.data() + sizeof(int64_t));

            size_t num_values = *length_pointer;
            if (values.size() < num_values) {
                values.resize(num_values * 2);
            }

            size_t num_read =
                streamvbyte_decode_0124((const uint8_t*)decompressed.data() +
                                            sizeof(uint32_t) + sizeof(int64_t),
                                        values.data(), num_values);

            if (num_read + sizeof(uint32_t) + sizeof(int64_t) !=
                decompressed_size) {
                throw std::runtime_error("Decoded too much? " +
                                         std::to_string(num_read) + " " +
                                         std::to_string(num_bytes));
            }

            uint32_t num_null = values[0];

            for (uint32_t i = 0; i < num_null; i++) {
                uint64_t offset = i % 64;
                uint64_t mask = 1;
                mask <<= offset;

                result[i / 64] &= ~mask;
            }

            return result;

        } else {
            absl::Span<const uint64_t> null_bytes{
                (const uint64_t*)decompressed.data(), num_null_bytes};

            return std::vector<uint64_t>(std::begin(null_bytes),
                                         std::end(null_bytes));
        }
    }

    MmapFile zdict_file;
    MmapFile data_file;

    std::unique_ptr<ZSTD_DCtx, decltype(context_deleter)> context;
    bool is_time;
};

template <typename T, typename F>
void iterate_null_map(std::vector<PropertyNullReader>& readers,
                      absl::Span<const int32_t> length_map, int32_t start,
                      int32_t end, F func) {
    std::vector<std::vector<uint64_t>> null_maps(readers.size());

    for (int32_t patient_id = start; patient_id < end; patient_id++) {
        int32_t length = length_map[patient_id];

        for (size_t i = 0; i < readers.size(); i++) {
            null_maps[i] = readers[i].get_null_bytes(patient_id, length);
        }

        std::vector<char> bytes(sizeof(T) * length);
        T* null_bytes = (T*)bytes.data();

        std::bitset<sizeof(T) * 8> next_null_byte;

        for (int32_t i = 0; i < length; i++) {
            next_null_byte.reset();

            for (size_t property_index = 0; property_index < readers.size();
                 property_index++) {
                uint64_t offset = i % 64;
                uint64_t mask = 1;
                mask <<= offset;

                bool is_present =
                    (null_maps[property_index][i / 64] & mask) != 0;
                next_null_byte.set(property_index, is_present);
            }

            *null_bytes++ = next_null_byte.to_ullong();
        }

        func(bytes);
    }
}

template <typename T>
std::vector<std::vector<char>> get_null_map_samples(
    std::vector<PropertyNullReader>& readers,
    absl::Span<const int32_t> length_map, int32_t start, int32_t end,
    size_t num_samples) {
    std::vector<std::vector<char>> samples;
    size_t sample_count = 0;

    iterate_null_map<T>(readers, length_map, start, end,
                        [&](std::vector<char> bytes) {
                            sample_count++;

                            if (samples.size() < num_samples) {
                                samples.emplace_back(std::move(bytes));
                            } else {
                                size_t j = (size_t)(rand() % sample_count);
                                if (j < num_samples) {
                                    samples[j] = std::move(bytes);
                                }
                            }
                        });

    return samples;
}

template <typename T>
std::pair<size_t, std::vector<uint64_t>> write_null_map(
    std::vector<PropertyNullReader>& readers,
    absl::Span<const int32_t> length_map, int32_t start, int32_t end,
    const std::vector<char>& dictionary,
    std::filesystem::path target_filename) {
    std::vector<uint64_t> offsets;

    std::ofstream output_file(target_filename, std::ios_base::out |
                                                   std::ios_base::binary |
                                                   std::ios_base::trunc);
    output_file.exceptions(std::ofstream::failbit | std::ofstream::badbit |
                           std::ofstream::eofbit);

    uint64_t num_bytes = 0;

    auto context_deleter = [](ZSTD_CCtx* context) { ZSTD_freeCCtx(context); };

    std::unique_ptr<ZSTD_CCtx, decltype(context_deleter)> context2{
        ZSTD_createCCtx(), context_deleter};

    size_t res =
        ZSTD_CCtx_setParameter(context2.get(), ZSTD_c_compressionLevel, 22);

    if (ZSTD_isError(res)) {
        throw std::runtime_error("Could not set the compression level");
    }

    res = ZSTD_CCtx_loadDictionary(context2.get(), dictionary.data(),
                                   dictionary.size());
    if (ZSTD_isError(res)) {
        throw std::runtime_error("Could not load the dictionary");
    }

    iterate_null_map<T>(
        readers, length_map, start, end, [&](std::vector<char> bytes) {
            std::vector<char> final_bytes(sizeof(uint32_t) +
                                          ZSTD_compressBound(bytes.size()));
            res = ZSTD_compress2(
                context2.get(), final_bytes.data() + sizeof(uint32_t),
                final_bytes.size(), bytes.data(), bytes.size());

            if (ZSTD_isError(res)) {
                throw std::runtime_error("Unable to compress");
            }

            final_bytes.resize(res + sizeof(uint32_t));

            uint32_t* length_pointer = (uint32_t*)final_bytes.data();
            *length_pointer = bytes.size();

            offsets.push_back(num_bytes);

            output_file.write(final_bytes.data(), final_bytes.size());

            num_bytes += final_bytes.size();
        });

    return std::make_pair(num_bytes, std::move(offsets));
}

template <typename T>
void process_null_map(
    const std::vector<std::pair<std::string, std::shared_ptr<arrow::DataType>>>&
        properties,
    const std::filesystem::path& destination_path, int num_threads,
    int num_patients) {
    MmapFile length_file(destination_path / "meds_reader.length");
    absl::Span<const int32_t> patient_lengths = length_file.data<int32_t>();

    std::vector<std::vector<PropertyNullReader>> property_readers(num_threads);
    for (int i = 0; i < num_threads; i++) {
        for (const auto& entry : properties) {
            property_readers[i].emplace_back(destination_path / entry.first,
                                             entry.first);
        }
    }

    std::filesystem::path string_path =
        destination_path / "meds_reader.null_map";
    std::filesystem::create_directories(string_path);

    // We have to start by reading everything in

    std::vector<std::vector<std::vector<char>>> all_samples(num_threads);

    size_t num_samples_per_entry = (10 * 1000 + num_threads - 1) / num_threads;

    run_all_simple(num_patients, num_threads,
                   [&all_samples, &property_readers, &patient_lengths,
                    num_samples_per_entry](size_t index, int start, int end) {
                       all_samples[index] = get_null_map_samples<T>(
                           property_readers[index], patient_lengths, start, end,
                           num_samples_per_entry);
                   });

    std::vector<size_t> sample_sizes;
    std::vector<char> sample_buffer;

    for (const auto& samples : all_samples) {
        for (const auto& sample : samples) {
            sample_sizes.push_back(sample.size());
            sample_buffer.insert(std::end(sample_buffer), std::begin(sample),
                                 std::end(sample));
        }
    }

    size_t dictionary_size = 100 * 1000;  // 100 kilobytes
    std::vector<char> dictionary(dictionary_size);

    size_t dict_size = ZDICT_trainFromBuffer(
        dictionary.data(), dictionary.size(), sample_buffer.data(),
        sample_sizes.data(), sample_sizes.size());

    if (ZDICT_isError(dict_size)) {
        dict_size = 0;
    }

    dictionary.resize(dict_size);

    {
        std::ofstream zdict(
            string_path / std::string("zdict"),
            std::ios_base::out | std::ios_base::binary | std::ios_base::trunc);
        zdict.write(dictionary.data(), dictionary.size());
    }

    std::vector<std::pair<size_t, std::vector<uint64_t>>> all_lengths(
        num_threads);

    run_all_simple(
        num_patients, num_threads,
        [&all_lengths, &property_readers, &string_path, &patient_lengths,
         &dictionary](size_t index, int start, int end) {
            std::filesystem::path target_path =
                string_path / (std::to_string(index) + ".data");
            all_lengths[index] =
                write_null_map<T>(property_readers[index], patient_lengths,
                                  start, end, dictionary, target_path);
        });

    uint64_t current_offset = (num_patients + 1) * sizeof(uint64_t);

    for (auto& length : all_lengths) {
        size_t temp = length.first;

        length.first = current_offset;

        current_offset += temp;
    }

    run_all_simple(num_patients, num_threads,
                   [&all_lengths](size_t index, int start, int end) {
                       auto& item = all_lengths[index];
                       for (auto& val : item.second) {
                           val += item.first;
                       }
                   });

    std::ofstream data_file((string_path / "data"), std::ios_base::out |
                                                        std::ios_base::binary |
                                                        std::ios_base::trunc);

    data_file.exceptions(std::ofstream::failbit | std::ofstream::badbit |
                         std::ofstream::eofbit);

    for (int i = 0; i < num_threads; i++) {
        auto& item = all_lengths[i];
        ssize_t num_to_write = item.second.size() * sizeof(uint64_t);
        const char* buffer = (const char*)item.second.data();
        data_file.write(buffer, num_to_write);
    }

    data_file.write((const char*)&current_offset, sizeof(current_offset));

    for (int i = 0; i < num_threads; i++) {
        std::filesystem::path entry_path =
            string_path / (std::to_string(i) + ".data");

        if (all_lengths[i].second.size() > 0) {
            std::ifstream entry_file(entry_path,
                                     std::ios_base::in | std::ios_base::binary);
            entry_file.exceptions(std::ofstream::failbit |
                                  std::ofstream::badbit);

            data_file << entry_file.rdbuf();
        }

        std::filesystem::remove(entry_path);
    }
}

}  // namespace

void create_database(const char* source, const char* destination,
                     int num_threads) {
    std::filesystem::path source_path(source);
    std::filesystem::path destination_path(destination);

    std::filesystem::create_directory(destination_path);

    {
        std::ofstream version_file(destination_path / "meds_reader.version");
        version_file << CURRENT_BINARY_VERSION << std::endl;
    }

    std::filesystem::copy(source_path / "metadata",
                          destination_path / "metadata");

    auto data = read_files(source_path / "data", num_threads);

    const auto& properties = data.first;

    const auto& work_entries = data.second;

    {
        std::ofstream property_file(
            destination_path / "meds_reader.properties",
            std::ios_base::out | std::ios_base::binary | std::ios_base::trunc);

        for (const auto& property : properties) {
            size_t name_length = property.first.size();
            property_file.write((const char*)&name_length, sizeof(name_length));
            property_file.write(property.first.data(), property.first.size());

            DataType type = convert_to_datatype(property.second);
            static_assert(sizeof(type) == sizeof(int64_t));
            int64_t casted_type = static_cast<int64_t>(type);
            property_file.write((const char*)&casted_type, sizeof(casted_type));
        }
    }

    auto patient_lengths_and_num_patients =
        process_patient_id(destination_path, work_entries, num_threads);

    const auto& patient_lengths = patient_lengths_and_num_patients.first;
    const auto& num_patients = patient_lengths_and_num_patients.second;

    for (const auto& property : properties) {
        process_property(property.first, property.second, patient_lengths,
                         destination_path, work_entries, num_threads);
    }

    if (properties.size() > 64) {
        throw std::runtime_error("Too many properties");
    } else if (properties.size() > 32) {
        process_null_map<uint64_t>(properties, destination_path, num_threads,
                                   num_patients);
    } else if (properties.size() > 16) {
        process_null_map<uint32_t>(properties, destination_path, num_threads,
                                   num_patients);
    } else if (properties.size() > 8) {
        process_null_map<uint16_t>(properties, destination_path, num_threads,
                                   num_patients);
    } else {
        process_null_map<uint8_t>(properties, destination_path, num_threads,
                                  num_patients);
    }
}
