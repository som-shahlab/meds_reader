#include "create_database.hh"

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
#include "blockingconcurrentqueue.h"
#include "dtype.hh"
#include "lightweightsemaphore.h"
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
        if (field.is_leaf()) {
            if (field.field->type()->id() != arrow::Type::NA) {
                result[field.field->name()] =
                    std::make_pair(field.field->type(), field.column_index);
            }
        } else {
            for (const auto& child : field.children) {
                to_process.emplace(std::move(child));
            }
        }
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

typedef std::tuple<std::filesystem::path, int64_t, int64_t, size_t> WorkEntry;

std::pair<std::map<std::string, std::shared_ptr<arrow::DataType>>,
          std::vector<WorkEntry>>
read_files(std::filesystem::path root_directory, int num_threads) {
    std::map<std::string, std::shared_ptr<arrow::DataType>> properties;

    std::vector<std::pair<std::filesystem::path, int64_t>> files_to_process;

    int total_row_groups = 0;

    for (const auto& dir_entry :
         std::filesystem::directory_iterator(root_directory)) {
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

        files_to_process.push_back(
            std::make_pair(dir_entry.path(), arrow_reader->num_row_groups()));
        total_row_groups += arrow_reader->num_row_groups();
    }

    int row_groups_per_thread =
        (total_row_groups + num_threads - 1) / num_threads;

    size_t current_file_index = 0;
    int current_row_groups_used = 0;

    std::vector<std::tuple<std::filesystem::path, int64_t, int64_t, size_t>>
        result;

    while (true) {
        if (current_file_index == files_to_process.size()) {
            break;
        }

        int remaining_row_groups = files_to_process[current_file_index].second -
                                   current_row_groups_used;
        int to_use = std::min(remaining_row_groups, row_groups_per_thread);

        size_t index = result.size();

        result.push_back(std::make_tuple(
            files_to_process[current_file_index].first, current_row_groups_used,
            current_row_groups_used + to_use, index));

        current_row_groups_used += to_use;

        if (current_row_groups_used ==
            files_to_process[current_file_index].second) {
            current_file_index++;
            current_row_groups_used = 0;
        }
    }

    return {properties, result};
}

std::set<std::string> known_properties = {"code", "text_value", "numeric_value",
                                          "datetime_value"};

template <typename F>
void iterate_strings(
    std::filesystem::path filename, int64_t start_row_group,
    int64_t end_row_group, std::string property_name,
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

    for (int64_t row_group = start_row_group; row_group < end_row_group;
         row_group++) {
        std::shared_ptr<arrow::Table> table;
        PARQUET_THROW_NOT_OK(
            arrow_reader->ReadRowGroup(row_group, columns, &table));

        auto chunked_events = table->GetColumnByName("events");

        for (const auto& event_chunk : chunked_events->chunks()) {
            auto events =
                std::dynamic_pointer_cast<arrow::ListArray>(event_chunk);
            if (events == nullptr) {
                throw std::runtime_error("Could not cast events");
            }

            auto event =
                std::dynamic_pointer_cast<arrow::StructArray>(events->values());
            if (event == nullptr) {
                throw std::runtime_error("Could not cast event");
            }

            if (known_properties.count(property_name) == 0) {
                event = std::dynamic_pointer_cast<arrow::StructArray>(
                    event->GetFieldByName("properties"));
                if (event == nullptr) {
                    throw std::runtime_error("Could not cast properties");
                }
            }

            auto array = event->GetFieldByName(property_name);
            if (array == nullptr) {
                throw std::runtime_error("Could not find property");
            }
            auto string_array =
                std::dynamic_pointer_cast<arrow::StringArray>(array);
            if (string_array == nullptr) {
                throw std::runtime_error("Could not cast property");
            }

            absl::flat_hash_map<std::string, uint32_t> per_patient_values;

            for (int64_t patient_id = 0; patient_id < events->length();
                 patient_id++) {
                per_patient_values.clear();

                std::vector<char> null_bytes;
                std::vector<uint32_t> text_value_lengths;
                std::vector<char> text_value_bytes;
                std::vector<uint32_t> values = {};

                text_value_lengths.push_back(0);

                size_t offset_in_bitset = 0;
                std::bitset<sizeof(uint64_t)* 8> bitset = {};

                auto num_events = events->value_length(patient_id);
                auto num_null_bytes =
                    (num_events + bitset.size() - 1) / bitset.size();

                null_bytes.resize(num_null_bytes * sizeof(uint64_t));

                uint64_t* nullmap_offset = (uint64_t*)null_bytes.data();

                auto flush = [&]() {
                    *nullmap_offset++ = bitset.to_ulong();
                    offset_in_bitset = 0;
                    bitset.reset();
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
                            text, per_patient_values.size() +
                                      dictionary_entries.size());

                        if (custom.second) {
                            text_value_lengths.push_back(text.size());
                            text_value_bytes.insert(std::end(text_value_bytes),
                                                    std::begin(text),
                                                    std::end(text));
                        }
                        values.push_back(custom.first->second);
                    }

                    bitset.set(offset_in_bitset);
                    offset_in_bitset++;
                    if (offset_in_bitset == bitset.size()) {
                        flush();
                    }
                };

                for (int64_t i = events->value_offset(patient_id);
                     i < events->value_offset(patient_id) +
                             events->value_length(patient_id);
                     i++) {
                    if (string_array->IsNull(i)) {
                        write_null();
                        continue;
                    }

                    std::string_view item = string_array->GetView(i);

                    if (item.empty()) {
                        write_null();
                        continue;
                    }

                    write_value(item);
                }

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
            }
        }
    }
}

std::vector<std::vector<char>> get_samples(
    std::filesystem::path filename, int64_t start_row_group,
    int64_t end_row_group, std::string property_name,
    const absl::flat_hash_map<std::string, size_t>& dictionary_entries,
    size_t num_samples) {
    size_t sample_count = 0;
    std::vector<std::vector<char>> samples;

    iterate_strings(filename, start_row_group, end_row_group, property_name,
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
    std::filesystem::path filename, int64_t start_row_group,
    int64_t end_row_group, std::string property_name,
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
        filename, start_row_group, end_row_group, property_name,
        dictionary_entries, [&](std::vector<char> bytes) {
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

void string_reader_thread(
    std::filesystem::path filename, int64_t start_row_group,
    int64_t end_row_group, std::string property_name,
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

    for (int64_t row_group = start_row_group; row_group < end_row_group;
         row_group++) {
        std::shared_ptr<arrow::Table> table;
        PARQUET_THROW_NOT_OK(
            arrow_reader->ReadRowGroup(row_group, columns, &table));

        auto chunked_events = table->GetColumnByName("events");

        for (const auto& event_chunk : chunked_events->chunks()) {
            auto events =
                std::dynamic_pointer_cast<arrow::ListArray>(event_chunk);
            if (events == nullptr) {
                throw std::runtime_error("Could not cast events");
            }

            auto event =
                std::dynamic_pointer_cast<arrow::StructArray>(events->values());
            if (event == nullptr) {
                throw std::runtime_error("Could not cast event");
            }

            if (known_properties.count(property_name) == 0) {
                event = std::dynamic_pointer_cast<arrow::StructArray>(
                    event->GetFieldByName("properties"));
                if (event == nullptr) {
                    throw std::runtime_error("Could not cast properties");
                }
            }

            auto array = event->GetFieldByName(property_name);
            if (array == nullptr) {
                throw std::runtime_error("Could not find property");
            }
            auto string_array =
                std::dynamic_pointer_cast<arrow::StringArray>(array);
            if (string_array == nullptr) {
                throw std::runtime_error("Could not cast property");
            }

            absl::flat_hash_map<std::string, uint64_t> items;

            for (int64_t patient_id = 0; patient_id < events->length();
                 patient_id++) {
                items.clear();

                for (int64_t i = events->value_offset(patient_id);
                     i < events->value_offset(patient_id) +
                             events->value_length(patient_id);
                     i++) {
                    if (string_array->IsNull(i)) {
                        continue;
                    }

                    std::string_view item = string_array->GetView(i);

                    if (item.empty()) {
                        continue;
                    }

                    items[item]++;
                }

                for (auto& item : items) {
                    size_t h = std::hash<std::string>{}(item.first);
                    size_t partition = h % all_write_queues.size();

                    item.second += (((uint64_t)1) << 32);

                    if (slots_to_write == 0) {
                        slots_to_write =
                            queue_semaphore.waitMany(SEMAPHORE_BLOCK_SIZE);
                    }
                    slots_to_write--;

                    all_write_queues[partition].enqueue(std::move(item));
                }
            }
        }
    }

    queue_semaphore.signal(slots_to_write);
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

void process_string_property(const std::string& property_name,
                             std::filesystem::path temp_path,
                             const std::vector<WorkEntry>& work_entries,
                             int num_threads) {
    std::filesystem::path string_path = temp_path / property_name;
    std::filesystem::create_directories(string_path);

    // We have to start by reading everything in

    std::vector<
        moodycamel::BlockingConcurrentQueue<std::pair<std::string, uint64_t>>>
        queues(num_threads);

    moodycamel::LightweightSemaphore write_semaphore(QUEUE_SIZE * num_threads);

    auto reader = [&property_name, &queues, &write_semaphore](
                      const std::filesystem::path fname, size_t start_row,
                      size_t end_row, size_t index) {
        string_reader_thread(fname, start_row, end_row, property_name, queues,
                             write_semaphore);
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

    run_all(
        work_entries, num_threads,
        [&all_samples, &property_name, &dictionary_entries,
         num_samples_per_entry](std::filesystem::path path, size_t start_row,
                                size_t end_row, size_t index) {
            all_samples[index] =
                get_samples(path, start_row, end_row, property_name,
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
            [&all_lengths, &string_path, &property_name, &dictionary_entries,
             &dictionary](std::filesystem::path path, size_t start_row,
                          size_t end_row, size_t index) {
                std::filesystem::path target_path =
                    string_path / (std::to_string(index) + ".data");
                all_lengths[index] =
                    write_files(path, start_row, end_row, property_name,
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
            [&all_lengths](std::filesystem::path path, size_t start_row,
                           size_t end_row, size_t index) {
                auto& item = all_lengths[index];
                for (auto& val : item.second) {
                    val += item.first;
                }
            });

    all_lengths.back().second.push_back(current_offset);

    std::ofstream data_file((string_path / "data"), std::ios_base::out |
                                                        std::ios_base::binary |
                                                        std::ios_base::trunc);

    for (const auto& entry : work_entries) {
        auto& item = all_lengths[std::get<3>(entry)];
        ssize_t num_to_write = item.second.size() * sizeof(uint64_t);
        const char* buffer = (const char*)item.second.data();
        data_file.write(buffer, num_to_write);
    }

    for (const auto& entry : work_entries) {
        std::filesystem::path entry_path =
            string_path / (std::to_string(std::get<3>(entry)) + ".data");

        std::ifstream entry_file(entry_path,
                                 std::ios_base::in | std::ios_base::binary);

        data_file << entry_file.rdbuf();

        std::filesystem::remove(entry_path);
    }
}

template <typename F>
void iterate_primitive(std::filesystem::path filename, int64_t start_row_group,
                       int64_t end_row_group, std::string property_name,
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

    for (int64_t row_group = start_row_group; row_group < end_row_group;
         row_group++) {
        std::shared_ptr<arrow::Table> table;
        PARQUET_THROW_NOT_OK(
            arrow_reader->ReadRowGroup(row_group, columns, &table));

        auto chunked_events = table->GetColumnByName("events");

        for (const auto& event_chunk : chunked_events->chunks()) {
            auto events =
                std::dynamic_pointer_cast<arrow::ListArray>(event_chunk);
            if (events == nullptr) {
                throw std::runtime_error("Could not cast events");
            }

            auto event =
                std::dynamic_pointer_cast<arrow::StructArray>(events->values());
            if (event == nullptr) {
                throw std::runtime_error("Could not cast event");
            }

            if (known_properties.count(property_name) == 0) {
                event = std::dynamic_pointer_cast<arrow::StructArray>(
                    event->GetFieldByName("properties"));
                if (event == nullptr) {
                    throw std::runtime_error("Could not cast properties");
                }
            }

            auto array = event->GetFieldByName(property_name);
            if (array == nullptr) {
                throw std::runtime_error("Could not find property");
            }
            auto primitive_array =
                std::dynamic_pointer_cast<arrow::PrimitiveArray>(array);
            if (primitive_array == nullptr) {
                throw std::runtime_error("Could not cast property");
            }

            int32_t type_bytes = primitive_array->type()->byte_width();

            if (type_bytes == -1) {
                throw std::runtime_error("Could not have a byte size of -1");
            }

            for (int64_t patient_id = 0; patient_id < events->length();
                 patient_id++) {
                std::vector<char> null_bytes;
                std::vector<char> value_bytes = {};

                size_t offset_in_bitset = 0;
                std::bitset<sizeof(uint64_t)* 8> bitset = {};

                auto num_events = events->value_length(patient_id);
                auto num_null_bytes =
                    (num_events + bitset.size() - 1) / bitset.size();

                null_bytes.resize(num_null_bytes * sizeof(uint64_t));

                uint64_t* nullmap_offset = (uint64_t*)null_bytes.data();

                auto flush = [&]() {
                    *nullmap_offset++ = bitset.to_ulong();
                    offset_in_bitset = 0;
                    bitset.reset();
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

                for (int64_t i = events->value_offset(patient_id);
                     i < events->value_offset(patient_id) + num_events; i++) {
                    if (primitive_array->IsNull(i)) {
                        write_null();
                    } else {
                        std::string_view item{
                            (const char*)primitive_array->values()->data() +
                                (primitive_array->offset() + i) * type_bytes,
                            (size_t)type_bytes};
                        write_value(item);
                    }
                }

                if (offset_in_bitset != 0) {
                    flush();
                }

                null_bytes.insert(std::end(null_bytes), std::begin(value_bytes),
                                  std::end(value_bytes));

                func(std::move(null_bytes));
            }
        }
    }
}

std::vector<std::vector<char>> get_primitive_samples(
    std::filesystem::path filename, int64_t start_row_group,
    int64_t end_row_group, std::string property_name, size_t num_samples) {
    std::vector<std::vector<char>> samples;
    size_t sample_count = 0;

    iterate_primitive(filename, start_row_group, end_row_group, property_name,
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
    std::filesystem::path filename, int64_t start_row_group,
    int64_t end_row_group, std::string property_name,
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

    iterate_primitive(
        filename, start_row_group, end_row_group, property_name,
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

void process_primitive_property(const std::string& property_name,
                                std::filesystem::path temp_path,
                                const std::vector<WorkEntry>& work_entries,
                                int num_threads) {
    std::filesystem::path string_path = temp_path / property_name;
    std::filesystem::create_directories(string_path);

    // We have to start by reading everything in

    std::vector<std::vector<std::vector<char>>> all_samples(
        work_entries.size());

    size_t num_samples_per_entry =
        (10 * 1000 + work_entries.size() - 1) / work_entries.size();

    run_all(work_entries, num_threads,
            [&all_samples, &property_name, num_samples_per_entry](
                std::filesystem::path path, size_t start_row, size_t end_row,
                size_t index) {
                all_samples[index] =
                    get_primitive_samples(path, start_row, end_row,
                                          property_name, num_samples_per_entry);
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
            [&all_lengths, &string_path, &property_name, &dictionary](
                std::filesystem::path path, size_t start_row, size_t end_row,
                size_t index) {
                std::filesystem::path target_path =
                    string_path / (std::to_string(index) + ".data");
                all_lengths[index] = write_primitive_files(
                    path, start_row, end_row, property_name, dictionary,
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
            [&all_lengths](std::filesystem::path path, size_t start_row,
                           size_t end_row, size_t index) {
                auto& item = all_lengths[index];
                for (auto& val : item.second) {
                    val += item.first;
                }
            });

    all_lengths.back().second.push_back(current_offset);

    std::ofstream data_file((string_path / "data"), std::ios_base::out |
                                                        std::ios_base::binary |
                                                        std::ios_base::trunc);

    for (const auto& entry : work_entries) {
        auto& item = all_lengths[std::get<3>(entry)];
        ssize_t num_to_write = item.second.size() * sizeof(uint64_t);
        const char* buffer = (const char*)item.second.data();
        data_file.write(buffer, num_to_write);
    }

    for (const auto& entry : work_entries) {
        std::filesystem::path entry_path =
            string_path / (std::to_string(std::get<3>(entry)) + ".data");

        std::ifstream entry_file(entry_path,
                                 std::ios_base::in | std::ios_base::binary);

        data_file << entry_file.rdbuf();

        std::filesystem::remove(entry_path);
    }
}

std::tuple<uint32_t, std::vector<int64_t>, std::vector<uint32_t>,
           std::vector<uint64_t>>
get_patient_ids(std::filesystem::path filename, int64_t start_row_group,
                int64_t end_row_group, std::filesystem::path target_path) {
    std::ofstream output_file(target_path, std::ios_base::out |
                                               std::ios_base::binary |
                                               std::ios_base::trunc);

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
    int64_t time_column = properties.find("time")->second.second;
    std::vector<int> columns = {(int)patient_id_column, (int)time_column};

    std::vector<int64_t> patient_ids;
    std::vector<uint32_t> lengths;
    std::vector<uint64_t> offsets;

    uint64_t num_bytes = 0;

    for (int64_t row_group = start_row_group; row_group < end_row_group;
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
                patient_ids.push_back(patient_id->Value(i));
            }
        }

        auto chunked_events = table->GetColumnByName("events");

        if (chunked_events == nullptr) {
            throw std::runtime_error("Could not get column");
        }

        for (const auto& event_chunk : chunked_events->chunks()) {
            auto events =
                std::dynamic_pointer_cast<arrow::ListArray>(event_chunk);
            if (events == nullptr) {
                throw std::runtime_error("Could not cast events");
            }

            auto event =
                std::dynamic_pointer_cast<arrow::StructArray>(events->values());
            if (event == nullptr) {
                throw std::runtime_error("Could not cast event");
            }

            auto time_array = std::dynamic_pointer_cast<arrow::TimestampArray>(
                event->GetFieldByName("time"));
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

            for (int64_t i = 0; i < events->length(); i++) {
                if (!events->IsValid(i)) {
                    throw std::runtime_error("Missing events?");
                }

                if (events->value_length(i) == 0) {
                    throw std::runtime_error("Zero events for patient?");
                }

                int64_t starting_index = events->value_offset(i);
                if (time_array->IsNull(starting_index)) {
                    throw std::runtime_error("First time is not valid?");
                }

                constexpr int64_t micros_per_second = ((int64_t)1000) * 1000;
                constexpr int64_t micros_per_day =
                    micros_per_second * 24 * 60 * 60;

                int64_t starting_timestamp = time_array->Value(starting_index);

                int64_t last_timestamp = starting_timestamp;

                int64_t current_timestamp = starting_timestamp;
                int64_t current_timestamp_count = 1;

                std::vector<uint32_t> values;

                auto flush_timestamp = [&]() {
                    while (current_timestamp_count != 0) {
                        int64_t delta = current_timestamp - last_timestamp;

                        int64_t delta_days = delta / micros_per_day;
                        int64_t delta_seconds =
                            (delta % micros_per_day) / micros_per_second;
                        int64_t delta_micros = delta % micros_per_second;

                        if (delta_days < 0 || delta_seconds < 0 ||
                            delta_micros < 0) {
                            throw std::runtime_error("Times are not in order");
                        }

                        int64_t num_to_emit =
                            std::min(current_timestamp_count, (int64_t)15);

                        if (delta_seconds == 0 && delta_micros == 0) {
                            values.push_back((delta_days << 4) + num_to_emit);
                        } else if (delta_micros == 0) {
                            values.push_back((delta_days << 4));
                            values.push_back((delta_seconds << 4) +
                                             num_to_emit);
                        } else {
                            values.push_back((delta_days << 4));
                            values.push_back((delta_seconds << 4));
                            values.push_back((delta_micros << 4) + num_to_emit);
                        }

                        current_timestamp_count -= num_to_emit;

                        last_timestamp = current_timestamp;
                    }
                };

                for (int64_t j = starting_index + 1;
                     j < starting_index + events->value_length(i); j++) {
                    if (event->IsNull(j) || time_array->IsNull(j)) {
                        throw std::runtime_error("Null time");
                    }

                    auto val = time_array->Value(j);

                    if (val == current_timestamp) {
                        current_timestamp_count++;
                    } else {
                        flush_timestamp();
                        current_timestamp = val;
                        current_timestamp_count = 1;
                    }
                }

                flush_timestamp();

                std::vector<char> helper(
                    sizeof(int64_t) + sizeof(uint32_t) +
                    streamvbyte_max_compressedbytes(values.size()));
                size_t count = streamvbyte_encode_0124(
                    values.data(), values.size(),
                    (uint8_t*)helper.data() + sizeof(uint32_t) +
                        sizeof(int64_t));

                int64_t* start_pointer = (int64_t*)(helper.data());
                *start_pointer = starting_timestamp;

                uint32_t* length_pointer =
                    (uint32_t*)(helper.data() + sizeof(int64_t));
                *length_pointer = values.size();

                helper.resize(sizeof(int64_t) + sizeof(uint32_t) + count);

                offsets.push_back(num_bytes);
                lengths.push_back(events->value_length(i));

                num_bytes += helper.size();

                output_file.write(helper.data(), helper.size());
            }
        }
    }

    return {num_bytes, std::move(patient_ids), std::move(lengths),
            std::move(offsets)};
}

void process_patient_id_and_time(std::filesystem::path temp_path,
                                 const std::vector<WorkEntry>& work_entries,
                                 int num_threads) {
    std::vector<std::tuple<uint32_t, std::vector<int64_t>,
                           std::vector<uint32_t>, std::vector<uint64_t>>>
        all_lengths(work_entries.size());

    run_all(
        work_entries, num_threads,
        [&temp_path, &all_lengths](std::filesystem::path path, size_t start_row,
                                   size_t end_row, size_t index) {
            std::filesystem::path target_path =
                temp_path / (std::to_string(index) + ".data");
            all_lengths[index] =
                get_patient_ids(path, start_row, end_row, target_path);
        });

    size_t num_patients = 0;

    for (const auto& length : all_lengths) {
        num_patients += std::get<1>(length).size();
    }

    uint64_t current_offset = (num_patients + 1) * sizeof(uint64_t);

    for (auto& length : all_lengths) {
        size_t temp = std::get<0>(length);

        std::get<0>(length) = current_offset;

        current_offset += temp;
    }

    run_all(work_entries, num_threads,
            [&all_lengths](std::filesystem::path path, size_t start_row,
                           size_t end_row, size_t index) {
                auto& item = all_lengths[index];
                for (auto& val : std::get<3>(item)) {
                    val += std::get<0>(item);
                }
            });

    std::get<3>(all_lengths.back()).push_back(current_offset);

    {
        std::ofstream pids_file(
            temp_path / "patient_id",
            std::ios_base::out | std::ios_base::binary | std::ios_base::trunc);
        std::ofstream lengths_file(
            temp_path / "length",
            std::ios_base::out | std::ios_base::binary | std::ios_base::trunc);

        std::ofstream data_file(
            (temp_path / "time"),
            std::ios_base::out | std::ios_base::binary | std::ios_base::trunc);

        absl::flat_hash_set<int64_t> seen_patient_ids;

        for (const auto& entry : work_entries) {
            const auto& item = all_lengths[std::get<3>(entry)];
            pids_file.write((const char*)std::get<1>(item).data(),
                            sizeof(int64_t) * std::get<1>(item).size());
            for (int64_t pid : std::get<1>(item)) {
                if (seen_patient_ids.count(pid) != 0) {
                    throw std::runtime_error("Had duplicate patient ids " +
                                             std::to_string(pid));
                }
                seen_patient_ids.insert(pid);
            }
            lengths_file.write((const char*)std::get<2>(item).data(),
                               sizeof(uint32_t) * std::get<2>(item).size());

            ssize_t num_to_write = std::get<3>(item).size() * sizeof(uint64_t);
            const char* buffer = (const char*)std::get<3>(item).data();

            data_file.write(buffer, num_to_write);
        }

        for (const auto& entry : work_entries) {
            size_t index = std::get<3>(entry);

            std::filesystem::path entry_path =
                temp_path / (std::to_string(index) + ".data");

            std::ifstream entry_file(entry_path,
                                     std::ios_base::in | std::ios_base::binary);

            data_file << entry_file.rdbuf();

            std::filesystem::remove(entry_path);
        }

        char nonsense_buffer[STREAMVBYTE_PADDING] = "PADDING";
        data_file.write(nonsense_buffer, STREAMVBYTE_PADDING);
    }
}

DataType convert_to_datatype(const std::shared_ptr<arrow::DataType>& type) {
    switch (type->id()) {
        case arrow::Type::STRING:
            return DataType::STRING;

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
                      std::filesystem::path temp_path,
                      const std::vector<WorkEntry>& work_entries,
                      int num_threads) {
    switch (type->id()) {
        case arrow::Type::STRING:
            process_string_property(property_name, temp_path, work_entries,
                                    num_threads);
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
            process_primitive_property(property_name, temp_path, work_entries,
                                       num_threads);
            return;

        default:
            throw std::runtime_error("Unsupported type " + type->ToString());
    };

    // Unreachable
    abort();
}

}  // namespace

void create_database(const char* source, const char* destination,
                     int num_threads) {
    std::filesystem::path source_path(source);
    std::filesystem::path destination_path(destination);

    std::filesystem::create_directory(destination_path);

    std::filesystem::copy(source_path / "metadata.json",
                          destination_path / "metadata.json");

    auto data = read_files(source_path / "data", num_threads);

    const auto& properties = data.first;

    const auto& work_entries = data.second;

    {
        std::ofstream property_file(
            destination_path / "properties",
            std::ios_base::out | std::ios_base::binary | std::ios_base::trunc);

        for (const auto& property : properties) {
            if (property.first == "patient_id") {
                continue;
            }

            size_t name_length = property.first.size();
            property_file.write((const char*)&name_length, sizeof(name_length));
            property_file.write(property.first.data(), property.first.size());

            DataType type = convert_to_datatype(property.second);
            static_assert(sizeof(type) == sizeof(int64_t));
            int64_t casted_type = static_cast<int64_t>(type);
            property_file.write((const char*)&casted_type, sizeof(casted_type));
        }
    }

    process_patient_id_and_time(destination_path, work_entries, num_threads);

    for (const auto& property : properties) {
        if (property.first == "patient_id" || property.first == "time") {
            continue;
        }

        std::cout << "Processing " << property.first << std::endl;

        process_property(property.first, property.second, destination_path,
                         work_entries, num_threads);
    }
}
