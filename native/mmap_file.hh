#pragma once

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include <filesystem>
#include <string_view>

#include "absl/types/span.h"

class MmapFile {
   public:
    template <typename P>
    MmapFile(const P& path) {
        fp = open(path.c_str(), O_RDONLY);
        if (fp == -1) {
            throw std::runtime_error(
                std::string("Could not open the following path ") +
                std::string(path) + " " + std::string(strerror(errno)));
        }
        data_size = std::filesystem::file_size(path);
        if (data_size != 0) {
            data_pointer =
                mmap(nullptr, data_size, PROT_READ, MAP_PRIVATE, fp, 0);
            if (data_pointer == MAP_FAILED) {
                throw std::runtime_error(
                    std::string("Could not mmap the requested file ") +
                    std::string(path) + " " + std::string(strerror(errno)));
            }
        } else {
            data_pointer = nullptr;
        }
    }

    MmapFile(MmapFile&& other) {
        fp = other.fp;
        data_size = other.data_size;
        data_pointer = other.data_pointer;

        other.fp = 0;
        other.data_size = 0;
        other.data_pointer = nullptr;
    }

    MmapFile(const MmapFile&) = delete;
    MmapFile& operator=(const MmapFile& other) = delete;

    std::string_view bytes() const {
        return std::string_view((const char*)data_pointer, data_size);
    }

    template <typename T>
    absl::Span<const T> data() const {
        return absl::Span<const T>((const T*)data_pointer,
                                   data_size / sizeof(T));
    }

    ~MmapFile() {
        if (data_pointer != nullptr) {
            munmap(data_pointer, data_size);
            close(fp);
        }
    }

   private:
    int fp;
    size_t data_size;
    void* data_pointer;
};
