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
        data_size = std::filesystem::file_size(path);
        data_pointer = mmap(nullptr, data_size, PROT_READ, MAP_PRIVATE, fp, 0);
    }

    std::string_view bytes() const {
        return std::string_view((const char*)data_pointer, data_size);
    }

    template <typename T>
    absl::Span<const T> data() const {
        return absl::Span<const T>((const T*)data_pointer,
                                   data_size / sizeof(T));
    }

    ~MmapFile() {
        munmap(data_pointer, data_size);
        close(fp);
    }

   private:
    int fp;
    size_t data_size;
    void* data_pointer;
};