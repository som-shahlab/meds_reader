#include <cstring>
#include <filesystem>
#include <string_view>

#include "absl/types/span.h"
#include <boost/iostreams/device/mapped_file.hpp>

inline std::string debug_me(std::string a){
    std::cout<<"About to open" << a << std::endl;
    return a;
}

class MmapFile {
   public:
    MmapFile(const std::filesystem::path& path): MmapFile(path.string()) {}
    MmapFile(const std::string& path) {
        std::uintmax_t size = std::filesystem::file_size(path);
        if (size == 0) {
            is_empty = true;
        } else {
            is_empty = false;
            file.open(path);
        }
    }

    MmapFile(MmapFile&& other): is_empty(other.is_empty), file(std::move(other.file)) {}

    MmapFile(const MmapFile&) = delete;
    MmapFile& operator=(const MmapFile& other) = delete;

    std::string_view bytes() const {
        if (is_empty) {
            return std::string_view(nullptr, 0);
        } else {
            return std::string_view((const char*)file.data(), file.size());
        }
    }

    template <typename T>
    absl::Span<const T> data() const {
        if (is_empty) {
            return absl::Span<const T>(nullptr, 0);
        } else {
            
            return absl::Span<const T>((const T*)file.data(),
                file.size() / sizeof(T));
        }
    }

   private:
    bool is_empty;
    boost::iostreams::mapped_file_source file;
};
