// Provides a small memory-mapped file wrapper.
//
// This header wraps boost::iostreams mapped_file_source and provides
// byte/span views used throughout the native reader/writer pipeline.
#include <cstring>
#include <filesystem>
#include <string_view>

#include "absl/types/span.h"
#include <boost/iostreams/device/mapped_file.hpp>

// Logs a filename and returns it for quick debugging.
inline std::string debug_me(std::string a){
    std::cout<<"About to open" << a << std::endl;
    return a;
}

class MmapFile {
   public:
    // Opens a file by filesystem path.
    MmapFile(const std::filesystem::path& path, bool allow_missing = false)
        : MmapFile(path.string(), allow_missing) {}
    // Opens a file by string path and maps it into memory.
    MmapFile(const std::string& path, bool allow_missing = false) {
        if (allow_missing && !std::filesystem::exists(path)) {
            is_empty = true;
            return;
        }
        std::uintmax_t size = std::filesystem::file_size(path);
        if (size == 0) {
            is_empty = true;
        } else {
            is_empty = false;
            file.open(path);
        }
    }

    // Moves ownership of the mapped file.
    MmapFile(MmapFile&& other): is_empty(other.is_empty), file(std::move(other.file)) {}

    MmapFile(const MmapFile&) = delete;
    MmapFile& operator=(const MmapFile& other) = delete;

    // Returns the raw file bytes as a string_view.
    std::string_view bytes() const {
        if (is_empty) {
            return std::string_view(nullptr, 0);
        } else {
            return std::string_view((const char*)file.data(), file.size());
        }
    }

    template <typename T>
    // Returns the mapped data as a typed span.
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
