#include "FileUtils.h"

#include <boost/filesystem/string_file.hpp>

namespace hf3fs {
Result<std::string> loadFile(const Path &path) {
  try {
    std::string output;
    boost::filesystem::load_string_file(path, output);
    return output;
  } catch (const std::exception &e) {
    return makeError(StatusCode::kIOError, fmt::format("Error when read {}: {}", path, e.what()));
  }
}

Result<Void> storeToFile(const Path &path, const std::string &content) {
  try {
    boost::filesystem::save_string_file(path, content);
    return Void{};
  } catch (const std::exception &e) {
    return makeError(StatusCode::kIOError, fmt::format("Error when write to {}: {}", path, e.what()));
  }
}

}  // namespace hf3fs
