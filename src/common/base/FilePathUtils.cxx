// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.

// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <https://www.gnu.org/licenses/>.

#include <boost/filesystem.hpp>
#include <boost/range/iterator_range.hpp>
#include <boost/algorithm/string/replace.hpp>
#include <regex>

#include <string>
#include <sstream>
#include <iomanip>
#include <chrono>

#include "FilePathUtils.h"

namespace o2
{
namespace DataDistribution
{

namespace fsb = boost::filesystem;

static
std::string getDateTimeStr()
{
  using namespace std::chrono;
  std::ostringstream ss;
  const auto lTimet = std::chrono::system_clock::to_time_t(system_clock::now());
  ss << std::put_time(gmtime(&lTimet), "%FT%TZ");
  return boost::replace_all_copy(ss.str(), ":", "_");
}

std::string FilePathUtils::getDataDirName(const std::string& pRootDir)
{
  const fsb::path lRootPath(pRootDir);

  // check if root directory exists
  if (!fsb::is_directory(lRootPath)) {
    using namespace std::string_literals;
    throw std::invalid_argument("'"s + pRootDir + "' is not a directory"s);
  }

  // storage dir is has format of ISO datetime string
  const std::string lNowString = o2::DataDistribution::getDateTimeStr();

  // see if already exists, add sequence number if so
  std::string lCheckName = lNowString;
  std::uint64_t lCurrSeq = 0;
  do {
    for (auto& entry : boost::make_iterator_range(fsb::directory_iterator(lRootPath), {})) {
      std::smatch result;
      const std::string lBaseName = entry.path().filename().string();
      if (lBaseName == lNowString) {
        lCurrSeq++;
        lCheckName += lNowString + "_" + std::to_string(lCurrSeq);
        continue;
      }
    }
  } while (0);

  return lCheckName;
}


std::string FilePathUtils::getNextSeqName(const std::string& pRootDir)
{
  static const std::regex seq_regex("(\\d+)(?=\\D*$)", std::regex::icase);

  fsb::path lRootPath(pRootDir);

  // check if root directory exists
  if (!fsb::is_directory(lRootPath)) {
    using namespace std::string_literals;
    throw std::invalid_argument("'"s + pRootDir + "' is not a directory"s);
  }

  // try to match the elements
  std::string lNameMatch;
  std::uint64_t lMaxSeq = 0;
  std::size_t lLen = 1;
  std::string lPrefix, lSuffix;

  for (auto& entry : boost::make_iterator_range(fsb::directory_iterator(lRootPath), {})) {
    std::smatch result;
    const std::string lBaseName = entry.path().filename().string();
    if (std::regex_search(lBaseName, result, seq_regex)) {

      const std::uint64_t lCurrSeq = std::stoull(result[1]) + 1;
      if (lCurrSeq >= lMaxSeq) {
        lMaxSeq = lCurrSeq;
        lLen = std::max(lLen, std::size_t(result[1].length()));
        lNameMatch = lBaseName;
        lPrefix = result.prefix().str();
        lSuffix = result.suffix().str();
      }
    }
  }

  // make sure the length is large enough
  lLen = std::max(lLen, std::to_string(lMaxSeq).length());

  // replace the string sequence
  if (lNameMatch.length() > 0) {
    std::stringstream lRet;
    lRet << std::dec << std::setw(lLen) << std::setfill('0') << lMaxSeq;
    lNameMatch = lPrefix + lRet.str() + lSuffix;
  } else {
    lNameMatch = std::to_string(lMaxSeq);
  }

  return lNameMatch;
}

std::vector<std::string> FilePathUtils::getAllFiles(const std::string& pDir)
{
  std::vector<std::string> lFileNames;

  for (auto& entry : boost::make_iterator_range(fsb::directory_iterator(pDir), {})) {

    const std::string lBaseName = entry.path().filename().string();
    if (is_regular_file(entry.path())) {
      lFileNames.push_back(lBaseName);
    }
  }

  std::sort(lFileNames.begin(), lFileNames.end());

  return lFileNames;
}


}
} /* o2::DataDistribution */
