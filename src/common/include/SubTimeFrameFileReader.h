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

#ifndef ALICEO2_SUBTIMEFRAME_FILE_READER_H_
#define ALICEO2_SUBTIMEFRAME_FILE_READER_H_

#include "SubTimeFrameDataModel.h"
#include <Headers/DataHeader.h>
#include <Headers/Stack.h>

#include <boost/filesystem.hpp>
#include <fstream>
#include <vector>

namespace o2
{
namespace DataDistribution
{

class SubTimeFrameFileBuilder;

////////////////////////////////////////////////////////////////////////////////
/// SubTimeFrameFileReader
////////////////////////////////////////////////////////////////////////////////

class SubTimeFrameFileReader : public ISubTimeFrameVisitor
{
 public:
  SubTimeFrameFileReader() = delete;
  SubTimeFrameFileReader(boost::filesystem::path& pFileName);
  ~SubTimeFrameFileReader();

  ///
  /// Read a single TF from the file
  ///
  std::unique_ptr<SubTimeFrame> read(SubTimeFrameFileBuilder &pFileBuilder);

  ///
  /// Tell the current position of the file
  ///
  std::uint64_t position() { return std::uint64_t(mFile.tellg()); }

  ///
  /// Tell the size of the file
  ///
  std::uint64_t size() const { return mFileSize; }

 private:
  void visit(SubTimeFrame& pStf) override;

  std::ifstream mFile;
  std::uint64_t mFileSize;

  // helper to make sure written chunks are buffered, only allow pointers
  template <typename pointer,
            typename = std::enable_if_t<std::is_pointer<pointer>::value>>
  std::istream& buffered_read(pointer pPtr, std::streamsize pLen)
  {
    return mFile.read(reinterpret_cast<char*>(pPtr), pLen);
  }

  std::size_t getHeaderStackSize();
  o2::header::Stack getHeaderStack(std::size_t *pOrigsize = nullptr);

  // vector of <hdr, fmqMsg> elements of a tf read from the file
  std::vector<SubTimeFrame::StfData> mStfData;


  // flags for upgrading DataHeader versions
  static std::uint64_t sStfId; // TODO: add id to files metadata

};
}
} /* o2::DataDistribution */

#endif /* ALICEO2_SUBTIMEFRAME_FILE_READER_H_ */
