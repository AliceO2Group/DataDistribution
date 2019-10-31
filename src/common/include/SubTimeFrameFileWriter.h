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

#ifndef ALICEO2_SUBTIMEFRAME_FILE_WRITER_H_
#define ALICEO2_SUBTIMEFRAME_FILE_WRITER_H_

#include "SubTimeFrameDataModel.h"
#include "SubTimeFrameFile.h"
#include <Headers/DataHeader.h>

#include <type_traits>
#include <boost/filesystem.hpp>
#include <fstream>
#include <vector>

namespace o2
{
namespace DataDistribution
{

////////////////////////////////////////////////////////////////////////////////
/// SubTimeFrameFileWriter
////////////////////////////////////////////////////////////////////////////////

class SubTimeFrameFileWriter : public ISubTimeFrameConstVisitor
{
  static const constexpr char* sSidecarFieldSep = " ";
  static const constexpr char* sSidecarRecordSep = "\n";

 public:
  SubTimeFrameFileWriter() = delete;
  SubTimeFrameFileWriter(const boost::filesystem::path& pFileName, bool pWriteInfo = false);
  ~SubTimeFrameFileWriter();

  ///
  /// Writes a (Sub)TimeFrame
  ///
  std::uint64_t write(const SubTimeFrame& pStf);

  ///
  /// Tell current size of the file
  ///
  std::uint64_t size() { return std::uint64_t(mFile.tellp()); }

 private:
  void visit(const SubTimeFrame& pStf) override;

  /// Writes a (Sub)TimeFrame
  std::uint64_t _write(const SubTimeFrame& pStf);

  //
  //  workaround for buffered operation (performance):
  //   - provide a new, larger, buffer
  //   - always write less than 1024B at a time (hard limit in libstdc++)
  //
  static constexpr std::streamsize sBuffSize = 256ul << 10; // 256 kiB
  static constexpr std::streamsize sChunkSize = 512;
  std::ofstream mFile;

  bool mWriteInfo;
  std::ofstream mInfoFile;

  std::unique_ptr<char[]> mFileBuf;
  std::unique_ptr<char[]> mInfoFileBuf;

  // helper to make sure the written blocks are buffered
  template <
    typename pointer,
    typename std::enable_if<
      std::is_pointer<pointer>::value &&                      // pointers only
      (std::is_void<std::remove_pointer_t<pointer>>::value || // void* or standard layout!
       std::is_standard_layout<std::remove_pointer_t<pointer>>::value)>::type* = nullptr>
  void buffered_write(const pointer p, std::streamsize pCount)
  {
    // make sure we're not doing a short write
    assert((pCount % sizeof(std::conditional_t<std::is_void<std::remove_pointer_t<pointer>>::value,
            char, std::remove_pointer_t<pointer>>) == 0) && "Performing short write?");

    const char* lPtr = reinterpret_cast<const char*>(p);
    // avoid the optimization if the write is large enough
    if (pCount >= sBuffSize) {
      mFile.write(lPtr, pCount);
    } else {
      // split the write to smaller chunks
      while (pCount > 0) {
        const auto lToWrite = std::min(pCount, sChunkSize);
        assert(lToWrite > 0 && lToWrite <= sChunkSize && lToWrite <= pCount);

        mFile.write(lPtr, lToWrite);
        lPtr += lToWrite;
        pCount -= lToWrite;
      }
    }
  }

  std::uint64_t getSizeInFile() const;

  // vector of <headers, data> elements of a Stf to be written
  std::vector<const SubTimeFrame::StfData*> mStfData;
  SubTimeFrameFileDataIndex mStfDataIndex;
  std::uint64_t mStfSize = std::uint64_t(0); // meta + index + data (and all headers)
};
}
} /* o2::DataDistribution */

#endif /* ALICEO2_SUBTIMEFRAME_FILE_WRITER_H_ */
