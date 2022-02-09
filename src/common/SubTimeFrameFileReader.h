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
#include <boost/iostreams/device/mapped_file.hpp>
#include <fstream>
#include <vector>

namespace o2::DataDistribution
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
  inline
  std::uint64_t position() const { return mFileMapOffset; }

  ///
  /// Set the current position of the file
  ///
  inline
  void set_position(std::uint64_t pPos)
  {
    const std::uint64_t lPos = std::min(pPos, mFileSize);
    assert(pPos == lPos);

    mFileMapOffset = lPos;
  }

  ///
  /// Is the stream position at EOF
  ///
  inline
  bool eof() const { return mFileMapOffset == mFileSize; }

  ///
  /// Tell the size of the file
  ///
  inline
  std::uint64_t size() const { return mFileSize; }

 private:
  void visit(SubTimeFrame& pStf, void*) override;

  std::string mFileName;
  boost::iostreams::mapped_file_source mFileMap;
  std::uint64_t mFileMapOffset = 0;
  std::uint64_t mFileSize = 0;

  // helper to make sure written chunks are buffered, only allow pointers
  template <typename pointer,
            typename = std::enable_if_t<std::is_pointer<pointer>::value>>
  bool read_advance(pointer pPtr, std::uint64_t pLen)
  {
    if (!mFileMap.is_open()) {
      return false;
    }

    assert(mFileMapOffset <= mFileSize);
    const std::uint64_t lToRead = std::min(pLen, mFileSize - mFileMapOffset);

    if (lToRead != pLen) {
      EDDLOG("FileReader: request to read beyond the file end. pos={} size={} len={}",
        mFileMapOffset, mFileSize, pLen);
      EDDLOG("Closing the file {}. The read data is invalid.", mFileName);
      mFileMap.close(); mFileMapOffset = 0; mFileSize = 0;
      return false;
    }

    std::memcpy(reinterpret_cast<char*>(pPtr), mFileMap.data() + mFileMapOffset, lToRead);
    mFileMapOffset += lToRead;
    return true;
  }

  // return the pointer
  unsigned char* peek() const
  {
    return const_cast<unsigned char*>(reinterpret_cast<const unsigned char*>(mFileMap.data() + mFileMapOffset));
  }

  inline
  bool ignore_nbytes(const std::size_t pLen)
  {
    const std::size_t lToIgnore = std::min(pLen, std::size_t(mFileSize - mFileMapOffset));
    if (pLen != lToIgnore) {
      EDDLOG("FileReader: request to ignore bytes beyond the file end. pos={} size={} len={}",
        mFileMapOffset, mFileSize, pLen);
      EDDLOG("Closing the file {}. The read data is invalid.", mFileName);
      mFileMap.close(); mFileMapOffset = 0; mFileSize = 0;
      return false;
    }

    mFileMapOffset += lToIgnore;
    assert(mFileMapOffset <= mFileSize);
    return true;
  }

  std::size_t getHeaderStackSize();
  o2::header::Stack getHeaderStack(std::size_t &pOrigsize);

  // vector of <hdr, fmqMsg> elements of a tf read from the file
  std::vector<SubTimeFrame::StfData> mStfData;

  // flags for upgrading DataHeader versions
  static std::uint64_t sStfId;

};

} /* o2::DataDistribution */

#endif /* ALICEO2_SUBTIMEFRAME_FILE_READER_H_ */
