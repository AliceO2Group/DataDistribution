// Copyright 2019-2022 CERN and copyright holders of ALICE O2.
// See https://alice-o2.web.cern.ch/copyright for details of the copyright holders.
// All rights not expressly granted are reserved.
//
// This software is distributed under the terms of the GNU General Public
// License v3 (GPL Version 3), copied verbatim in the file "COPYING".
//
// In applying this license CERN does not waive the privileges and immunities
// granted to it by virtue of its status as an Intergovernmental Organization
// or submit itself to any jurisdiction.

/// \author Gvozden Nešković, Frankfurt Institute for Advanced Studies and Goethe University Frankfurt

#ifndef ALICEO2_SUBTIMEFRAME_FILE_WRITER_H_
#define ALICEO2_SUBTIMEFRAME_FILE_WRITER_H_

#include "SubTimeFrameDataModel.h"
#include "SubTimeFrameFile.h"
#include <Headers/DataHeader.h>

#include <type_traits>
#include <boost/filesystem.hpp>
#include <fstream>
#include <vector>
#include <optional>

namespace o2::DataDistribution
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
  SubTimeFrameFileWriter(const boost::filesystem::path& pFileName, bool pWriteInfo, std::optional<EosMetadata> pEosMetadata);
  virtual ~SubTimeFrameFileWriter();

  ///
  /// Writes a (Sub)TimeFrame
  ///
  std::uint64_t write(const SubTimeFrame& pStf);

  ///
  /// Tell current size of the file
  ///
  std::uint64_t size() { return std::uint64_t(mFile.tellp()); }

  ///
  /// Delete the (Sub)TimeFrame file on error
  ///
  void remove();

  ///
  /// Close the (Sub)TimeFrame file on error
  ///
  void close();

 private:


  void visit(const SubTimeFrame& pStf, void*) override;

  /// Writes a (Sub)TimeFrame
  std::uint64_t _write(const SubTimeFrame& pStf);

  //
  //  workaround for buffered operation (performance):
  //   - provide a new, larger, buffer
  //   - always write less than 1024B at a time (hard limit in libstdc++)
  //
  static constexpr std::streamsize sBuffSize = 1ul << 20; // 1 MiB
  static constexpr std::streamsize sChunkSize = 512;
  boost::filesystem::path mFileName;
  std::ofstream mFile;
  bool mRemoved = false;

  bool mWriteInfo;
  std::ofstream mInfoFile;

  std::optional<EosMetadata> mEosMetadata;
  boost::filesystem::path mMetaFileName;

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
  std::vector<const SubTimeFrame::StfMessage*> mStfData;
  SubTimeFrameFileDataIndex mStfDataIndex;
  std::uint64_t mStfSize = std::uint64_t(0); // meta + index + data (and all headers)
};

} /* o2::DataDistribution */

#endif /* ALICEO2_SUBTIMEFRAME_FILE_WRITER_H_ */
