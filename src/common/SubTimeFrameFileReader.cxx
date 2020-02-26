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

#include "SubTimeFrameFile.h"
#include "SubTimeFrameFileReader.h"

#include "DataDistLogger.h"

namespace o2
{
namespace DataDistribution
{

using namespace o2::header;

////////////////////////////////////////////////////////////////////////////////
/// SubTimeFrameFileReader
////////////////////////////////////////////////////////////////////////////////

SubTimeFrameFileReader::SubTimeFrameFileReader(boost::filesystem::path& pFileName)
{
  using ios = std::ios_base;

  try {
    mFile.open(pFileName.string(), ios::binary | ios::in);
    mFile.exceptions(std::fstream::failbit | std::fstream::badbit);

    // get the file size
    mFile.seekg(0, std::ios_base::end);
    mFileSize = mFile.tellg();
    mFile.seekg(0, std::ios_base::beg);

  } catch (std::ifstream::failure& eOpenErr) {
    DDLOG(fair::Severity::ERROR) << "Failed to open TF file for reading. Error: " << eOpenErr.what();
  } catch (std::exception &err) {
    DDLOG(fair::Severity::ERROR) << "Failed to open TF file for reading. Error: " << err.what();
  }

  // DDLOG(fair::Severity::DEBUG) << "Opened new STF file for reading: " << pFileName.string();
}

SubTimeFrameFileReader::~SubTimeFrameFileReader()
{
  try {
    if (mFile.is_open())
      mFile.close();
  } catch (std::ifstream::failure& eCloseErr) {
    DDLOG(fair::Severity::ERROR) << "Closing TF file failed. Error: " << eCloseErr.what();
  } catch (...) {
    DDLOG(fair::Severity::ERROR) << "Closing TF file failed.";
  }
}

void SubTimeFrameFileReader::visit(SubTimeFrame& pStf)
{
  for (auto& lStfDataPair : mStfData) {
    pStf.addStfData(std::move(lStfDataPair));
  }
}

std::int64_t SubTimeFrameFileReader::getHeaderStackSize() // throws ios_base::failure
{
  return sizeof(DataHeader); // TODO: allow arbitrary header stacks

#if 0
  std::int64_t lHdrStackSize = 0;

  const auto lStartPos = mFile.tellg();

  DataHeader lBaseHdr;

  do {
    buffered_read(&lBaseHdr, sizeof(BaseHeader));
    if (nullptr == BaseHeader::get(reinterpret_cast<o2::byte*>(&lBaseHdr))) {
      // error: expected a header here
      return -1;
    } else {
      lHdrStackSize += lBaseHdr.headerSize;
    }

    // skip the rest of the current header
    if (lBaseHdr.headerSize > sizeof(BaseHeader)) {
      mFile.ignore(lBaseHdr.headerSize - sizeof(BaseHeader));
    } else {
      // error: invalid header size value
      return -1;
    }
  } while (lBaseHdr.next() != nullptr);

  // we should not eof here,
  if (mFile.eof())
    return -1;

  // rewind the file to the start of Header stack
  mFile.seekg(lStartPos);

  return lHdrStackSize;
#endif
}

std::unique_ptr<SubTimeFrame> SubTimeFrameFileReader::read(FairMQChannel& pDstChan)
{
  // TODO: add id to files metadata
  static std::uint64_t sStfId = 0;

  // make sure headers and chunk pointers don't linger
  mStfData.clear();

  // record current position
  const auto lTfStartPosition = position();

  if (lTfStartPosition == size()) {
    return nullptr;
  }

  // If mFile is good, we're positioned to read a TF
  if (!mFile || mFile.eof()) {
    return nullptr;
  }

  if (!mFile.good()) {
   DDLOG(fair::Severity::WARNING) << "Error while reading a TF from file. (bad stream state)";
    return nullptr;
  }

  // NOTE: StfID will be updated from the stf header
  std::unique_ptr<SubTimeFrame> lStf = std::make_unique<SubTimeFrame>(sStfId++);

  DataHeader lStfMetaDataHdr;
  SubTimeFrameFileMeta lStfFileMeta;

  try {
    // Read DataHeader + SubTimeFrameFileMeta
    buffered_read(&lStfMetaDataHdr, sizeof(DataHeader));
    buffered_read(&lStfFileMeta, sizeof(SubTimeFrameFileMeta));

  } catch (const std::ios_base::failure& eFailExc) {
    DDLOG(fair::Severity::ERROR) << "Reading from file failed. Error: " << eFailExc.what();
    return nullptr;
  }

  // verify we're actually reading the correct data in
  if (!(SubTimeFrameFileMeta::getDataHeader().dataDescription == lStfMetaDataHdr.dataDescription)) {
   DDLOG(fair::Severity::WARNING) << "Reading bad data: SubTimeFrame META header";
    mFile.close();
    return nullptr;
  }

  // prepare to read the TF data
  const auto lStfSizeInFile = lStfFileMeta.mStfSizeInFile;
  if (lStfSizeInFile == (sizeof(DataHeader) + sizeof(SubTimeFrameFileMeta))) {
   DDLOG(fair::Severity::WARNING) << "Reading an empty TF from file. Only meta information present";
    return nullptr;
  }

  // check there's enough data in the file
  if ((lTfStartPosition + lStfSizeInFile) > this->size()) {
   DDLOG(fair::Severity::WARNING) << "Not enough data in file for this TF. Required: " << lStfSizeInFile
                 << ", available: " << (this->size() - lTfStartPosition);
    mFile.close();
    return nullptr;
  }

  // Index
  // TODO: skip the index for now, check in future all data is there
  DataHeader lStfIndexHdr;
  try {
    // Read DataHeader + SubTimeFrameFileMeta
    buffered_read(&lStfIndexHdr, sizeof(DataHeader));
    mFile.seekg(lStfIndexHdr.payloadSize, std::ios_base::cur);
  } catch (const std::ios_base::failure& eFailExc) {
    DDLOG(fair::Severity::ERROR) << "Reading from file failed. Error: " << eFailExc.what();
    return nullptr;
  }

  const auto lStfDataSize = lStfSizeInFile - (sizeof(DataHeader) + sizeof(SubTimeFrameFileMeta))
    - (sizeof (lStfIndexHdr) + lStfIndexHdr.payloadSize);

  // read all data blocks and headers
  assert(mStfData.empty());
  try {

    std::int64_t lLeftToRead = lStfDataSize;

    // read <hdrStack + data> pairs
    while (lLeftToRead > 0) {

      // read the header stack
      const std::int64_t lHdrSize = getHeaderStackSize();
      if (lHdrSize < std::int64_t(sizeof(DataHeader))) {
        // error while checking headers
       DDLOG(fair::Severity::WARNING) << "Reading bad data: Header stack cannot be parsed";
        mFile.close();
        return nullptr;
      }
      // allocate and read the Headers
      auto lHdrStackMsg = pDstChan.NewMessage(lHdrSize);
      if (!lHdrStackMsg) {
       DDLOG(fair::Severity::WARNING) << "Out of memory: header message, allocation size: " << lHdrSize;
        mFile.close();
        return nullptr;
      }

      buffered_read(lHdrStackMsg->GetData(), lHdrSize);

      // read the data
      DataHeader lDataHeader;
      std::memcpy(&lDataHeader, lHdrStackMsg->GetData(), sizeof(DataHeader));
      const std::uint64_t lDataSize = lDataHeader.payloadSize;

      auto lDataMsg = pDstChan.NewMessage(lDataSize);
      if (!lDataMsg) {
       DDLOG(fair::Severity::WARNING) << "Out of memory: data message, allocation size: " << lDataSize;
        mFile.close();
        return nullptr;
      }
      buffered_read(lDataMsg->GetData(), lDataSize);

      mStfData.emplace_back(
        SubTimeFrame::StfData{
          std::move(lHdrStackMsg),
          std::move(lDataMsg) });

      // update the counter
      lLeftToRead -= (lHdrSize + lDataSize);
    }

    if (lLeftToRead < 0) {
      DDLOG(fair::Severity::ERROR) << "FileRead: Read more data than it is indicated in the META header!";
      return nullptr;
    }

  } catch (const std::ios_base::failure& eFailExc) {
    DDLOG(fair::Severity::ERROR) << "Reading from file failed. Error: " << eFailExc.what();
    return nullptr;
  }

  // build the SubtimeFrame
  lStf->accept(*this);

  DDLOG(fair::Severity::DEBUG) << "FileReader: read TF size: " << lStfFileMeta.mStfSizeInFile
            << ", created on " << lStfFileMeta.getTimeString()
            << " (timestamp: " << lStfFileMeta.mWriteTimeMs << ")";

  return lStf;
}
}
} /* o2::DataDistribution */
