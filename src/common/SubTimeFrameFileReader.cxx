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
#include "SubTimeFrameBuilder.h"

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

std::size_t SubTimeFrameFileReader::getHeaderStackSize() // throws ios_base::failure
{
  // Expect valid Stack in the file.
  // First Header must be DataHeader. The size is unknown since there are multiple versions.
  // Each header in the stack extends BaseHeader

  // Read first the base header then the rest of the extended header. Keep going until the next flag is set.
  // reset the file pointer to the original incoming position, so the complete Stack can be read in

  bool readNextHeader = true;
  std::size_t lStackSize = 0;
  DataHeader lBaseHdr; // Use DataHeader  since the BaseHeader has no default contructor.

  const auto lFilePosStart = position();

  const auto cMaxHeaders = 8; /* make sure we don't loop forever */
  auto lNumHeaders = 0;
  while (readNextHeader && (++lNumHeaders <= cMaxHeaders)) {
    buffered_read(&lBaseHdr, sizeof(BaseHeader)); // read BaseHeader only!

    mFile.ignore(lBaseHdr.size());

    lStackSize += lBaseHdr.size();
    readNextHeader = (lBaseHdr.next() != nullptr);
  }
  // reset the file pointer
  mFile.seekg(lFilePosStart);

  if (lNumHeaders >= cMaxHeaders) {
    DDLOGF(fair::Severity::ERROR, "FileRead: Reached max number of headers allowed: {}.", cMaxHeaders);
    return 0;
  }

  return lStackSize;
}

Stack SubTimeFrameFileReader::getHeaderStack(std::size_t *pOrigsize) // throws ios_base::failure
{
  const auto lStackSize = getHeaderStackSize();
  if (lStackSize < sizeof(BaseHeader)) {
    // error in the stream
    return Stack{};
  }

  if (pOrigsize) {
    *pOrigsize = lStackSize;
  }

  // std::unique_ptr<o2::byte[]> lStackMem = std::make_unique<o2::byte[]>(lStackSize);
  auto lStackMem = std::make_unique<o2::byte[]>(lStackSize);

  // This must handle different versions of DataHeader
  buffered_read(lStackMem.get(), lStackSize);

  // check if DataHeader needs an upgrade by looking at the version number
  const BaseHeader *lBaseOfDH = BaseHeader::get(lStackMem.get());
  if (!lBaseOfDH) {
    return Stack{};
  }

  if (lBaseOfDH->headerVersion < DataHeader::sVersion) {
    DataHeader lNewDh;

    // Write over the new DataHeader. We need to update some of the BaseHeader values.
    std::memcpy(&lNewDh, lBaseOfDH->data(), lBaseOfDH->size());
    // make sure to bump the version in the BaseHeader.
    // TODO: Is there a better way?
    lNewDh.headerSize = sizeof(DataHeader);
    lNewDh.headerVersion = DataHeader::sVersion;

    if (lBaseOfDH->headerVersion == 1) {
      mDHUpdateFirstOrbit = true;
    } else {
      DDLOGF(fair::Severity::ERROR, "DataHeader version {} read from file is not upgraded to the current version {}",
        lBaseOfDH->headerVersion, DataHeader::sVersion);
      DDLOGF(fair::Severity::ERROR, "Try newer version of DataDistribution or file a BUG");
    }

    assert (sizeof (DataHeader) > lBaseOfDH->size() ); // current DataHeader must be larger

    if (lBaseOfDH->size() == lStackSize) {
      return Stack(lNewDh);
    } else {
      assert(lBaseOfDH->size() < lStackSize);

      return Stack(
        lNewDh,
        Stack(lStackMem.get() + lBaseOfDH->size())
      );
    }
  }

  return Stack(lStackMem.get());
}

std::uint64_t SubTimeFrameFileReader::sStfId = 0; // TODO: add id to files metadata

std::unique_ptr<SubTimeFrame> SubTimeFrameFileReader::read(SubTimeFrameFileBuilder &pFileBuilder)
{
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

  std::unique_ptr<Stack> lMetaHdrStack;
  std::size_t lMetaHdrStackSize = 0;
  const DataHeader *lStfMetaDataHdr = nullptr;
  SubTimeFrameFileMeta lStfFileMeta;

  try {
    // Read DataHeader + SubTimeFrameFileMeta
    lMetaHdrStack = std::make_unique<Stack>(getHeaderStack(&lMetaHdrStackSize));
    lStfMetaDataHdr = o2::header::DataHeader::Get(lMetaHdrStack->first());
    if (!lStfMetaDataHdr) {
      DDLOGF(fair::Severity::ERROR, "Failed to read the TF file header. The file might be corrupted.");
      mFile.close();
      return nullptr;
    }

    buffered_read(&lStfFileMeta, sizeof(SubTimeFrameFileMeta));
  } catch (const std::ios_base::failure& eFailExc) {
    DDLOGF(fair::Severity::ERROR, "Reading from file failed. Error: {}", eFailExc.what());
    mFile.close();
    return nullptr;
  }

  // verify we're actually reading the correct data in
  if (!(SubTimeFrameFileMeta::getDataHeader().dataDescription == lStfMetaDataHdr->dataDescription)) {
    DDLOGF(fair::Severity::WARNING, "Reading bad data: SubTimeFrame META header");
    mFile.close();
    return nullptr;
  }

  // prepare to read the TF data
  const auto lStfSizeInFile = lStfFileMeta.mStfSizeInFile;
  if (lStfSizeInFile == (sizeof(DataHeader) + sizeof(SubTimeFrameFileMeta))) {
    DDLOGF(fair::Severity::WARNING, "Reading an empty TF from file. Only meta information present");
    mFile.close();
    return nullptr;
  }

  // check there's enough data in the file
  if ((lTfStartPosition + lStfSizeInFile) > this->size()) {
    DDLOGF(fair::Severity::WARNING, "Not enough data in file for this TF. Required: {}, available: {}",
      lStfSizeInFile, (this->size() - lTfStartPosition));
    mFile.close();
    return nullptr;
  }

  // Index
  // TODO: skip the index for now, check in future all data is there
  std::unique_ptr<Stack> lStfIndexHdrStack;
  std::size_t lStfIndexHdrStackSize = 0;
  const DataHeader *lStfIndexHdr = nullptr;
  try {
    // Read DataHeader + SubTimeFrameFileMeta
    lStfIndexHdrStack = std::make_unique<Stack>(getHeaderStack(&lStfIndexHdrStackSize));
    lStfIndexHdr = o2::header::DataHeader::Get(lStfIndexHdrStack->first());
    if (!lStfIndexHdr) {
      DDLOGF(fair::Severity::ERROR, "Failed to read the TF index structure. The file might be corrupted.");
      return nullptr;
    }

    mFile.ignore(lStfIndexHdr->payloadSize);
  } catch (const std::ios_base::failure& eFailExc) {
    DDLOG(fair::Severity::ERROR) << "Reading TF index from file failed. Error: " << eFailExc.what();
    return nullptr;
  }

  // Remaining data size of the TF:
  // total size in file - meta (hdr+struct) - index (hdr + payload)
  const auto lStfDataSize = lStfSizeInFile - (lMetaHdrStackSize + sizeof(SubTimeFrameFileMeta))
    - (lStfIndexHdrStackSize + lStfIndexHdr->payloadSize);

  // read all data blocks and headers
  assert(mStfData.empty());
  try {
    std::int64_t lLeftToRead = lStfDataSize;

    // read <hdrStack + data> pairs
    while (lLeftToRead > 0) {

      // allocate and read the Headers
      std::size_t lDataHeaderStackSize = 0;
      Stack lDataHeaderStack = getHeaderStack(&lDataHeaderStackSize);
      const DataHeader *lDataHeader = o2::header::DataHeader::Get(lDataHeaderStack.first());
      if (!lDataHeader) {
        DDLOGF(fair::Severity::ERROR, "Failed to read the TF HBF DataHeader structure. The file might be corrupted.");
        return nullptr;
      }

      auto lHdrStackMsg = pFileBuilder.getHeaderMessage(lDataHeaderStack, lStf->id());
      if (!lHdrStackMsg) {
        DDLOGF(fair::Severity::WARNING, "Out of memory: header message, allocation size: {}", lDataHeaderStackSize);
        mFile.close();
        return nullptr;
      }

      // read the data
      const std::uint64_t lDataSize = lDataHeader->payloadSize;

      auto lDataMsg = pFileBuilder.getDataMessage(lDataSize);
      if (!lDataMsg) {
        DDLOGF(fair::Severity::WARNING, "Out of memory: data message, allocation size: {}", lDataSize);
        mFile.close();
        return nullptr;
      }
      buffered_read(lDataMsg->GetData(), lDataSize);

      // Try to figure out the first orbit
      try {
        const auto lHdr = reinterpret_cast<DataHeader*>(lDataHeaderStack.data());

        if (lHdr && lHdr->firstTForbit == 0 && lHdr->dataDescription == o2::header::gDataDescriptionRawData) {
          const auto R = RDHReader(lDataMsg);
          lStf->updateFirstOrbit(R.getOrbit());
        }
      } catch (...) {
        DDLOGF(fair::Severity::ERROR, "Error getting RDHReader instace. Not setting firstOrbit for file data");
      }

      mStfData.emplace_back(
        SubTimeFrame::StfData{
          std::move(lHdrStackMsg),
          std::move(lDataMsg) }
      );

      // update the counter
      lLeftToRead -= (lDataHeaderStackSize + lDataSize);
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

  return lStf;
}
}
} /* o2::DataDistribution */
