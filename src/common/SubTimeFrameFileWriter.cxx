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
#include "SubTimeFrameFileWriter.h"

#include "DataDistLogger.h"

#include <iomanip>

namespace o2
{
namespace DataDistribution
{

using namespace o2::header;

////////////////////////////////////////////////////////////////////////////////
/// SubTimeFrameFileWriter
////////////////////////////////////////////////////////////////////////////////

SubTimeFrameFileWriter::SubTimeFrameFileWriter(const boost::filesystem::path& pFileName, bool pWriteInfo)
  : mWriteInfo(pWriteInfo)
{
  using ios = std::ios_base;

  // allocate and set the larger stream buffer
  mFileBuf = std::make_unique<char[]>(sBuffSize);
  mFile.rdbuf()->pubsetbuf(mFileBuf.get(), sBuffSize);
  mFile.clear();
  mFile.exceptions(std::fstream::failbit | std::fstream::badbit);
  // allocate and set the larger stream buffer (info file)
  if (mWriteInfo) {
    mInfoFileBuf = std::make_unique<char[]>(sBuffSize);
    mInfoFile.rdbuf()->pubsetbuf(mInfoFileBuf.get(), sBuffSize);
    mInfoFile.clear();
    mInfoFile.exceptions(std::fstream::failbit | std::fstream::badbit);
  }

  try {
    mFile.open(pFileName.string(), ios::binary | ios::trunc | ios::out | ios::ate);

    if (mWriteInfo) {
      auto lInfoFileName = pFileName.string();
      lInfoFileName += ".info";

      mInfoFile.open(lInfoFileName, ios::trunc | ios::out);

      mInfoFile << "TF_ID ";
      mInfoFile << "TF_OFFSET ";
      mInfoFile << "TF_SIZE ";
      mInfoFile << "ORIGIN ";
      mInfoFile << "DESCRIPTION ";
      mInfoFile << "SUBSPECIFICATION ";
      mInfoFile << "DATA_INDEX ";
      mInfoFile << "HEADER_OFFSET ";
      mInfoFile << "HEADER_SIZE ";
      mInfoFile << "DATA_OFFSET ";
      mInfoFile << "DATA_SIZE ";
      mInfoFile << "RDH_MEM_SIZE ";
      mInfoFile << "RDH_STOP_BIT ";
      mInfoFile << "FEE_ID ";
      mInfoFile << "HB_ORBIT" << '\n';
    }
  } catch (std::ifstream::failure& eOpenErr) {
    DDLOG(fair::Severity::ERROR) << "Failed to open/create TF file for writing. Error: " << eOpenErr.what();
    throw eOpenErr;
  }
}

SubTimeFrameFileWriter::~SubTimeFrameFileWriter()
{
  try {
    mFile.close();
    if (mWriteInfo) {
      mInfoFile.close();
    }
  } catch (std::ifstream::failure& eCloseErr) {
    DDLOG(fair::Severity::ERROR) << "Closing TF file failed. Error: " << eCloseErr.what();
  } catch (...) {
    DDLOG(fair::Severity::ERROR) << "Closing TF file failed.";
  }
}

void SubTimeFrameFileWriter::visit(const SubTimeFrame& pStf)
{
  assert(mStfData.empty() && mStfSize == 0);
  assert(mStfDataIndex.empty());

  // Write data in lexicographical order of DataIdentifier + subSpecification
  // for easier binary comparison
  std::vector<EquipmentIdentifier> lEquipIds = pStf.getEquipmentIdentifiers();
  std::sort(std::begin(lEquipIds), std::end(lEquipIds));

  //  sizes for different equipment identifiers
  std::unordered_map<EquipmentIdentifier, std::tuple<std::uint64_t, std::uint32_t>> lDataIdSizeCnt;

  for (const auto& lEquip : lEquipIds) {

    const auto& lEquipDataVec = pStf.mData.at(lEquip).at(lEquip.mSubSpecification);

    for (const auto& lData : lEquipDataVec) {
      // NOTE: get only pointers to <hdr, data> struct
      mStfData.emplace_back(&lData);
      // account the size
      const auto lHdrDataSize = lData.mHeader->GetSize() + lData.mData->GetSize();

      // total size
      mStfSize += lHdrDataSize;

      // calculate the size for the index
      auto & [ lSize, lCnt ] = lDataIdSizeCnt[lEquip];
      lSize += lHdrDataSize;
      lCnt++;
    }
  }

  // build the index
  {
    std::uint64_t lCurrOff = 0;
    for (const auto& lId : lEquipIds) {
      const auto[lIdSize, IdCnt] = lDataIdSizeCnt[lId];
      assert(lIdSize > sizeof(DataHeader));
      mStfDataIndex.AddStfElement(lId, IdCnt, lCurrOff, lIdSize);
      lCurrOff += lIdSize;
    }
  }
}

std::uint64_t SubTimeFrameFileWriter::getSizeInFile() const
{
  return SubTimeFrameFileMeta::getSizeInFile() + mStfDataIndex.getSizeInFile() + mStfSize;
}

std::uint64_t SubTimeFrameFileWriter::write(const SubTimeFrame& pStf)
{
  if (!mFile.good()) {
   DDLOG(fair::Severity::WARNING) << "Error while writing a TF to file. (bad stream state)";
    return std::uint64_t(0);
  }

  const auto ret = this->_write(pStf);

  // cleanup:
  // make sure headers and chunk pointers don't linger
  mStfData.clear();
  mStfDataIndex.clear();
  mStfSize = 0;

  return ret;
}

std::uint64_t SubTimeFrameFileWriter::_write(const SubTimeFrame& pStf)
{
  // collect all stf blocks
  pStf.accept(*this);

  // get file position
  const std::uint64_t lPrevSize = size();
  const std::uint64_t lStfSizeInFile = getSizeInFile();
  std::uint64_t lDataOffset = 0;

  SubTimeFrameFileMeta lStfFileMeta(lStfSizeInFile);

  try {
    // Write DataHeader + SubTimeFrameFileMeta
    mFile << lStfFileMeta;

    // Write DataHeader + SubTimeFrameFileDataIndex
    mFile << mStfDataIndex;

    lDataOffset = size(); // save for the info file

    for (const auto& lStfData : mStfData) {
      // only write DataHeader (make a local DataHeader copy to clear flagsNextHeader bit)
      const DataHeader lDh = lStfData->getDataHeader();
      buffered_write(reinterpret_cast<const char*>(&lDh), sizeof (DataHeader));
      buffered_write(lStfData->mData->GetData(), lStfData->mData->GetSize());
    }

    // flush the buffer and check the state
    mFile.flush();

  } catch (const std::ios_base::failure& eFailExc) {
    DDLOG(fair::Severity::ERROR) << "Writing to file failed. Error: " << eFailExc.what();
    return std::uint64_t(0);
  }

  assert((size() - lPrevSize == lStfSizeInFile) && "Calculated and written sizes differ");

  // sidecar
  if (mWriteInfo) {
    try {
      const auto l1StfId = pStf.header().mId;
      const auto l2StfFileOff = lPrevSize;
      const auto l3StfFileSize = lStfSizeInFile;

      for (const auto& lStfData : mStfData) {
        DataHeader lDH;
        std::memcpy(&lDH, lStfData->mHeader->GetData(), sizeof(DataHeader));

        const auto& l4DataOrigin = lDH.dataOrigin;
        const auto& l5DataDescription = lDH.dataDescription;
        const auto l6SubSpec = lDH.subSpecification;
        const auto l7DataIndex = lDH.splitPayloadIndex;

        const auto l8HdrOff = lDataOffset;
        lDataOffset += lStfData->mHeader->GetSize();
        const auto l9HdrSize = lStfData->mHeader->GetSize();
        const auto l10DataOff = lDataOffset;
        lDataOffset += lStfData->mData->GetSize();
        const auto l11DataSize = lStfData->mData->GetSize();
        const auto [l12MemSize, l13StopBit] = ReadoutDataUtils::getRdhMemorySize(
          reinterpret_cast<const char*>(lStfData->mData->GetData()),
          lStfData->mData->GetSize());

        const auto l14FeeId = ReadoutDataUtils::getFeeId(
          reinterpret_cast<const char*>(lStfData->mData->GetData()),
          lStfData->mData->GetSize());

        const auto l15Orbit = ReadoutDataUtils::getHBOrbit(
          reinterpret_cast<const char*>(lStfData->mData->GetData()),
          lStfData->mData->GetSize());

        mInfoFile << l1StfId << sSidecarFieldSep;
        mInfoFile << l2StfFileOff << sSidecarFieldSep;
        mInfoFile << l3StfFileSize << sSidecarFieldSep;
        mInfoFile << l4DataOrigin.str << sSidecarFieldSep;
        mInfoFile << l5DataDescription.str << sSidecarFieldSep;


        std::ios_base::fmtflags lFlags( mInfoFile.flags() );
        mInfoFile << "0x" << std::setfill('0')
                  << std::setw(8)
                  << std::hex
                  << l6SubSpec << sSidecarFieldSep;
        mInfoFile.flags(lFlags);

        mInfoFile << l7DataIndex << sSidecarFieldSep;
        mInfoFile << l8HdrOff << sSidecarFieldSep;
        mInfoFile << l9HdrSize << sSidecarFieldSep;
        mInfoFile << l10DataOff << sSidecarFieldSep;
        mInfoFile << l11DataSize << sSidecarFieldSep;
        mInfoFile << l12MemSize << sSidecarFieldSep;
        mInfoFile << l13StopBit << sSidecarFieldSep;
        mInfoFile << l14FeeId << sSidecarFieldSep;
        mInfoFile << l15Orbit << sSidecarRecordSep;
      }
      mInfoFile.flush();
    } catch (const std::ios_base::failure& eFailExc) {
      DDLOG(fair::Severity::ERROR) << "Writing to file failed. Error: " << eFailExc.what();
      return std::uint64_t(0);
    }
  }

  return (size() - lPrevSize);
}
}
} /* o2::DataDistribution */
