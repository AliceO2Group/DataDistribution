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
namespace impl {
  struct SidecarInfoData {
    const char *mHdrFmt;  // not including sep
    const char *mHdr;
    const char *mValFmt;
  };

  enum SidecarInfoDataType {
    TF_ID = 0,
    TF_OFFSET,
    TF_SIZE,
    ORIGIN,
    DESC,
    SUBSPEC,
    DATA_IDX,
    HDR_OFF,
    HDR_SIZE,
    DATA_OFF,
    DATA_SIZE,
    RDH_MEM_SIZE,
    RDH_STOP_BIT,
    RDH_FEE_ID,
    RDH_ORBIT,
    RDH_BC,
    RDH_TRG,
  };

  static const SidecarInfoData sInfoData[] = {
    { "{:<10}", "TF_ID",        "{:<10d}"   },
    { "{:<10}", "TF_OFFSET",    "{:<10d}"   },
    { "{:<9}",  "TF_SIZE",      "{:<9d}"    },
    { "{:<6}",  "ORIGIN",       "{:<6}"     },
    { "{:<9}",  "DESC",         "{:<9}"     },
    { "{:<10}", "SUBSPEC",      "{:<#010x}" },
    { "{:<8}",  "DATA_IDX",     "{:<8}"     },
    { "{:<10}", "HDR_OFF",      "{:<10}"    },
    { "{:<8}",  "HDR_SIZE",     "{:<8}"     },
    { "{:<10}", "DATA_OFF",     "{:<10}"    },
    { "{:<9}",  "DATA_SIZE",    "{:<9}"     },
    { "{:<12}", "RDH_MEM_SIZE", "{:<12}"    },
    { "{:<12}", "RDH_STOP_BIT", "{:<12}"    },
    { "{:<10}", "RDH_FEE_ID",   "{:<10}"    },
    { "{:<12}", "RDH_ORBIT",    "{:<#12d}"  },
    { "{:<10}", "RDH_BC",       "{:<#10d}"  },
    { "{:<10}", "RDH_TRG",      "{:<#010x}" },
  };

  static std::string sInfoToHdrString() {
    fmt::memory_buffer lHeader;
    const auto lHdrCnt = sizeof(sInfoData) / sizeof(SidecarInfoData);

    for (std::size_t i = 0; i < lHdrCnt; i++) {
      fmt::format_to(std::back_inserter(lHeader), sInfoData[i].mHdrFmt, sInfoData[i].mHdr);
      fmt::format_to(std::back_inserter(lHeader), "{}", (i < lHdrCnt - 1) ? " " : "");
    }

    return std::string(std::string(lHeader.begin(), lHeader.end()));
  }

  template<class T>
  static void sInfoVal(fmt::memory_buffer &pBuf, const SidecarInfoDataType pType, const T& pVal) {
    const auto lHdrCnt = sizeof(sInfoData) / sizeof(SidecarInfoData);
    fmt::format_to(std::back_inserter(pBuf), sInfoData[pType].mValFmt, pVal);
    fmt::format_to(std::back_inserter(pBuf), "{}", (pType < lHdrCnt - 1) ? " " : "");
  }
}

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
      mInfoFile << impl::sInfoToHdrString() << '\n';
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
      DataHeader lDh = lStfData->getDataHeader();
      lDh.flagsNextHeader = 0;
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
        fmt::memory_buffer lValRow;

        const DataHeader &lDH = lStfData->getDataHeader();

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

        impl::sInfoVal(lValRow, impl::TF_ID, l1StfId);
        impl::sInfoVal(lValRow, impl::TF_OFFSET, l2StfFileOff);
        impl::sInfoVal(lValRow, impl::TF_SIZE, l3StfFileSize);
        impl::sInfoVal(lValRow, impl::ORIGIN, l4DataOrigin.str);
        impl::sInfoVal(lValRow, impl::DESC, l5DataDescription.str);
        impl::sInfoVal(lValRow, impl::SUBSPEC, l6SubSpec);
        impl::sInfoVal(lValRow, impl::DATA_IDX, l7DataIndex);
        impl::sInfoVal(lValRow, impl::HDR_OFF, l8HdrOff);
        impl::sInfoVal(lValRow, impl::HDR_SIZE, l9HdrSize);
        impl::sInfoVal(lValRow, impl::DATA_OFF, l10DataOff);
        impl::sInfoVal(lValRow, impl::DATA_SIZE, l11DataSize);

        // only if the O2 header is RAWDATA
        if (lDH.dataDescription == gDataDescriptionRawData) {
          try {
            const auto R = RDHReader(lStfData->mData);
            const auto [l12MemSize, l13StopBit] = ReadoutDataUtils::getHBFrameMemorySize(lStfData->mData);
            const auto l14FeeId = R.getFeeID();
            const auto l15Orbit = R.getOrbit();
            const auto l16Bc = R.getBC();
            const auto l17Trig = R.getTriggerType();

            impl::sInfoVal(lValRow, impl::RDH_MEM_SIZE, l12MemSize);
            impl::sInfoVal(lValRow, impl::RDH_STOP_BIT, l13StopBit ? 1 : 0);
            impl::sInfoVal(lValRow, impl::RDH_FEE_ID, l14FeeId);
            impl::sInfoVal(lValRow, impl::RDH_ORBIT, l15Orbit);
            impl::sInfoVal(lValRow, impl::RDH_BC, l16Bc);
            impl::sInfoVal(lValRow, impl::RDH_TRG, l17Trig);
          } catch (RDHReaderException &e) {
            DDLOGF(fair::Severity::ERROR, e.what());
          }
        }

        mInfoFile << std::string_view(lValRow.begin(), lValRow.size()) << '\n';
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
