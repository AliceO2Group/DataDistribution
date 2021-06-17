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

#include "DataModelUtils.h"
#include "ReadoutDataModel.h"
#include "DataDistLogger.h"

#include <fairmq/FairMQDevice.h>

#include <Headers/DataHeader.h>
#include <Headers/DAQID.h>

#include <tuple>

namespace o2::DataDistribution
{

o2::header::DataOrigin ReadoutDataUtils::sSpecifiedDataOrigin = o2::header::gDataOriginInvalid; // to be initialized if not RDH6
ReadoutDataUtils::SubSpecMode ReadoutDataUtils::sRawDataSubspectype = eCruLinkId;
ReadoutDataUtils::SanityCheckMode ReadoutDataUtils::sRdhSanityCheckMode = eNoSanityCheck;
ReadoutDataUtils::RdhVersion ReadoutDataUtils::sRdhVersion = eRdhInvalid;
ReadoutDataUtils::RunType ReadoutDataUtils::sRunType = RunType::eInvalid;

bool ReadoutDataUtils::sEmptyTriggerHBFrameFilterring = false;

std::unique_ptr<RDHReaderIf> RDHReader::sRDHReader = nullptr;

/// static
std::uint32_t ReadoutDataUtils::sFirstSeenHBOrbitCnt = 0;

o2::header::DataOrigin
ReadoutDataUtils::getDataOrigin(const RDHReader &R)
{
  if (sRdhVersion == eRdhVer6) {
    const auto lOrig =  o2::header::DAQID::DAQtoO2(R.getSystemID());
    if (lOrig != o2::header::DAQID::DAQtoO2(o2::header::DAQID::INVALID)) {
      return lOrig;
    } else {
        EDDLOG_RL(1000, "Data origin in RDH is invalid: {}. Please configure the correct SYSTEM_ID in the hardware."
          " Using the configuration value {}.", R.getSystemID(), std::string(sSpecifiedDataOrigin.str));
    }
  }

  return sSpecifiedDataOrigin;
}

o2::header::DataHeader::SubSpecificationType
ReadoutDataUtils::getSubSpecification(const RDHReader &R)
{
  static_assert( sizeof(o2::header::DataHeader::SubSpecificationType) == 4);
  o2::header::DataHeader::SubSpecificationType lSubSpec = ~0;

  if (ReadoutDataUtils::sRawDataSubspectype == eCruLinkId) {
    /* add 1 to linkID because they start with 0 */
    lSubSpec = (R.getCruID() << 16) | ((R.getLinkID() + 1) << (R.getEndPointID() == 0 ? 0 : 8));
  } else if (ReadoutDataUtils::sRawDataSubspectype == eFeeId) {
    lSubSpec = R.getFeeID();
  } else {
    EDDLOG("Invalid SubSpecification method={}", ReadoutDataUtils::sRawDataSubspectype);
  }

  return lSubSpec;
}

std::tuple<std::size_t, bool>
ReadoutDataUtils::getHBFrameMemorySize(const FairMQMessagePtr &pMsg)
{
  std::size_t lMemRet = 0;
  bool lStopRet = false;

  try {
    auto R = RDHReader(pMsg);
    while (R != R.end()) {
      lMemRet += R.getMemorySize();
      lStopRet = R.getStopBit();

      R = R.next();
    }
  } catch (RDHReaderException &e) {
    EDDLOG( e.what());
  }

  if (lMemRet > pMsg->GetSize()) {
    EDDLOG("BLOCK CHECK: StopBit lookup failed: advanced beyond end of the buffer.");
    lStopRet = false;
  }

  return {lMemRet, lStopRet};
}

bool ReadoutDataUtils::rdhSanityCheck(const char* pData, const std::size_t pLen)
{
  const auto R = RDHReader(pData, pLen);

  if (pLen < R.getRDHSize()) { // size of one RDH
    EDDLOG("Data block is shorter than RDH: {}", pLen);
    o2::header::hexDump("Short readout block", pData, pLen);
    return false;
  }

  // set first hbframe orbit if not set for this stf
  {
    std::uint32_t lOrbit = R.getOrbit();
    if (sFirstSeenHBOrbitCnt == 0) {
      sFirstSeenHBOrbitCnt = lOrbit;
    } else {
      if (lOrbit < sFirstSeenHBOrbitCnt) {
        EDDLOG("Orbit counter of the current data packet (HBF) is smaller than first orbit of the STF."
          " orbit={} first_orbit={} diff={}", lOrbit, sFirstSeenHBOrbitCnt, (sFirstSeenHBOrbitCnt - lOrbit));
        return false;
      }
    }
  }

  // sub spec of first RDH
  const auto lSubSpec = getSubSpecification(R);

  std::int64_t lDataLen = pLen;
  const char* lCurrData = pData;
  std::uint32_t lPacketCnt = 1;

  while(lDataLen > 0) {
    const auto Rc = RDHReader(lCurrData, lDataLen);

    if (lDataLen > 0 && lDataLen < 64/*RDH*/ ) {
      EDDLOG("BLOCK CHECK: Data is shorter than RDH. Block offset: {}", (lCurrData - pData));
      o2::header::hexDump("Data at the end of the block", lCurrData, lDataLen);
      return false;
    }

    // check if sub spec matches
    if (lSubSpec != getSubSpecification(Rc)) {
      EDDLOG("BLOCK CHECK: Data sub-specification of trailing RDHs does not match."
        " RDH[0]::SubSpec: {:#06x}, RDH[{}]::SubSpec: {:#06x}",
        lSubSpec, lPacketCnt, getSubSpecification(Rc));
      return false;
    }

    const auto lMemSize = Rc.getMemorySize();
    const auto lOffsetNext = Rc.getOffsetToNext();
    const auto lStopBit = Rc.getStopBit();

    // check if last package
    if (lStopBit) {
      if (lMemSize <= lDataLen) {
        return true; // all memory is accounted for
      } else {
        EDDLOG("BLOCK CHECK: RDH has bit stop set, but memory size is different from remaining block size."
          " memory_size={} remaining_buffer_size={}", lMemSize, lDataLen);
        return false;
      }
    }

    if (lOffsetNext == 0) {
      EDDLOG("BLOCK CHECK: Next block offset is 0.");
      return false;
    }

    if (lOffsetNext >= lDataLen) {
      EDDLOG("BLOCK CHECK: Next offset points beyond end of data block (stop bit is not set).");
      return false;
    }

    if (lMemSize >= lDataLen) {
      EDDLOG("BLOCK CHECK: Memory size is larger than remaining data block size for packet {}", lPacketCnt);
      return false;
    }

    lDataLen -= lOffsetNext;
    lCurrData += lOffsetNext;
    lPacketCnt += 1;
  }

  return true;
}

bool ReadoutDataUtils::filterEmptyTriggerBlocks(const char* pData, const std::size_t pLen)
{
  static std::size_t sNumFiltered64Blocks = 0;
  static std::size_t sNumFiltered128Blocks = 0;
  static std::size_t sNumFiltered16kBlocks = 0;

  std::uint32_t lMemSize1, lOffsetNext1, lStopBit1;
  std::uint32_t lMemSize2, lStopBit2;

  if (pLen == 64 || pLen == 128 || pLen == 16384) { /* usual case */
    try{
      const auto R1 = RDHReader(pData, pLen);
      lStopBit1 = R1.getStopBit();
      lMemSize1 = R1.getMemorySize();
      // check the 64B case
      if (lStopBit1 && pLen == R1.getRDHSize() && lMemSize1 == R1.getRDHSize()) {
        sNumFiltered64Blocks++;
        if (sNumFiltered128Blocks % 250000 == 0) {
          IDDLOG("Filtered {} of 64 B blocks in trigger mode.", sNumFiltered128Blocks);
        }
        return true;
      } else if (lStopBit1) {
        return false;
      }

      lOffsetNext1 = R1.getOffsetToNext();

      if (lOffsetNext1 > pLen) {
        EDDLOG("BLOCK CHECK: Invalid offset (beyond end of the buffer). offset={}", lOffsetNext1);
      } if (lOffsetNext1 < R1.getRDHSize() ) {
        EDDLOG("BLOCK CHECK: Invalid offset (less than the RDH size). offset={}", lOffsetNext1);
      }

      const char *lRDH1 = pData;
      const char *lRDH2 = lRDH1 + lOffsetNext1;
      const std::size_t lRDH2Size = std::min(std::size_t(pLen - lOffsetNext1), std::size_t(8192));

      const auto R2 = RDHReader(lRDH2, lRDH2Size);

      // check the subspecification
      if (getSubSpecification(R1) != getSubSpecification(R2)) {
        return false;
      }

      lMemSize2 = R2.getMemorySize();
      lStopBit2 = R2.getStopBit();

      if (lMemSize1 != lMemSize2 || lMemSize1 != R1.getRDHSize()) {
        return false;
      }

      if ((lStopBit1 != 0) || (lStopBit2 != 1)) {
        return false ;
      }

      if (pLen == 128) {
        sNumFiltered128Blocks++;
        if (sNumFiltered128Blocks % 250000 == 0) {
          IDDLOG("Filtered {} of 128 B blocks in trigger mode.", sNumFiltered128Blocks);
        }
      } else if (pLen == 16384) {
        sNumFiltered16kBlocks++;
        if (sNumFiltered16kBlocks % 250000 == 0) {
          IDDLOG("Filtered {} of 16 kiB blocks in trigger mode.", sNumFiltered16kBlocks);
        }
      }
    } catch (RDHReaderException &e) {
      EDDLOG(e.what());
      return false;
    }
  } else {
    return false; // size does not match
  }

  // looks like it should be empty trigger message
  return true;
}

std::istream& operator>>(std::istream& in, ReadoutDataUtils::SanityCheckMode& pRetVal)
{
  std::string token;
  in >> token;

  if (token == "off") {
    pRetVal = ReadoutDataUtils::eNoSanityCheck;
  } else if (token == "drop") {
    pRetVal = ReadoutDataUtils::eSanityCheckDrop;
  } else if (token == "print") {
    pRetVal = ReadoutDataUtils::eSanityCheckPrint;
  } else {
    in.setstate(std::ios_base::failbit);
  }
  return in;
}

std::istream& operator>>(std::istream& in, ReadoutDataUtils::SubSpecMode& pRetVal)
{
  std::string token;
  in >> token;

  if (token == "cru_linkid") {
    pRetVal = ReadoutDataUtils::eCruLinkId;
  } else if (token == "feeid") {
    pRetVal = ReadoutDataUtils::eFeeId;
  } else {
    in.setstate(std::ios_base::failbit);
  }
  return in;
}

std::istream& operator>>(std::istream& in, ReadoutDataUtils::RdhVersion& pRetVal)
{
  std::string token;
  in >> token;

  if (token == "3") {
    pRetVal = ReadoutDataUtils::eRdhVer3;
  } else if (token == "4") {
    pRetVal = ReadoutDataUtils::eRdhVer4;
  } else if (token == "5") {
    pRetVal = ReadoutDataUtils::eRdhVer5;
  } else if (token == "6") {
    pRetVal = ReadoutDataUtils::eRdhVer6;
  } else {
    in.setstate(std::ios_base::failbit);
    pRetVal = ReadoutDataUtils::eRdhInvalid;
  }
  return in;
}

std::istream& operator>>(std::istream& in, ReadoutDataUtils::RunType& pRetVal)
{
  std::string token;
  in >> token;

  if (token == "physics") {
    pRetVal = ReadoutDataUtils::RunType::ePhysics;
  } else if (token == "scan") {
    pRetVal = ReadoutDataUtils::RunType::eThresholdScan;
  } else {
    in.setstate(std::ios_base::failbit);
    pRetVal = ReadoutDataUtils::RunType::eInvalid;
  }
  return in;
}

std::ostream& operator<<(std::ostream& out, const ReadoutDataUtils::RdhVersion& pRetVal)
{
  std::string token;
  out << (int) pRetVal;
  return out;
}

std::string to_string(const ReadoutDataUtils::SubSpecMode pSubSpec)
{
  switch (pSubSpec)
  {
    case ReadoutDataUtils::eCruLinkId:
      return "cru_linkid";
    case ReadoutDataUtils::eFeeId:
      return "feeid";
    default:
      return "invalid";
  }
}

std::string to_string(const ReadoutDataUtils::RunType pRunType)
{
  switch (pRunType)
  {
    case ReadoutDataUtils::RunType::ePhysics:
      return "physics";
      break;
    case ReadoutDataUtils::RunType::eThresholdScan:
      return "scan";
      break;
    default:
      return "invalid";
      break;
  }
}


} /* o2::DataDistribution */
