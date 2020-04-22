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
#include <tuple>

namespace o2
{
namespace DataDistribution
{

ReadoutDataUtils::SubSpecMode ReadoutDataUtils::sRawDataSubspectype = eCruLinkId;
ReadoutDataUtils::SanityCheckMode ReadoutDataUtils::sRdhSanityCheckMode = eNoSanityCheck;
ReadoutDataUtils::RdhVersion ReadoutDataUtils::sRdhVersion = eRdhInvalid;
bool ReadoutDataUtils::sEmptyTriggerHBFrameFilterring = false;

std::unique_ptr<RDHReaderIf> RDHReader::sRDHReader = nullptr;

/// static
thread_local std::uint64_t ReadoutDataUtils::sFirstSeenHBOrbitCnt = 0;

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
    DDLOGF(fair::Severity::FATAL, "Invalid SubSpecification method={}", ReadoutDataUtils::sRawDataSubspectype);
  }

  return lSubSpec;
}

std::tuple<std::size_t, bool>
ReadoutDataUtils::getHBFrameMemorySize(const FairMQMessagePtr &pMsg)
{
  const char *data = reinterpret_cast<const char*>(pMsg->GetData());
  const std::size_t len = pMsg->GetSize();

  std::size_t lMemRet = 0;
  bool lStopRet = false;

  const char *p = data;

  while (p < data + len) {
    const auto R = RDHReader(p, data + len - p);
    const auto lMemSize = R.getMemorySize();
    const auto lOffsetNext = R.getOffsetToNext();
    const auto lStopBit = R.getStopBit();

    lMemRet += lMemSize;
    lStopRet = lStopBit;

    if (lStopBit) {
      break;
    }

    p += std::max(lOffsetNext, uint32_t(64));
  }

  if (p > data + len) {
    DDLOGF(fair::Severity::ERROR, "BLOCK CHECK: StopBit lookup failed: advanced beyond end of the buffer.");
    lStopRet = false;
  }

  return {lMemRet, lStopRet};
}

bool ReadoutDataUtils::rdhSanityCheck(const char* pData, const std::size_t pLen)
{
  const auto R = RDHReader(pData, pLen);

  if (pLen < 64) { // size of one RDH
    DDLOGF(fair::Severity::ERROR, "Data block is shorter than RDH: {}", pLen);
    o2::header::hexDump("Short readout block", pData, pLen);
    return false;
  }

  // set first hbframe orbit if not set for this stf
  {
    std::uint32_t lOrbit = 0;
    std::memcpy(&lOrbit, pData + (5 * sizeof (std::uint32_t)), sizeof (std::uint32_t));
    if (sFirstSeenHBOrbitCnt == 0) {
      sFirstSeenHBOrbitCnt = lOrbit;
    } else {
      if (lOrbit < sFirstSeenHBOrbitCnt) {
        DDLOGF(fair::Severity::ERROR, "Orbit counter of current data packet (HBF) is smaller than first orbit of the STF."
          " orbit={} first_orbit={} diff={}", lOrbit, sFirstSeenHBOrbitCnt, sFirstSeenHBOrbitCnt-lOrbit);
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
      DDLOGF(fair::Severity::ERROR, "BLOCK CHECK: Data is shorter than RDH. Block offset: {}", (lCurrData - pData));
      o2::header::hexDump("Data at the end of the block", lCurrData, lDataLen);
      return false;
    }

    // check if sub spec matches
    if (lSubSpec != getSubSpecification(Rc)) {
      DDLOGF(fair::Severity::ERROR, "BLOCK CHECK: Data sub-specification of trailing RDHs does not match."
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
        DDLOGF(fair::Severity::ERROR, "BLOCK CHECK: RDH has bit stop set, but memory size is different from remaining block size."
          " memory_size={} remaining_buffer_size={}", lMemSize, lDataLen);
        return false;
      }
    }

    if (lOffsetNext == 0) {
      DDLOGF(fair::Severity::ERROR, "BLOCK CHECK: Next block offset is 0.");
      return false;
    }

    if (lOffsetNext >= lDataLen) {
      DDLOGF(fair::Severity::ERROR, "BLOCK CHECK: Next offset points beyond end of data block (stop bit is not set).");
      return false;
    }

    if (lMemSize >= lDataLen) {
      DDLOGF(fair::Severity::ERROR, "BLOCK CHECK: Memory size is larger than remaining data block size for packet {}", lPacketCnt);
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
  static thread_local std::size_t sNumFiltered128Blocks = 0;
  static thread_local std::size_t sNumFiltered16kBlocks = 0;

  std::uint32_t lMemSize1, lOffsetNext1, lStopBit1;
  std::uint32_t lMemSize2, lStopBit2;

  if (pLen == 128 || pLen == 16384) { /* usual case */
    if (pData[0] != 4) {
      return false; // not RDH4
    }

    const auto R1 = RDHReader(pData, pLen);
    lMemSize1 = R1.getMemorySize();
    lOffsetNext1 = R1.getOffsetToNext();
    lStopBit1 = R1.getStopBit();

    if (lStopBit1) {
      return false;
    }

    if (lOffsetNext1 > pLen) {
      DDLOGF(fair::Severity::ERROR, "BLOCK CHECK: Invalid offset (beyond end of the buffer). offset={}", lOffsetNext1);
    } if (lOffsetNext1 < 64) {
      DDLOGF(fair::Severity::ERROR, "BLOCK CHECK: Invalid offset (less than the RDH size). offset={}", lOffsetNext1);
    }

    const char *lRDH1 = pData;
    const char *lRDH2 = lRDH1 + lOffsetNext1;
    const std::size_t lRDH2Size = std::min(std::size_t(pLen-lOffsetNext1), std::size_t(8192));

    const auto R2 = RDHReader(lRDH2, lRDH2Size);
    lMemSize2 = R2.getMemorySize();
    lStopBit2 = R2.getStopBit();

    if (getSubSpecification(R1) != getSubSpecification(R2)) {
      return false;
    }

    if (lMemSize1 != lMemSize2 || lMemSize1 != 64) {
      return false;
    }

    if ((lStopBit1 != 0) || (lStopBit2 != 1)) {
      return false;
    }

    if (pLen == 128) {
      sNumFiltered128Blocks++;
      if (sNumFiltered128Blocks % 250000 == 0) {
        DDLOGF(fair::Severity::INFO, "Filtered {} of 128 B blocks in trigger mode.", sNumFiltered128Blocks);
      }
    } else if (pLen == 16384) {
      sNumFiltered16kBlocks++;
      if (sNumFiltered16kBlocks % 250000 == 0) {
        DDLOGF(fair::Severity::INFO, "Filtered {} of 16 kiB blocks in trigger mode.", sNumFiltered16kBlocks);
      }
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

std::string to_string (ReadoutDataUtils::SubSpecMode pSubSpec)
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

}
} /* o2::DataDistribution */
