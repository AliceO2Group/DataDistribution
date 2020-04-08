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

ReadoutDataUtils::SanityCheckMode ReadoutDataUtils::sRdhSanityCheckMode = eNoSanityCheck;

/// static
thread_local std::uint64_t ReadoutDataUtils::sFirstSeenHBOrbitCnt = 0;

std::tuple<std::uint32_t,std::uint32_t,std::uint32_t>
ReadoutDataUtils::getSubSpecificationComponents(const char* pRdhData, const std::size_t len)
{
  std::uint32_t lCruId = 0, lEndPoint = 0, lLinkId = 0;
  if (len < 64) { // size of one RDH
    return std::tuple{~lCruId, ~lEndPoint, ~lLinkId};
  }

  // get the RDH version
  const auto lVer = pRdhData[0];
  switch (lVer) {
    case 3:
    {
      // no CRUIDin v3! -> get feeId
      std::memcpy(&lCruId, pRdhData + (1 * sizeof(std::uint32_t)), sizeof(std::uint32_t));
      lCruId &= 0x0000FFFF;

      // no endpoint in V3! -> 0
      lEndPoint = 0;

      std::memcpy(&lLinkId, pRdhData + (3 * sizeof(std::uint32_t)), sizeof(std::uint32_t));
      lLinkId &= 0x000000FF;
      break;
    }
    case 4:
    case 5:
    {
      std::memcpy(&lCruId, pRdhData + (3 * sizeof(std::uint32_t)), sizeof(std::uint32_t));
      lCruId >>= 16;
      lCruId &= 0x00000FFF; /* 12 bit */

      std::memcpy(&lEndPoint, pRdhData + (3 * sizeof(std::uint32_t)), sizeof(std::uint32_t));
      lEndPoint >>= 28;
      lEndPoint &= 0x0000000F; /* 4 bit */

      std::memcpy(&lLinkId, pRdhData + (3 * sizeof(std::uint32_t)), sizeof(std::uint32_t));
      lLinkId &= 0x000000FF; /* 8 bit */
      break;
    }
    default:
    {
      static auto lErrorRate = 0;
      if (lErrorRate++ % 2048 == 0) {
        DDLOGF(fair::Severity::ERROR, "Unknown RDH version: {}. Please report the issue.", lVer);
      }
      return std::tuple{~lCruId, ~lEndPoint, ~lLinkId};
      break;
    }
  }

  return { lCruId, lEndPoint, lLinkId };
}

o2::header::DataHeader::SubSpecificationType
ReadoutDataUtils::getSubSpecification(const char* pRdhData, const std::size_t len)
{
  static_assert( sizeof(o2::header::DataHeader::SubSpecificationType) == 4);

  o2::header::DataHeader::SubSpecificationType lSubSpec = 0;
  if (len < 64) { // size of one RDH
    return ~lSubSpec;
  }

  const auto [lCruId, lEndPoint, lLinkId] = getSubSpecificationComponents(pRdhData, len);

  /* add 1 to linkID because they start with 0 */
  lSubSpec = (lCruId << 16) | ((lLinkId + 1) << (lEndPoint == 0 ? 0 : 8));

  return lSubSpec;
}

std::tuple<std::size_t, int>
ReadoutDataUtils::getRdhMemorySize(const char* data, const std::size_t len)
{
  std::size_t lMemRet = 0;
  int lStopRet = 0;

  if (data[0] != 4) {
    return {-1, -1};
  }

  const char *p = data;

  while (p < data + len) {
    const auto [lMemSize, lOffsetNext, lStopBit] = getRdhNavigationVals(p);
    lMemRet += lMemSize;
    lStopRet = lStopBit;

    if( lStopBit ) {
      break;
    }

    p += std::max(lOffsetNext, uint32_t(64));
  }

  if (p > data + len) {
    DDLOGF(fair::Severity::ERROR, "BLOCK CHECK: StopBit lookup failed: advanced beyond end of the buffer.");
    lStopRet = -1;
  }

  return {lMemRet, lStopRet};
}

std::uint16_t ReadoutDataUtils::getFeeId(const char* data, const std::size_t len)
{
  std::uint16_t lFeeId = 0;

  if (len < 64 || data[0] != 4) {
    return std::uint16_t(-1);
  }

  std::memcpy(&lFeeId, data + (1 * sizeof(std::uint32_t)), sizeof(std::uint16_t));

  return lFeeId;
}

std::tuple<uint32_t,uint32_t,uint32_t> // orbit, bc, trig
ReadoutDataUtils::getOrbitBcTrg(const char* data, const std::size_t len)
{
  std::uint32_t lOrbit = 0;
  std::uint32_t lBc = 0;
  std::uint32_t lTrg = 0;

  if (len < 64 || data[0] != 4) {
    return {std::uint32_t(-1), std::uint32_t(-1), std::uint32_t(-1)};
  }

  std::memcpy(&lOrbit, data + (5 * sizeof(std::uint32_t)), sizeof(std::uint32_t));
  std::memcpy(&lBc, data + (8 * sizeof(std::uint32_t)), sizeof(std::uint32_t));
  lBc &= 0x00000FFF; // 12-bit

  std::memcpy(&lTrg, data + (9 * sizeof(std::uint32_t)), sizeof(std::uint32_t));

  return { lOrbit, lBc, lTrg};
}


std::tuple<uint32_t,uint32_t,uint32_t>
ReadoutDataUtils::getRdhNavigationVals(const char* pRdhData)
{
  std::uint32_t lMemSize, lOffsetNext, lStopBit;

  const auto lVer = pRdhData[0];
  switch (lVer) {
    case 4:
    {
      std::memcpy(&lOffsetNext, pRdhData + (2 * sizeof(std::uint32_t)), sizeof(std::uint32_t));
      lOffsetNext &= 0x0000FFFF;

      std::memcpy(&lMemSize, pRdhData + (2 * sizeof(std::uint32_t)), sizeof(std::uint32_t));
      lMemSize >>= 16;
      lMemSize &= 0x0000FFFF;

      std::memcpy(&lStopBit, pRdhData + (13 * sizeof(std::uint32_t)), sizeof(std::uint32_t));
      lStopBit &= 0x000000FF;
      break;
    }
    default:
    {
      lMemSize = lOffsetNext = lStopBit = ~std::uint32_t(0);
      break;
    }
  }

  return {lMemSize, lOffsetNext, lStopBit};
}

bool ReadoutDataUtils::rdhSanityCheck(const char* pData, const std::size_t pLen)
{
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
  const auto lSubSpec = getSubSpecification(pData, pLen);

  std::int64_t lDataLen = pLen;
  const char* lCurrData = pData;
  std::uint32_t lPacketCnt = 1;

  while(lDataLen > 0) {

    if (lDataLen > 0 && lDataLen < 64/*RDH*/ ) {
      DDLOGF(fair::Severity::ERROR, "BLOCK CHECK: Data is shorter than RDH. Block offset: {}", (lCurrData - pData));
      o2::header::hexDump("Data at the end of the block", lCurrData, lDataLen);
      return false;
    }

    // check if sub spec matches
    if (lSubSpec != getSubSpecification(lCurrData, lDataLen)) {
      DDLOGF(fair::Severity::ERROR, "BLOCK CHECK: Data sub-specification of trailing RDHs does not match."
        " RDH[0]::SubSpec: {:#06X}, RDH[{}]::SubSpec: {:#06X}",
        lSubSpec, lPacketCnt, getSubSpecification(lCurrData, lDataLen));
      return false;
    }

    const auto [lMemSize, lOffsetNext, lStopBit] = getRdhNavigationVals(lCurrData);

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

bool ReadoutDataUtils::filterTriggerEmpyBlocksV4(const char* pData, const std::size_t pLen)
{
  std::uint32_t lMemSize1, lOffsetNext1, lStopBit1;
  std::uint32_t lMemSize2, lOffsetNext2, lStopBit2;

  if (pLen == 128 || pLen == 16384) { /* usual case */
    static thread_local std::size_t sNumFiltered128Blocks = 0;
    static thread_local std::size_t sNumFiltered16kBlocks = 0;

    if (pData[0] != 4) {
      return false; // not RDH4
    }

    std::tie(lMemSize1, lOffsetNext1, lStopBit1) = getRdhNavigationVals(pData);

    if (lStopBit1) {
      return false;
    }

    if (lOffsetNext1 > pLen) {
      DDLOGF(fair::Severity::ERROR, "BLOCK CHECK: Invalid offset (beyond end of the buffer). offset={}", lOffsetNext1);
    } if (lOffsetNext1 < 64) {
      DDLOGF(fair::Severity::ERROR, "BLOCK CHECK: Invalid offset (less than the RDH size). offset={}", lOffsetNext1);
    }

    const char *lRDH1 = pData;
    const std::size_t lRDH1Size = std::min(std::size_t(lOffsetNext1), std::size_t(8192));

    const char *lRDH2 = lRDH1 + lOffsetNext1;

    std::tie(lMemSize2, lOffsetNext2, lStopBit2) = getRdhNavigationVals(lRDH2);
    const std::size_t lRDH2Size = std::min(std::size_t(lMemSize2), std::size_t(8192));

    if (getSubSpecification(lRDH1, lRDH1Size) != getSubSpecification(lRDH2, lRDH2Size)) {
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


}
} /* o2::DataDistribution */
