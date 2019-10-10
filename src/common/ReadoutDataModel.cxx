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

#include <fairmq/FairMQDevice.h>

#include <Headers/DataHeader.h>
#include <tuple>

namespace o2
{
namespace DataDistribution
{

ReadoutDataUtils::SanityCheckMode ReadoutDataUtils::sRdhSanityCheckMode = eNoSanityCheck;

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
    case 4: [[fallthrough]]
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
        LOG(ERROR) << "Unknown RDH version: " << lVer << ". Please report the issue.";
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

  lSubSpec = (lCruId << 16) | (lLinkId << (lEndPoint == 0 ? 0 : 8));

  return lSubSpec;
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
    LOG(ERROR) << "Data block is shorter than RDH: " << pLen;
    o2::header::hexDump("Short readout block", pData, pLen);
    return false;
  }

  // sub spec of first RDH
  const auto lSubSpec = getSubSpecification(pData, pLen);

  std::int64_t lDataLen = pLen;
  const char* lCurrData = pData;
  std::uint32_t lPacketCnt = 1;

  while(lDataLen > 0) {

    if (lDataLen > 0 && lDataLen < 64 /*RDH*/ ) {
      LOG(ERROR) << "BLOCK CHECK: Data is shorter than RDH. Block offset: " << lCurrData - pData;
      o2::header::hexDump("Data at the end of the block", lCurrData, lDataLen);
      return false;
    }

    // check if sub spec matches
    if (lSubSpec != getSubSpecification(lCurrData, lDataLen)) {
      LOG(ERROR) << "BLOCK CHECK: Data sub-specification of trailing RDHs does not match."
                    " Subspecification of the first RDH: 0x" << std::hex << lSubSpec
                 << " Subspecification of " << lPacketCnt << ". RDH: 0x" << std::hex << getSubSpecification(lCurrData, lDataLen);
      return false;
    }

    const auto [lMemSize, lOffsetNext, lStopBit] = getRdhNavigationVals(lCurrData);

    // check if last package
    if (lStopBit) {
      if (lMemSize <= lDataLen) {
        return true; // all memory is accounted for
      } else {
        LOG(ERROR) << "BLOCK CHECK: RDH has bit stop set, but memory size is different from remaining block size."
                      " memory size: " << lMemSize <<
                      " remaining buffer size: " << lDataLen;
        return false;
      }
    }

    if (lOffsetNext == 0) {
      LOG(ERROR) << "BLOCK CHECK: Next block offset is 0.";
      return false;
    }

    if (lOffsetNext >= lDataLen) {
      LOG(ERROR) << "BLOCK CHECK: Next offset points beyond end of data block (stop bit is not set).";
      return false;
    }

    if (lMemSize >= lDataLen) {
      LOG(ERROR) << "BLOCK CHECK: Memory size is larger than remaining data block size for packet " << lPacketCnt;
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
  if (pLen != 16384) {
    return false; // size is not right
  }

  if (pData[0] != 4) {
    return false; // not RDH4
  }

  if (getSubSpecification(pData, 8192) != getSubSpecification(pData+8192, 8192)) {
    return false;
  }

  const auto [lMemSize1, lOffsetNext1, lStopBit1] = getRdhNavigationVals(pData);
  const auto [lMemSize2, lOffsetNext2, lStopBit2] = getRdhNavigationVals(pData + 8192);

  if (lMemSize1 != lMemSize2 || lMemSize1 != 64) {
    return false;
  }

  if (lOffsetNext1 != lOffsetNext2 || lOffsetNext1 != 8192) {
    return false;
  }

  if (lStopBit2 == 0) {
    return false;
  }

  // looks like it should be empy trigger message
  return true;
}


}
} /* o2::DataDistribution */
