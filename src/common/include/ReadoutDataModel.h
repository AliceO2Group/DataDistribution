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

#ifndef ALICEO2_READOUT_DATAMODEL_H_
#define ALICEO2_READOUT_DATAMODEL_H_

#include <DataDistLogger.h>

#include <Headers/DataHeader.h>
#include <Headers/RAWDataHeader.h>
#include <Headers/DAQID.h>

#include <fairmq/FairMQMessage.h>

#include <istream>
#include <cstdint>
#include <tuple>
#include <variant>

namespace o2
{
namespace DataDistribution
{
////////////////////////////////////////////////////////////////////////////////
/// RDH reader interface
////////////////////////////////////////////////////////////////////////////////

class RDHReaderException : public std::runtime_error {
  const char *mData;
  const std::size_t mSize;

public:
  RDHReaderException() = delete;
  RDHReaderException(const char* data, const std::size_t size, const std::string &pMsg)
  : std::runtime_error("READOUT INTERFACE: Error when accessing RDH: " + pMsg),
    mData(data),
    mSize(size) {}
};

struct RDHReaderIf {
public:
  virtual ~RDHReaderIf() = default;

  // We walidate the data and the size on creation of the reader. No need to pass the size again
  virtual std::size_t CheckRdhData(const char* data, const std::size_t size) const = 0;

  // equipment
  virtual std::uint8_t getSystemID(const char* data) const = 0;
  virtual std::uint64_t getFeeID(const char* data) const = 0;
  virtual std::uint16_t getLinkID(const char* data)  const = 0;
  virtual std::uint8_t getEndPointID(const char* data)  const = 0;
  virtual std::uint16_t getCruID(const char* data)  const = 0;
  // memory layout
  virtual std::uint32_t getMemorySize(const char* data)  const = 0;
  virtual std::uint32_t getOffsetToNext(const char* data)  const = 0;
  virtual bool getStopBit(const char* data)  const = 0;
  // trigger
  virtual std::uint32_t getOrbit(const char* data) const = 0;
  virtual std::uint16_t getBC(const char* data) const = 0;
  virtual std::uint32_t getTriggerType(const char* data) const = 0;
};

template<typename RDH>
class RDHReaderImpl final : public RDHReaderIf {

  inline static
  const RDH& getHdrRef(const char* data) {
    return *reinterpret_cast<const RDH*>(data);
  }
public:
  RDHReaderImpl() = default;
  virtual ~RDHReaderImpl() {};

  virtual inline
  std::size_t CheckRdhData(const char* data, const std::size_t size) const override final {
    if (!data) {
      throw RDHReaderException(data, size, "Received NULL pointer instead of an RDH.");
    }

    if (sizeof(RDH) > size) {
      throw RDHReaderException(data, size, "RDH size is too small. size=" + std::to_string(size));
    }
    return sizeof(RDH);
  }

  // RDH field for SubSpecification
  virtual inline
  std::uint8_t getSystemID(const char* data) const override final {
    if constexpr (std::is_same_v<RDH, o2::header::RAWDataHeaderV6>) {
      const RDH &lRdh = getHdrRef(data);
      return lRdh.sourceID; /* systemID */
    }
    (void) data;
    return o2::header::DAQID::INVALID;
  }

  virtual inline
  std::uint64_t getFeeID(const char* data) const override final {
    const RDH &lRdh = getHdrRef(data);
    return lRdh.feeId;
  }

  virtual inline
  std::uint16_t getLinkID(const char* data) const override final {
    const RDH &lRdh = getHdrRef(data);
    return lRdh.linkID;
  }

  virtual inline
  std::uint8_t getEndPointID(const char* data) const override final {
    const RDH &lRdh = getHdrRef(data);
    return lRdh.endPointID;
  }

  virtual inline
  std::uint16_t getCruID(const char* data) const override final {
    const RDH &lRdh = getHdrRef(data);
    return lRdh.cruID;
  }

  // RDH memory layout
  virtual inline
  std::uint32_t getMemorySize(const char* data) const override final {
    const RDH &lRdh = getHdrRef(data);
    return lRdh.memorySize;
  }

  virtual inline
  std::uint32_t getOffsetToNext(const char* data) const override final {
    const RDH &lRdh = getHdrRef(data);
    return lRdh.offsetToNext;
  }

  virtual inline
  bool getStopBit(const char* data) const override final {
    const RDH &lRdh = getHdrRef(data);
    return lRdh.stop;
  }

  // RDH trigger information
  virtual inline
  std::uint32_t getOrbit(const char* data) const override final {
    const RDH &lRdh = getHdrRef(data);
    if constexpr(std::is_same_v<RDH, o2::header::RAWDataHeaderV4>) {
      return lRdh.heartbeatOrbit;
    } else {
      return lRdh.orbit;
    }
  }

  virtual inline
  std::uint16_t getBC(const char* data) const override final {
    const RDH &lRdh = getHdrRef(data);
    if constexpr(std::is_same_v<RDH, o2::header::RAWDataHeaderV4>) {
      return lRdh.heartbeatBC;
    } else {
      return lRdh.bunchCrossing;
    }
  }
  virtual inline
  std::uint32_t getTriggerType(const char* data) const override final {
    const RDH &lRdh = getHdrRef(data);
    return lRdh.triggerType;
  }
};

using RDHv3Reader = RDHReaderImpl<o2::header::RAWDataHeaderV4>; // V4 and V3 are equivalent?
using RDHv4Reader = RDHReaderImpl<o2::header::RAWDataHeaderV4>; // V4 and V3 are equivalent?
using RDHv5Reader = RDHReaderImpl<o2::header::RAWDataHeaderV5>;
using RDHv6Reader = RDHReaderImpl<o2::header::RAWDataHeaderV6>;

class RDHReader {
  // NOTE: This must be set in the Init() phase. Logical program error otherwise.
  static std::unique_ptr<RDHReaderIf> sRDHReader;
  inline static const RDHReaderIf& I() { return *sRDHReader; }

  // store the RDH pointer for later use
  char *mData;
  std::size_t mSize;
  std::size_t mRDHSize;

  RDHReader()
  : mData(nullptr),
    mSize(0),
    mRDHSize(0) {}

  RDHReader(const char* data, const std::size_t size, const std::size_t rsize)
  : mData(const_cast<char*>(data)),
    mSize(size),
    mRDHSize(rsize) {}
public:

  static void Initialize(const unsigned pVer) {
    switch (pVer) {
      case 3:
        sRDHReader = std::make_unique<RDHv3Reader>();
        break;
      case 4:
        sRDHReader = std::make_unique<RDHv4Reader>();
        break;
      case 5:
        sRDHReader = std::make_unique<RDHv5Reader>();
        break;
      case 6:
        sRDHReader = std::make_unique<RDHv6Reader>();
        break;
      default:
        DDLOGF(fair::Severity::error, "Unknown RDH version! version={}"
          "Supported versions are RDHv3, RDHv4, RDHv5, and RDHv6", pVer);
        throw std::runtime_error("Unknown RDH version");
        break;
    }
  }

  RDHReader(const char* data, const std::size_t size)
  : mData(const_cast<char*>(data)),
    mSize(size)
  {
    if (!sRDHReader) {
      DDLOGF(fair::Severity::WARNING, "RDH version not initialized manually! Using the value from the first data packet.");
      if (size > 0) {
        Initialize(data[0]);
      } else {
        throw RDHReaderException(data, size, "Cannot determine RDH version. Requested=" + std::to_string(data[0]));
      }
    }
    assert(!!sRDHReader);
    mRDHSize = sRDHReader->CheckRdhData(mData, mSize);
  }

  explicit RDHReader(const FairMQMessagePtr &msg)
  : RDHReader(reinterpret_cast<const char*>(msg->GetData()), msg->GetSize()) { }

  RDHReader(const RDHReader &b) = default;
  RDHReader(RDHReader &&b) = default;

  constexpr RDHReader& operator=(const RDHReader &b) {
    mData = b.mData;
    mSize = b.mSize;
    mRDHSize = b.mRDHSize;
    return *this;
  }

  inline
  RDHReader next() const {
    if (getStopBit()) {
      return RDHReader();
    }

    const auto lOffNext = this->getOffsetToNext();
    if (lOffNext < 64 || lOffNext > 8192) {
      return RDHReader(); // error
    }

    const char *p = mData + lOffNext;

    if (((mData + mSize) - mRDHSize) < p) {
      return RDHReader(); // the rest of original buffer is too short
    }

    return RDHReader(p, mData + mSize - p, mRDHSize);
  }

  inline
  RDHReader end() const { return RDHReader(); }

  inline
  bool operator==(const RDHReader& b) {
    if (mData == b.mData && mSize == b.mSize) {
      return true;
    }
    return false;
  }

  inline
  bool operator!=(const RDHReader &b) {
    return !(*this == b);
  }

  inline
  std::size_t getRDHSize() const { return mRDHSize; };

  // RDH equipment
  inline
  std::uint8_t getSystemID() const { return I().getSystemID(mData); }

  inline
  std::uint64_t getFeeID() const { return I().getFeeID(mData); }

  inline
  std::uint16_t getLinkID() const { return I().getLinkID(mData); }

  inline
  std::uint8_t getEndPointID() const { return I().getEndPointID(mData); }

  inline
  std::uint16_t getCruID() const { return I().getCruID(mData); }

  // RDH memory layout
  inline
  std::uint32_t getMemorySize() const { return I().getMemorySize(mData); }

  inline
  std::uint32_t getOffsetToNext() const { return I().getOffsetToNext(mData); }

  inline
  bool getStopBit() const { return I().getStopBit(mData); }

  // RDH trigger information
  inline
  std::uint32_t getOrbit() const { return I().getOrbit(mData); };

  inline
  std::uint16_t getBC() const { return I().getBC(mData); };

  inline
  std::uint32_t getTriggerType() const { return I().getTriggerType(mData); };
};

////////////////////////////////////////////////////////////////////////////////
/// ReadoutSubTimeframeHeader
////////////////////////////////////////////////////////////////////////////////

// FIXME: copied from Readout/SubTimeframe.h
// definition of the header message for a subtimeframe made of 1
// message with this header followed by a message for each HBFrame
// All data belong to the same source (a link or user logic)

struct ReadoutSubTimeframeHeader {
  uint8_t  mVersion = 2;
  uint32_t mTimeFrameId = 0; // id of timeframe
  uint32_t mRunNumber = 0;
  uint8_t mSystemId = 0xFF;
  uint16_t mFeeId = 0xFFFF;
  uint16_t mEquipmentId = 0xFFFF;
  uint8_t mLinkId = 0xFF;
  uint32_t mTimeframeOrbitFirst = 0;
  uint32_t mTimeframeOrbitLast = 0;
  struct {
    uint8_t mLastTFMessage : 1; // bit 0
    uint8_t mIsRdhFormat : 1;   // bit 1
    uint8_t mFlagsUnused : 6;   // bit 2-7: unused
  } mFlags;
};

class ReadoutDataUtils {
public:
  enum SubSpecMode {
    eCruLinkId,
    eFeeId
  };
  static SubSpecMode sRawDataSubspectype;

  enum SanityCheckMode {
    eNoSanityCheck = 0,
    eSanityCheckDrop,
    eSanityCheckPrint
  };
  static SanityCheckMode sRdhSanityCheckMode;

  enum RdhVersion {
    eRdhInvalid = -1,
    eRdhVer3 = 3,
    eRdhVer4 = 4,
    eRdhVer5 = 5,
    eRdhVer6 = 6,
  };

  static o2::header::DataOrigin sSpecifiedDataOrigin; // to be initialized if not RDH6
  static RdhVersion sRdhVersion;

  static bool sEmptyTriggerHBFrameFilterring;

  static std::uint32_t sFirstSeenHBOrbitCnt;

  static o2::header::DataOrigin getDataOrigin(const RDHReader &R);
  static o2::header::DataHeader::SubSpecificationType getSubSpecification(const RDHReader &R);

  static std::tuple<std::size_t, bool> getHBFrameMemorySize(const FairMQMessagePtr &pMsg);

  static bool rdhSanityCheck(const char* data, const std::size_t len);
  static bool filterEmptyTriggerBlocks(const char* pData, const std::size_t pLen);
};

std::istream& operator>>(std::istream& in, ReadoutDataUtils::SanityCheckMode& pRetVal);
std::istream& operator>>(std::istream& in, ReadoutDataUtils::SubSpecMode& pRetVal);
std::istream& operator>>(std::istream& in, ReadoutDataUtils::RdhVersion& pRetVal);

std::string to_string (ReadoutDataUtils::SubSpecMode c);

}
} /* o2::DataDistribution */

#endif /* ALICEO2_READOUT_DATAMODEL_H_ */
