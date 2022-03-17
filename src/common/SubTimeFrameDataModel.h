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

#ifndef ALICEO2_SUBTIMEFRAME_DATAMODEL_H_
#define ALICEO2_SUBTIMEFRAME_DATAMODEL_H_

#include "Utilities.h"
#include "DataModelUtils.h"
#include "ReadoutDataModel.h"

#include <Headers/DataHeader.h>

#include <vector>
#include <map>
#include <unordered_set>
#include <stdexcept>

#include <functional>

namespace std
{

template <>
struct hash<o2::header::DataDescription> {
  typedef o2::header::DataDescription argument_type;
  typedef std::uint64_t result_type;

  result_type operator()(argument_type const& a) const noexcept
  {

    static_assert(sizeof(o2::header::DataDescription::ItgType) == sizeof(uint64_t) &&
                    sizeof(o2::header::DataDescription) == 16,
                  "DataDescription must be 16B long (uint64_t itg[2])");

    return std::hash<o2::header::DataDescription::ItgType>{}(a.itg[0]) ^
           std::hash<o2::header::DataDescription::ItgType>{}(a.itg[1]);
  }
};

template <>
struct hash<o2::header::DataOrigin> {
  typedef o2::header::DataOrigin argument_type;
  typedef std::uint32_t result_type;

  result_type operator()(argument_type const& a) const noexcept
  {

    static_assert(sizeof(o2::header::DataOrigin::ItgType) == sizeof(uint32_t) &&
                    sizeof(o2::header::DataOrigin) == 4,
                  "DataOrigin must be 4B long (uint32_t itg[1])");

    return std::hash<o2::header::DataOrigin::ItgType>{}(a.itg[0]);
  }
};

template <>
struct hash<o2::header::DataIdentifier> {
  typedef o2::header::DataIdentifier argument_type;
  typedef std::uint64_t result_type;

  result_type operator()(argument_type const& a) const noexcept
  {

    return std::hash<o2::header::DataDescription>{}(a.dataDescription) ^
           std::hash<o2::header::DataOrigin>{}(a.dataOrigin);
  }
};

} //namespace std

namespace o2::DataDistribution
{

namespace o2hdr = o2::header;

namespace impl
{
static inline o2hdr::DataIdentifier getDataIdentifier(const o2hdr::DataHeader& pDataHdr)
{
  o2hdr::DataIdentifier lRetId;
  lRetId.dataDescription = pDataHdr.dataDescription;
  lRetId.dataOrigin = pDataHdr.dataOrigin;
  return lRetId;
}
}

static constexpr o2hdr::DataDescription gDataDescSubTimeFrame{ "DISTSUBTIMEFRAME" };

struct EquipmentIdentifier {
  o2hdr::DataDescription mDataDescription;                   /* 2 x uint64_t */
  o2hdr::DataHeader::SubSpecificationType mSubSpecification; /* uint32_t */
  o2hdr::DataOrigin mDataOrigin;                             /* 1 x uint32_t */

  EquipmentIdentifier() = delete;

  EquipmentIdentifier(const o2hdr::DataDescription& pDataDesc,
    const o2hdr::DataOrigin& pDataOrig,
    const o2hdr::DataHeader::SubSpecificationType& pSubSpec) noexcept
    : mDataDescription(pDataDesc),
      mSubSpecification(pSubSpec),
      mDataOrigin(pDataOrig)
  {
  }

  EquipmentIdentifier(const o2hdr::DataIdentifier& pDataId,
    const o2hdr::DataHeader::SubSpecificationType& pSubSpec) noexcept
    : EquipmentIdentifier(pDataId.dataDescription, pDataId.dataOrigin, pSubSpec)
  {
  }

  EquipmentIdentifier(const EquipmentIdentifier& pEid) noexcept
    : EquipmentIdentifier(pEid.mDataDescription, pEid.mDataOrigin, pEid.mSubSpecification)
  {
  }

  EquipmentIdentifier(const o2hdr::DataHeader& pDh) noexcept
    : EquipmentIdentifier(pDh.dataDescription, pDh.dataOrigin, pDh.subSpecification)
  {
  }

  operator o2hdr::DataIdentifier() const noexcept
  {
    o2hdr::DataIdentifier lRetId;
    lRetId.dataDescription = mDataDescription;
    lRetId.dataOrigin = mDataOrigin;
    return lRetId;
  }

  bool operator<(const EquipmentIdentifier& other) const noexcept
  {
    if (mDataDescription < other.mDataDescription) {
      return true;
    }

    if (mDataDescription == other.mDataDescription && mDataOrigin < other.mDataOrigin) {
      return true;
    }

    if (mDataDescription == other.mDataDescription && mDataOrigin == other.mDataOrigin &&
        mSubSpecification < other.mSubSpecification) {
      return true;
    }

    return false;
  }

  bool operator==(const EquipmentIdentifier& other) const noexcept
  {
    if (mDataDescription == other.mDataDescription &&
        mSubSpecification == other.mSubSpecification &&
        mDataOrigin == other.mDataOrigin)
      return true;
    else
      return false;
  }

  bool operator!=(const EquipmentIdentifier& other) const noexcept
  {
    return !(*this == other);
  }

  const std::string info() const
  {
    return fmt::format("{}/{}/{}",
      mDataOrigin.as<std::string>(),
      mDataDescription.as<std::string>(),
      mSubSpecification);
  }
};


////////////////////////////////////////////////////////////////////////////////
/// Visitor friends
////////////////////////////////////////////////////////////////////////////////
#define DECLARE_STF_FRIENDS                    \
  friend class SubTimeFrameReadoutBuilder;     \
  friend class SubTimeFrameFileBuilder;        \
  friend class TimeFrameBuilder;               \
  friend class SubTimeFrameFileWriter;         \
  friend class SubTimeFrameFileReader;         \
  friend class StfToDplAdapter;                \
  friend class DplToStfAdapter;                \
  friend class IovSerializer;                  \
  friend class IovDeserializer;                \
  friend class StfSenderOutputUCX;

////////////////////////////////////////////////////////////////////////////////
/// SubTimeFrame
////////////////////////////////////////////////////////////////////////////////
using TimeFrameIdType = std::uint64_t;
using SubTimeFrameIdType = TimeFrameIdType;
static constexpr TimeFrameIdType sInvalidTimeFrameId = TimeFrameIdType(-1);

class SubTimeFrame : public IDataModelObject
{
  DECLARE_STF_FRIENDS

  struct StfData {

    StfData() = delete;
    StfData(std::unique_ptr<FairMQMessage> &&pHdr, std::unique_ptr<FairMQMessage> &&pData)
    : mHeader(std::move(pHdr)), mData(std::move(pData)) { }

    std::unique_ptr<FairMQMessage> mHeader;
    std::unique_ptr<FairMQMessage> mData;

    inline const o2hdr::DataHeader* getDataHeader() const
    {
      if (!mHeader) {
        return nullptr;
      }
      // this is fine since we created the DataHeader there
      return reinterpret_cast<o2hdr::DataHeader*>(mHeader->GetData());
    }
  };

  struct StfMessage {

    StfMessage() = delete;

    StfMessage(std::unique_ptr<FairMQMessage> &&pHeader)
      : mHeader(std::move(pHeader))
    {
      assert(mHeader);
    }

    std::unique_ptr<FairMQMessage> mHeader;
    std::vector<std::unique_ptr<FairMQMessage>> mDataParts;

    inline o2hdr::DataHeader* getDataHeaderMutable() {
      if (!mHeader) {
        return nullptr;
      }
      // this is fine since we created the DataHeader there
      return reinterpret_cast<o2hdr::DataHeader*>(mHeader->GetData());
    }

    inline o2hdr::DataHeader getDataHeaderCopy() const
    {
      return *reinterpret_cast<o2hdr::DataHeader*>(mHeader->GetData());
    }

    inline void setTfCounter_RunNumber(const std::uint32_t pTfCounter, const std::uint32_t pRunNumber) {
      // can be removed if redundant
      if (!mHeader) {
        return;
      }
      // DataHeader must be first in the stack
      o2hdr::DataHeader *lDataHdr = reinterpret_cast<o2hdr::DataHeader*>(mHeader->GetData());
      lDataHdr->tfCounter = pTfCounter;
      lDataHdr->runNumber = pRunNumber;
    }

    inline void setFirstOrbit(const std::uint32_t pFirstOrbit)
    {
      // Redundant headers can be removed
      if (!mHeader) {
        return;
      }
      // DataHeader must be first in the stack
      o2hdr::DataHeader *lDataHdr = reinterpret_cast<o2hdr::DataHeader*>(mHeader->GetData());
      lDataHdr->firstTForbit = pFirstOrbit;
    }
  };

  // we SHOULD be able to get away with this
  // make sure the vector has sufficient initial capacity
  struct StfDataVectorT : public std::vector<StfMessage> {
    StfDataVectorT() : std::vector<StfMessage>() { reserve(512); }

    StfDataVectorT(const StfDataVectorT&) = delete;
    StfDataVectorT(StfDataVectorT&&) = default;
  };

 public:
  SubTimeFrame(TimeFrameIdType pStfId);
  //SubTimeFrame() = default;
  virtual ~SubTimeFrame() = default;
  // no copy
  SubTimeFrame(const SubTimeFrame&) = delete;
  SubTimeFrame& operator=(const SubTimeFrame&) = delete;
  // default move
  SubTimeFrame(SubTimeFrame&& a) = default;
  SubTimeFrame& operator=(SubTimeFrame&& a) = default;

  // adopt all data from another STF
  void mergeStf(std::unique_ptr<SubTimeFrame> pStf, const std::string &mStfSenderId);

  // get data size (not including o2 headers)
  std::uint64_t getDataSize() const { updateStf(); return mDataSize; }

  std::vector<EquipmentIdentifier> getEquipmentIdentifiers() const;

  struct Header {
    static constexpr std::uint64_t sInvalidTimeMs = std::numeric_limits<std::uint64_t>::max();
    TimeFrameIdType mId = sInvalidTimeFrameId;
    std::uint32_t mFirstOrbit = std::numeric_limits<std::uint32_t>::max();
    std::uint32_t mRunNumber = 0;
    enum Origin {
      eInvalid = -1,
      eReadout = 1,
      eReadoutTopology, // MFT/ITS topology run
      eNull
    } mOrigin = eInvalid;
    std::uint64_t mCreationTimeMs = sInvalidTimeMs; // miliseconds since unix epoch

    Header() = default;
    explicit Header(TimeFrameIdType pId)
    : mId(pId) { }
  };

  const Header& header() const { return mHeader; }
  TimeFrameIdType id() const { return mHeader.mId; }
  Header::Origin origin() const { return mHeader.mOrigin; }
  void setOrigin(const Header::Origin pOrig) {
    if (mHeader.mOrigin == Header::Origin::eInvalid) {
      mHeader.mOrigin = pOrig;
    }
  }

  void clear() { mData.clear(); mDataUpdated = false; }
  // NOTE: method declared const to work with const visitors, manipulated fields are mutable
  void updateStf() const;

 protected:
  void accept(ISubTimeFrameVisitor& v, void *p = nullptr) override { updateStf(); v.visit(*this, p); }
  void accept(ISubTimeFrameConstVisitor& v, void *p = nullptr) const override { updateStf(); v.visit(*this, p); }

 private:

  using StfDataVector = StfDataVectorT; // vector with more capacity on creation
  using StfSubSpecMap = std::unordered_map<o2hdr::DataHeader::SubSpecificationType, StfDataVector>;
  using StfDataIdentMap = std::unordered_map<o2hdr::DataIdentifier, StfSubSpecMap>;

  ///
  /// Fields
  ///
  Header mHeader;
  mutable StfDataIdentMap mData;
  mutable std::uint64_t mDataSize = 0;

  ///
  /// internal: do lazy accounting. Must be invalidated every time StubTimeFrame is changed
  ///
  mutable bool mDataUpdated = false;

public:

  void updateId(const std::uint64_t pNewId) {
    if (pNewId > 0) {
      mHeader.mId = pNewId;
      mDataUpdated = false;
    }
  }

  void updateFirstOrbit(const std::uint32_t pOrbit) {
    if (pOrbit < mHeader.mFirstOrbit) {
      mHeader.mFirstOrbit = pOrbit;
      mDataUpdated = false;
    }
  }

  void updateRunNumber(const std::uint32_t pRunNum) {
    if (mHeader.mRunNumber != pRunNum) {
      mHeader.mRunNumber = pRunNum;
      mDataUpdated = false;
    }
  }

  void updateCreationTimeMs() {
    mHeader.mCreationTimeMs = std::chrono::time_point_cast<std::chrono::milliseconds>(std::chrono::system_clock::now()).time_since_epoch().count();
  }

  void updateCreationTimeMs(const std::uint64_t pTimeMs) {
    mHeader.mCreationTimeMs = pTimeMs;
  }

private:
  ///
  /// helper methods
  ///

  inline bool stfDataExists(const o2hdr::DataIdentifier &pDataId, const o2::header::DataHeader::SubSpecificationType pSubSpec)
  {
    const auto lDataIt = mData.find(pDataId);
    if (lDataIt == mData.end()) {
      return false;
    }

    if (lDataIt->second.count(pSubSpec) > 0) {
      return true;
    }

    return false;
  }

  // This is only to be used with the data building from readout
  // in this case, only a single split-payload will be present in the data vector
  // If any other header is provided, it can be discarded
  inline auto addStfDataReadout(const o2hdr::DataIdentifier &pDataId, const o2::header::DataHeader::SubSpecificationType pSubSpec, StfData&& pStfData)
  {
    auto &lDataVec = mData[pDataId][pSubSpec];

    if (lDataVec.empty()) {
      assert(pStfData.mHeader);

      lDataVec.emplace_back(StfMessage(std::move(pStfData.mHeader)));
    }

    if (!lDataVec.empty()) {
      // remove the header
      pStfData.mHeader = nullptr;
    }

    auto &lNewMsg = lDataVec.back();

    lNewMsg.mDataParts.reserve(512); // usually HBFs
    lNewMsg.mDataParts.emplace_back(std::move(pStfData.mData));
    mDataUpdated = false;

    return &(lNewMsg.mDataParts);
  }
  // optimized version without vector lookup
  inline void addStfDataReadout(std::vector<FairMQMessagePtr> *pInVector, FairMQMessagePtr &&pData)
  {
    if (!pInVector || pInVector->empty()) {
      EDDLOG("BUG: addStfDataAppend: No header message.");
      return;
    }

    pInVector->emplace_back(std::move(pData));
    mDataUpdated = false;
  }


  // Start a new single or split-payload message
  // e.g. when deserializing a channel or a file
  inline auto addStfDataStart(const o2hdr::DataIdentifier &pDataId, const o2::header::DataHeader::SubSpecificationType pSubSpec, StfData&& pStfData)
  {
    auto &lDataVec = mData[pDataId][pSubSpec];
    auto &lNewMsg = lDataVec.emplace_back(StfMessage(std::move(pStfData.mHeader)));

    lNewMsg.mDataParts.reserve(512);
    lNewMsg.mDataParts.emplace_back(std::move(pStfData.mData));

    mDataUpdated = false;
    return &(lNewMsg.mDataParts);
  }

  // Append a split-payload message
  // e.g. when deserializing a channel or a file
  inline void addStfDataAppend(std::vector<FairMQMessagePtr> *pInVector, FairMQMessagePtr &&pData)
  {
    if (!pInVector || pInVector->empty()) {
      EDDLOG("BUG: addStfDataAppend: No header message");
      return;
    }

    pInVector->emplace_back(std::move(pData));
    mDataUpdated = false;
  }

};

} /* o2::DataDistribution */

namespace std
{

template <>
struct hash<o2::DataDistribution::EquipmentIdentifier> {
  typedef o2::DataDistribution::EquipmentIdentifier argument_type;
  typedef std::uint64_t result_type;

  result_type operator()(argument_type const& a) const noexcept
  {

    return std::hash<o2::header::DataDescription>{}(a.mDataDescription) ^
           (std::hash<o2::header::DataOrigin>{}(a.mDataOrigin) << 1) ^
           a.mSubSpecification;
  }
};

} //namespace std

#endif /* ALICEO2_SUBTIMEFRAME_DATAMODEL_H_ */
