// Copyright CERN and copyright holders of ALICE O2. This software is
// distributed under the terms of the GNU General Public License v3 (GPL
// Version 3), copied verbatim in the file "COPYING".
//
// See http://alice-o2.web.cern.ch/license for full licensing information.
//
// In applying this license CERN does not waive the privileges and immunities
// granted to it by virtue of its status as an Intergovernmental Organization
// or submit itself to any jurisdiction.

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

namespace o2
{
namespace DataDistribution
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
  o2hdr::DataHeader::SubSpecificationType mSubSpecification; /* uint64_t */
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
    return std::string("DataDescription: ") + std::string(mDataDescription.str) +
           std::string(" DataOrigin: ") + std::string(mDataOrigin.str) +
           std::string(" SubSpecification: ") + std::to_string(mSubSpecification);
  }
};

struct HBFrameHeader : public o2hdr::BaseHeader {

  // Required to do the lookup
  static const o2hdr::HeaderType sHeaderType;
  static const uint32_t sVersion = 1;

  uint32_t mHBFrameId;

  HBFrameHeader(uint32_t pId)
    : BaseHeader(sizeof(HBFrameHeader), sHeaderType, o2hdr::gSerializationMethodNone, sVersion),
      mHBFrameId(pId)
  {
  }

  HBFrameHeader()
    : HBFrameHeader(0)
  {
  }
};

////////////////////////////////////////////////////////////////////////////////
/// Visitor friends
////////////////////////////////////////////////////////////////////////////////
#define DECLARE_STF_FRIENDS                    \
  friend class SubTimeFrameReadoutBuilder;     \
  friend class InterleavedHdrDataSerializer;   \
  friend class InterleavedHdrDataDeserializer; \
  friend class DataIdentifierSplitter;         \
  friend class SubTimeFrameFileWriter;         \
  friend class SubTimeFrameFileReader;         \
  friend class StfDplAdapter;

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

    std::unique_ptr<FairMQMessage> mHeader;
    std::unique_ptr<FairMQMessage> mData;

    inline o2hdr::DataHeader getDataHeader() const
    {
      o2hdr::DataHeader lDataHdr;
      // DataHeader must be first in the stack
      std::memcpy(&lDataHdr, mHeader->GetData(), sizeof(o2hdr::DataHeader));
      return lDataHdr;
    }

    inline void setPayloadIndex(o2hdr::DataHeader::SplitPayloadIndexType pIdx,
      o2hdr::DataHeader::SplitPayloadPartsType pTotal)
    {
      assert(mHeader && mHeader->GetData() != nullptr);
      assert(pIdx < pTotal);

      // TODO: get returns const ptr
      // DataHeader must be first in the stack
      // DataHeader *lHdr = o2hdr::get<o2hdr::DataHeader*>(static_cast<o2::byte*>(mHeader->GetData()), mHeader->GetSize());

      o2hdr::DataHeader lDataHdr;
      // DataHeader must be first in the stack
      std::memcpy(&lDataHdr, mHeader->GetData(), sizeof(o2hdr::DataHeader));
      lDataHdr.splitPayloadIndex = pIdx;
      lDataHdr.splitPayloadParts = pTotal;
      std::memcpy(mHeader->GetData(), &lDataHdr, sizeof(o2hdr::DataHeader));
    }
  };

 public:
  SubTimeFrame(TimeFrameIdType pStfId);
  //SubTimeFrame() = default;
  ~SubTimeFrame() = default;
  // no copy
  SubTimeFrame(const SubTimeFrame&) = delete;
  SubTimeFrame& operator=(const SubTimeFrame&) = delete;
  // default move
  SubTimeFrame(SubTimeFrame&& a) = default;
  SubTimeFrame& operator=(SubTimeFrame&& a) = default;

  // adopt all data from a
  void mergeStf(std::unique_ptr<SubTimeFrame> pStf);

  std::uint64_t getDataSize() const;

  std::vector<EquipmentIdentifier> getEquipmentIdentifiers() const;

  struct Header {
    TimeFrameIdType mId = sInvalidTimeFrameId;
  };

  const Header& header() const { return mHeader; }

 protected:
  void accept(ISubTimeFrameVisitor& v) override { updateStf(mData); v.visit(*this); }
  void accept(ISubTimeFrameConstVisitor& v) const override { updateStf(mData); v.visit(*this); }

 private:
  using StfDataVector = std::vector<StfData>;
  using StfSubSpecMap = std::unordered_map<o2hdr::DataHeader::SubSpecificationType, StfDataVector>;
  using StfDataIdentMap = std::unordered_map<o2hdr::DataIdentifier, StfSubSpecMap>;

  ///
  /// Fields
  ///
  Header mHeader;
  mutable StfDataIdentMap mData;

  ///
  /// internal
  ///
  mutable bool _mUpdated = false;

  ///
  /// helper methods
  ///
  inline void addStfData(const o2hdr::DataHeader& pDataHeader, StfData&& pStfData)
  {
    const o2hdr::DataIdentifier lDataId = impl::getDataIdentifier(pDataHeader);

    auto& lDataVector = mData[lDataId][pDataHeader.subSpecification];

    // allocate enough space
    const auto lCap = lDataVector.capacity();
    if (lCap < 1024) {
      lDataVector.reserve(std::max(lCap * 2, StfDataVector::size_type(1024)));
    }

    lDataVector.emplace_back(std::move(pStfData));
    _mUpdated = false;
  }

  inline void addStfData(StfData&& pStfData)
  {
    const o2hdr::DataHeader lDataHeader = pStfData.getDataHeader();
    addStfData(lDataHeader, std::move(pStfData));
  }

  // NOTE: method declared const to work with const visitors, manipulated fields are mutable
  inline void updateStf(StfDataIdentMap &pData) const
  {
    if (_mUpdated) {
      return;
    }

    // Update data block indexes
    for (auto &lIdentSubSpecVect : pData) {
      StfSubSpecMap &lSubSpecMap = lIdentSubSpecVect.second;

      for (auto &lSubSpecDataVector : lSubSpecMap) {
        StfDataVector &lDataVector = lSubSpecDataVector.second;

        const auto lTotalCount = lDataVector.size();
        for (StfDataVector::size_type i = 0; i < lTotalCount; i++) {
          lDataVector[i].setPayloadIndex(i, lTotalCount);
        }

        assert(lDataVector.empty() ? true :
          lDataVector.front().getDataHeader().splitPayloadIndex == 0
        );
        assert(lDataVector.empty() ? true :
          lDataVector.back().getDataHeader().splitPayloadIndex == (lTotalCount - 1)
        );
        assert(lDataVector.empty() ? true :
          lDataVector.front().getDataHeader().splitPayloadParts == lTotalCount
        );
        assert(lDataVector.empty() ? true :
          lDataVector.front().getDataHeader().splitPayloadParts ==
          lDataVector.back().getDataHeader().splitPayloadParts
        );

      }

    }

    _mUpdated = true;
  }

};
}
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
