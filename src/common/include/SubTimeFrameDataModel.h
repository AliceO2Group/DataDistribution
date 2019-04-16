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

#include <O2Device/O2Device.h>
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

namespace impl
{
static inline o2::header::DataIdentifier getDataIdentifier(const o2::header::DataHeader& pDataHdr)
{
  o2::header::DataIdentifier lRetId;
  lRetId.dataDescription = pDataHdr.dataDescription;
  lRetId.dataOrigin = pDataHdr.dataOrigin;
  return lRetId;
}
}

using namespace o2::base;
using namespace o2::header;

static constexpr o2::header::DataDescription gDataDescSubTimeFrame{ "DISTSUBTIMEFRAME" };

struct EquipmentIdentifier {
  DataDescription mDataDescription;                   /* 2 x uint64_t */
  DataHeader::SubSpecificationType mSubSpecification; /* uint64_t */
  DataOrigin mDataOrigin;                             /* 1 x uint32_t */

  EquipmentIdentifier() = delete;

  EquipmentIdentifier(const DataDescription& pDataDesc, const DataOrigin& pDataOrig, const DataHeader::SubSpecificationType& pSubSpec) noexcept
    : mDataDescription(pDataDesc),
      mSubSpecification(pSubSpec),
      mDataOrigin(pDataOrig)
  {
  }

  EquipmentIdentifier(const DataIdentifier& pDataId, const DataHeader::SubSpecificationType& pSubSpec) noexcept
    : EquipmentIdentifier(pDataId.dataDescription, pDataId.dataOrigin, pSubSpec)
  {
  }

  EquipmentIdentifier(const EquipmentIdentifier& pEid) noexcept
    : EquipmentIdentifier(pEid.mDataDescription, pEid.mDataOrigin, pEid.mSubSpecification)
  {
  }

  EquipmentIdentifier(const o2::header::DataHeader& pDh) noexcept
    : EquipmentIdentifier(pDh.dataDescription, pDh.dataOrigin, pDh.subSpecification)
  {
  }

  operator DataIdentifier() const noexcept
  {
    DataIdentifier lRetId;
    lRetId.dataDescription = mDataDescription;
    lRetId.dataOrigin = mDataOrigin;
    return lRetId;
  }

  bool operator<(const EquipmentIdentifier& other) const noexcept
  {
    if (mDataDescription < other.mDataDescription)
      return true;

    else if (mDataDescription == other.mDataDescription &&
             mDataOrigin < other.mDataOrigin)
      return true;

    else if (mDataDescription == other.mDataDescription &&
             mDataOrigin == other.mDataOrigin &&
             mSubSpecification < other.mSubSpecification)
      return true;
    else
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

struct HBFrameHeader : public BaseHeader {

  // Required to do the lookup
  static const o2::header::HeaderType sHeaderType;
  static const uint32_t sVersion = 1;

  uint32_t mHBFrameId;

  HBFrameHeader(uint32_t pId)
    : BaseHeader(sizeof(HBFrameHeader), sHeaderType, o2::header::gSerializationMethodNone, sVersion),
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

    DataHeader getDataHeader()
    {
      DataHeader lDataHdr;
      std::memcpy(&lDataHdr, mHeader->GetData(), sizeof(DataHeader));
      return lDataHdr;
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
  void accept(ISubTimeFrameVisitor& v) override { v.visit(*this); }
  void accept(ISubTimeFrameConstVisitor& v) const override { v.visit(*this); }

 private:
  using StfDataVector = std::vector<StfData>;
  using StfSubSpecMap = std::unordered_map<DataHeader::SubSpecificationType, StfDataVector>;
  using StfDataIdentMap = std::unordered_map<DataIdentifier, StfSubSpecMap>;

  ///
  /// Fields
  ///
  Header mHeader;
  StfDataIdentMap mData;

  ///
  /// helper methods
  ///
  inline void addStfData(const DataHeader& pDataHeader, StfData&& pStfData)
  {
    const DataIdentifier lDataId = impl::getDataIdentifier(pDataHeader);

    auto& lDataVector = mData[lDataId][pDataHeader.subSpecification];

    lDataVector.reserve(512);
    lDataVector.emplace_back(std::move(pStfData));
  }

  inline void addStfData(StfData&& pStfData)
  {
    const DataHeader lDataHeader = pStfData.getDataHeader();
    addStfData(lDataHeader, std::move(pStfData));
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
