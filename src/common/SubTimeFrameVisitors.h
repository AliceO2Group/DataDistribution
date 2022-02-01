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

#ifndef ALICEO2_SUBTIMEFRAME_VISITORS_H_
#define ALICEO2_SUBTIMEFRAME_VISITORS_H_

#include "SubTimeFrameDataModel.h"
#include "SubTimeFrameBuilder.h"

#include <Headers/DataHeader.h>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include <discovery.pb.h>
#pragma GCC diagnostic pop

#include <vector>

#include <fairmq/FwdDecls.h>

namespace o2
{
namespace DataDistribution
{


////////////////////////////////////////////////////////////////////////////////
/// IovSerializer
////////////////////////////////////////////////////////////////////////////////

class IovSerializer : public ISubTimeFrameVisitor
{
 public:

  IovSerializer() = delete;
  IovSerializer(FairMQChannel& pChan)
    : mChan(pChan)
  {
    mData.reserve(25600);
  }

  virtual ~IovSerializer() = default;

  void serialize(std::unique_ptr<SubTimeFrame>&& pStf);

  void stop() { mRunning = false; }

 protected:
  void visit(SubTimeFrame& pStf) override;

 private:
  std::atomic_bool mRunning = true;

  std::vector<FairMQMessagePtr> mData;

  IovStfHeader mIovHeader;

  FairMQChannel& mChan;
};


////////////////////////////////////////////////////////////////////////////////
/// IovDeserializer
////////////////////////////////////////////////////////////////////////////////

class IovDeserializer : public ISubTimeFrameVisitor
{
 public:
  IovDeserializer() = delete;
  IovDeserializer(TimeFrameBuilder &pTfBld)
  : mTfBld(pTfBld) { }
  virtual ~IovDeserializer() = default;

  std::unique_ptr<SubTimeFrame> deserialize(FairMQChannel& pChan, bool pLogError = false);
  std::unique_ptr<SubTimeFrame> deserialize(FairMQMessagePtr &pHdrMetaMsg, std::vector<FairMQMessagePtr>& pDataMsgs);

  SubTimeFrame::Header peek_tf_header(FairMQMessagePtr& pHeaderMsg) const;

  bool copy_to_region(std::vector<FairMQMessagePtr>& pMsgs /* in/out */);

 protected:
  std::unique_ptr<SubTimeFrame> deserialize_impl();
  void visit(SubTimeFrame& pStf) override;

 private:
  IovStfHeader mIovHeader;
  std::vector<FairMQMessagePtr> mHdrs;
  std::vector<FairMQMessagePtr> mData;

  TimeFrameBuilder &mTfBld;
};


}
} /* o2::DataDistribution */

#endif /* ALICEO2_SUBTIMEFRAME_VISITORS_H_ */
