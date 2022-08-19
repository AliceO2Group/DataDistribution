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
  IovSerializer(fair::mq::Channel& pChan)
    : mChan(pChan)
  {
    mData.reserve(25600);
  }

  virtual ~IovSerializer() = default;

  void serialize(std::unique_ptr<SubTimeFrame>&& pStf);

  void stop() { mRunning = false; }

 protected:
  void visit(SubTimeFrame& pStf, void*) override;

 private:
  std::atomic_bool mRunning = true;

  std::vector<fair::mq::MessagePtr> mData;

  IovStfHeader mIovHeader;

  fair::mq::Channel& mChan;
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

  std::unique_ptr<SubTimeFrame> deserialize(const IovStfHdrMeta &pHdrMeta, std::vector<fair::mq::MessagePtr>& pDataMsgs);

  SubTimeFrame::Header peek_tf_header(const IovStfHdrMeta &pHdrMeta) const;

  bool copy_to_region(std::vector<fair::mq::MessagePtr>& pMsgs /* in/out */);

 protected:
  std::unique_ptr<SubTimeFrame> deserialize_impl();
  void visit(SubTimeFrame& pStf, void*) override;

 private:
  // IovStfHeader mIovHeader;
  IovStfHdrMeta mIovStfHeader;

  std::vector<fair::mq::MessagePtr> mHdrs;
  std::vector<fair::mq::MessagePtr> mData;

  TimeFrameBuilder &mTfBld;
};


}
} /* o2::DataDistribution */

#endif /* ALICEO2_SUBTIMEFRAME_VISITORS_H_ */
