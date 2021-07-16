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

#include <vector>

#include <fairmq/FwdDecls.h>

namespace o2
{
namespace DataDistribution
{

////////////////////////////////////////////////////////////////////////////////
/// InterleavedHdrDataSerializer
////////////////////////////////////////////////////////////////////////////////

class InterleavedHdrDataSerializer : public ISubTimeFrameVisitor
{
 public:
  InterleavedHdrDataSerializer() = delete;
  InterleavedHdrDataSerializer(FairMQChannel& pChan)
    : mChan(pChan)
  {
    mMessages.reserve(1024);
  }

  virtual ~InterleavedHdrDataSerializer() = default;

  void serialize(std::unique_ptr<SubTimeFrame>&& pStf);

 protected:
  void visit(SubTimeFrame& pStf) override;

 private:
  std::vector<FairMQMessagePtr> mMessages;
  FairMQChannel& mChan;
};

////////////////////////////////////////////////////////////////////////////////
/// InterleavedHdrDataDeserializer
////////////////////////////////////////////////////////////////////////////////

class InterleavedHdrDataDeserializer : public ISubTimeFrameVisitor
{
 public:
  InterleavedHdrDataDeserializer() = default;
  virtual ~InterleavedHdrDataDeserializer() = default;

  std::unique_ptr<SubTimeFrame> deserialize(FairMQChannel& pChan, bool pLogError = false);
  std::unique_ptr<SubTimeFrame> deserialize(FairMQParts& pMsgs);

 protected:
  std::unique_ptr<SubTimeFrame> deserialize_impl();
  void visit(SubTimeFrame& pStf) override;

 private:
  std::vector<FairMQMessagePtr> mMessages;
};


////////////////////////////////////////////////////////////////////////////////
/// CoalescedHdrDataSerializer
////////////////////////////////////////////////////////////////////////////////

class CoalescedHdrDataSerializer : public ISubTimeFrameVisitor
{
 public:

  struct header_info {
    std::size_t start;
    std::size_t len;
  };

  CoalescedHdrDataSerializer() = delete;
  CoalescedHdrDataSerializer(FairMQChannel& pChan)
    : mChan(pChan)
  {
    mData.reserve(25600);
  }

  virtual ~CoalescedHdrDataSerializer() = default;

  void serialize(std::unique_ptr<SubTimeFrame>&& pStf);

  void stop() { mRunning = false; }

 protected:
  void visit(SubTimeFrame& pStf) override;

 private:
  std::atomic_bool mRunning = true;
  std::vector<FairMQMessagePtr> mHdrs;
  std::vector<FairMQMessagePtr> mData;

  FairMQChannel& mChan;
};

////////////////////////////////////////////////////////////////////////////////
/// CoalescedHdrDataDeserializer
////////////////////////////////////////////////////////////////////////////////

class CoalescedHdrDataDeserializer : public ISubTimeFrameVisitor
{
 public:
  CoalescedHdrDataDeserializer() = delete;
  CoalescedHdrDataDeserializer(TimeFrameBuilder &pTfBld)
  : mTfBld(pTfBld) { }
  virtual ~CoalescedHdrDataDeserializer() = default;

  std::unique_ptr<SubTimeFrame> deserialize(FairMQChannel& pChan, bool pLogError = false);
  std::unique_ptr<SubTimeFrame> deserialize(std::vector<FairMQMessagePtr>& pMsgs);

 protected:
  std::unique_ptr<SubTimeFrame> deserialize_impl();
  void visit(SubTimeFrame& pStf) override;

 private:
  std::vector<FairMQMessagePtr> mHdrs;
  std::vector<FairMQMessagePtr> mData;

  TimeFrameBuilder &mTfBld;
};


}
} /* o2::DataDistribution */

#endif /* ALICEO2_SUBTIMEFRAME_VISITORS_H_ */
