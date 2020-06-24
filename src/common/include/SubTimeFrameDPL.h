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

#ifndef ALICEO2_SUBTIMEFRAME_DPL_H_
#define ALICEO2_SUBTIMEFRAME_DPL_H_

#include "SubTimeFrameDataModel.h"
#include "SubTimeFrameVisitors.h"

#include <Headers/DataHeader.h>

namespace o2
{
namespace DataDistribution
{

////////////////////////////////////////////////////////////////////////////////
/// StfDplAdapter
////////////////////////////////////////////////////////////////////////////////

class StfToDplAdapter : public ISubTimeFrameVisitor
{
 public:
  StfToDplAdapter() = delete;
  StfToDplAdapter(FairMQChannel& pDplBridgeChan)
    : mChan(pDplBridgeChan)
  {
    mMessages.reserve(1024);
  }

  virtual ~StfToDplAdapter() = default;

  void sendToDpl(std::unique_ptr<SubTimeFrame>&& pStf);

  inline void stop() { mRunning = false; }

 protected:
  void visit(SubTimeFrame& pStf) override;

 private:
  std::atomic_bool mRunning = true;
  std::vector<FairMQMessagePtr> mMessages;
  FairMQChannel& mChan;
};

////////////////////////////////////////////////////////////////////////////////
/// DplToStfAdapter
////////////////////////////////////////////////////////////////////////////////

class DplToStfAdapter : public ISubTimeFrameVisitor
{
  constexpr static std::uint64_t sCurrentTfId = 0;

 public:
  DplToStfAdapter() = default;
  virtual ~DplToStfAdapter() = default;

  std::unique_ptr<SubTimeFrame> deserialize(FairMQChannel& pChan);
  std::unique_ptr<SubTimeFrame> deserialize(FairMQParts& pMsgs);

 protected:
  std::unique_ptr<SubTimeFrame> deserialize_impl();
  void visit(SubTimeFrame& pStf) override;

 private:
  std::vector<FairMQMessagePtr> mMessages;
};

}
} /* o2::DataDistribution */

#endif /* ALICEO2_SUBTIMEFRAME_DPL_H_ */
