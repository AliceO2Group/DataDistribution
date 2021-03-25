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

#include "TfBuilderRpc.h"
#include <grpcpp/grpcpp.h>

#include <DataDistLogger.h>

#include <condition_variable>
#include <stdexcept>

namespace o2
{
namespace DataDistribution
{

using namespace std::chrono_literals;

void TfBuilderRpcImpl::initDiscovery(const std::string pRpcSrvBindIp, int &lRealPort /*[out]*/)
{
  // start our own gRPC server
  ServerBuilder lSrvBuilder;
  lSrvBuilder.AddListeningPort(pRpcSrvBindIp + ":0", grpc::InsecureServerCredentials(), &lRealPort);
  lSrvBuilder.RegisterService(this);
  assert(!mServer);
  mServer = lSrvBuilder.BuildAndStart();
  IDDLOG("gRPC server is started. server_ep={}:{}", pRpcSrvBindIp, lRealPort);
}

bool TfBuilderRpcImpl::start(const std::uint64_t pBufferSize)
{
  mCurrentTfBufferSize = pBufferSize;
  mTerminateRequested = false;

  // Interact with the scheduler
  if (!mTfSchedulerRpcClient.start(mDiscoveryConfig)) {
    return false;
  }

  // Start gRPC connections to all StfSenders
  if (!mStfSenderRpcClients.start() ) {
    return false;
  }

  mTfBuildRequests = std::make_unique<ConcurrentFifo<TfBuildingInformation>>();

  // start the update sending thread
  mRunning = true;
  mUpdateThread = create_thread_member("tfb_sched_upd", &TfBuilderRpcImpl::UpdateSendingThread, this);

  // start the stf requester thread
  mStfRequestThread = create_thread_member("tfb_sched_req", &TfBuilderRpcImpl::StfRequestThread, this);

  return true;
}

void TfBuilderRpcImpl::stop()
{
  mRunning = false;

  stopAcceptingTfs();

  if (mUpdateThread.joinable()) {
    mUpdateThread.join();
  }

  {
    if (mTfBuildRequests) {
      mTfBuildRequests->stop();
    }
    if (mStfRequestThread.joinable()) {
      mStfRequestThread.join();
    }
  }

  if (mServer) {
    mServer->Shutdown();
    mServer.reset(nullptr);
  }

  mStfSenderRpcClients.stop();
  mTfSchedulerRpcClient.stop();

  mCurrentTfBufferSize = 0;
  mNumBufferedTfs = 0;
  mLastBuiltTfId = 0;
}

// make sure these are sent immediately
void TfBuilderRpcImpl::startAcceptingTfs() {
  std::unique_lock lLock(mUpdateLock);
  mAcceptingTfs = true;
  sendTfBuilderUpdate();
}

void TfBuilderRpcImpl::stopAcceptingTfs() {
  std::unique_lock lLock(mUpdateLock);
  mAcceptingTfs = false;
  sendTfBuilderUpdate();
}

void TfBuilderRpcImpl::UpdateSendingThread()
{
  using namespace std::chrono_literals;
  DDDLOG("Starting TfBuilder Update sending thread.");

  while (mRunning) {
    if (!mTerminateRequested) {
      std::unique_lock lLock(mUpdateLock);
      sendTfBuilderUpdate();
      mUpdateCondition.wait_for(lLock, 500ms);
    } else {
      stopAcceptingTfs();
      std::this_thread::sleep_for(1s);
    }
  }

  // send disconnect update
  assert (!mRunning);

  std::unique_lock lLock(mUpdateLock);
  sendTfBuilderUpdate();

  DDDLOG("Exiting TfBuilder Update sending thread.");
}

void TfBuilderRpcImpl::StfRequestThread()
{
  using namespace std::chrono_literals;
  DDDLOG("Starting Stf requesting thread.");

  const auto &lTfBuilderId = mDiscoveryConfig->status().info().process_id();
  TfBuildingInformation mTfInfo;

  StfDataRequestMessage lStfRequest;
  StfDataResponse lStfResponse;
  lStfRequest.set_tf_builder_id(lTfBuilderId);
  std::uint64_t lNumTfRequests = 0;

  while (mRunning) {
    if (!mTfBuildRequests->pop(mTfInfo)) {
      continue; // mRunning will change to false
    }

    lNumTfRequests++;
    DDDLOG_RL(1000, "Requesting SubTimeFrame. stf_id={} tf_size={} total_requests={}",
      mTfInfo.tf_id(), mTfInfo.tf_size(), lNumTfRequests);

    for (auto &lStfDataIter : mTfInfo.stf_size_map()) {
      const auto &lStfSenderId = lStfDataIter.first;
      // const auto &lStfSize = lStfDataIter.second;
      lStfRequest.set_stf_id(mTfInfo.tf_id());

      grpc::Status lStatus = StfSenderRpcClients()[lStfSenderId]->StfDataRequest(lStfRequest, lStfResponse);
      if (!lStatus.ok()) {
        // gRPC problem... continue asking for other STFs
        EDDLOG("StfSender gRPC connection problem. stfs_id={} code={} error={}",
          lStfSenderId, lStatus.error_code(), lStatus.error_message());
        continue;
      }

      if (lStfResponse.status() != StfDataResponse::OK) {
        EDDLOG("StfSender did not sent data. stfs_id={} reason={}",
          lStfSenderId, StfDataResponse_StfDataStatus_Name(lStfResponse.status()));
        continue;
      }
    }
  }

  // send disconnect update
  assert (!mRunning);
  DDDLOG("Exiting Stf requesting thread.");
}

bool TfBuilderRpcImpl::sendTfBuilderUpdate()
{
  TfBuilderUpdateMessage lUpdate;
  const auto &lStatus = mDiscoveryConfig->status();
  static std::uint64_t sUpdateCnt = 0;

  *lUpdate.mutable_info() = lStatus.info();
  *lUpdate.mutable_partition() = lStatus.partition();

  lUpdate.mutable_info()->set_process_state(mAcceptingTfs ? BasicInfo::RUNNING : BasicInfo::NOT_RUNNING);

  {
    std::scoped_lock lLock(mTfIdSizesLock);

    lUpdate.set_free_memory(mCurrentTfBufferSize);
    lUpdate.set_num_buffered_tfs(mNumBufferedTfs);
    lUpdate.set_last_built_tf_id(mLastBuiltTfId);
  }

  sUpdateCnt++;
  DDDLOG_RL(5000, "Sending TfBuilder update. accepting={} total={}", mAcceptingTfs, sUpdateCnt);

  auto lRet = mTfSchedulerRpcClient.TfBuilderUpdate(lUpdate);
  if (!lRet) {
    EDDLOG_RL(1000, "Sending TfBuilder status update failed.");
  }
  return lRet;
}

bool TfBuilderRpcImpl::recordTfBuilt(const SubTimeFrame &pTf)
{
  if (!mRunning) {
    return false;
  }
  const auto lTfSize = pTf.getDataSize();

  {
    std::scoped_lock lLock(mTfIdSizesLock);

    if (mCurrentTfBufferSize >= lTfSize) {
      mCurrentTfBufferSize -= lTfSize;
    } else {
      EDDLOG("TimeFrame memory buffer overrun! size=0 missing={}", (lTfSize - mCurrentTfBufferSize));
      mCurrentTfBufferSize = 0;
    }

    assert (mTfIdSizes.count(pTf.header().mId) == 0);

    // save the size and id to increment the state later
    mTfIdSizes[pTf.header().mId] = lTfSize;

    mNumBufferedTfs++;

    mLastBuiltTfId = std::max(mLastBuiltTfId, pTf.header().mId);
  }
  mUpdateCondition.notify_one();

  return true;
}

bool TfBuilderRpcImpl::recordTfForwarded(const std::uint64_t &pTfId)
{
  if (!mRunning) {
    return false;
  }

  {
    std::scoped_lock lLock(mTfIdSizesLock);

    if (mTfIdSizes.count(pTfId) != 1) {
      EDDLOG("TimeFrame buffer size increase error: No such TimeFrame. tf_id=={}", pTfId);
      return false;
    }

    const auto lTfSize = mTfIdSizes[pTfId];
    mCurrentTfBufferSize += lTfSize;

    // remove the tf id from the map
    mTfIdSizes.erase(pTfId);

    mNumBufferedTfs--;
  }

  mUpdateCondition.notify_one();

  return true;
}

::grpc::Status TfBuilderRpcImpl::BuildTfRequest(::grpc::ServerContext* /*context*/,
                                                const TfBuildingInformation* request, BuildTfResponse* response)
{
  if (!mRunning || mTerminateRequested) {
    response->set_status(BuildTfResponse::ERROR_NOT_RUNNING);
    return ::grpc::Status::OK;
  }

  const auto &lTfId = request->tf_id();
  const auto &lTfSize = request->tf_size();

  // sanity checks for accepting new TFs
  {
    std::scoped_lock lLock(mTfIdSizesLock);

    if (lTfSize > mCurrentTfBufferSize) {
      EDDLOG("Request to build a TimeFrame: Not enough free memory! tf_id={} tf_size={} buffer_size={}",
        lTfId,lTfSize, mCurrentTfBufferSize);

      response->set_status(BuildTfResponse::ERROR_NOMEM);
      return ::grpc::Status::OK;
    }
  }

  // add request to the queue
  mTfBuildRequests->push(*request);

  return ::grpc::Status::OK;
}

::grpc::Status TfBuilderRpcImpl::TerminatePartition(::grpc::ServerContext* /*context*/,
  const ::o2::DataDistribution::PartitionInfo* /*request*/, ::o2::DataDistribution::PartitionResponse* response)
{
  IDDLOG("TerminatePartition request received.");
  mTerminateRequested = true;

  response->set_partition_state(PartitionState::PARTITION_TERMINATING);

  return ::grpc::Status::OK;
}

}
} /* o2::DataDistribution */
