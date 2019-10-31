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

#include <FairMQLogger.h>

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
  LOG(INFO) << "gRPC server listening on : " << pRpcSrvBindIp << ":" << lRealPort;
}


bool TfBuilderRpcImpl::start(const std::uint64_t pBufferSize)
{
  mCurrentTfBufferSize = pBufferSize;

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
  mUpdateThread = std::thread(&TfBuilderRpcImpl::UpdateSendingThread, this);

  // start the stf requester thread
  mStfRequestThread = std::thread(&TfBuilderRpcImpl::StfRequestThread, this);

  return true;
}

void TfBuilderRpcImpl::stop()
{
  stopAcceptingTfs();
  mRunning = false;

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
  LOG(DEBUG) << "Starting TfBuilder Update sending thread...";

  while (mRunning) {
    std::unique_lock lLock(mUpdateLock);
    sendTfBuilderUpdate();
    mUpdateCondition.wait_for(lLock, 500ms);
  }

  // send disconnect update
  assert (!mRunning);

  std::unique_lock lLock(mUpdateLock);
  sendTfBuilderUpdate();

  LOG(DEBUG) << "Exiting TfBuilder Update sending thread...";
}

void TfBuilderRpcImpl::StfRequestThread()
{
  using namespace std::chrono_literals;
  LOG(DEBUG) << "Starting Stf requesting thread...";

  const auto &lTfBuilderId = mDiscoveryConfig->status().info().process_id();
  TfBuildingInformation mTfInfo;

  StfDataRequestMessage lStfRequest;
  StfDataResponse lStfResponse;
  lStfRequest.set_tf_builder_id(lTfBuilderId);

  while (mRunning) {
    if (!mTfBuildRequests->pop(mTfInfo)) {
      continue; // mRunning will change to false
    }

    {
      static std::uint64_t sNumTfRequests = 0;
      if (++sNumTfRequests % 50 == 0) {
        LOG (INFO) << "Requesting SubTimeFrames with id: " << mTfInfo.tf_id()
                   << ", total size: " << mTfInfo.tf_size() << ". Total requests: " << sNumTfRequests;
      }
    }

    for (auto &lStfDataIter : mTfInfo.stf_size_map()) {
      const auto &lStfSenderId = lStfDataIter.first;
      // const auto &lStfSize = lStfDataIter.second;
      lStfRequest.set_stf_id(mTfInfo.tf_id());

      grpc::Status lStatus = StfSenderRpcClients()[lStfSenderId]->StfDataRequest(lStfRequest, lStfResponse);
      if (!lStatus.ok()) {
        // gRPC problem... continue asking for other STFs
        LOG (WARNING) << "StfSender gRPC connection problem. Code: " <<lStatus.error_code()
                      << ", message: " << lStatus.error_message();
        continue;
      }

      if (lStfResponse.status() != StfDataResponse::OK) {
        LOG (WARNING) << "StfSender " << lStfSenderId
                      << " cannot send data. Reason: " << StfDataResponse_StfDataStatus_Name(lStfResponse.status());
        continue;
      }
    }
  }

  // send disconnect update
  assert (!mRunning);

  LOG(DEBUG) << "Exiting Stf requesting thread...";
}

bool TfBuilderRpcImpl::sendTfBuilderUpdate()
{
  TfBuilderUpdateMessage lUpdate;
  const auto &lStatus = mDiscoveryConfig->status();

  *lUpdate.mutable_info() = lStatus.info();
  *lUpdate.mutable_partition() = lStatus.partition();

  if (mAcceptingTfs) {
    lUpdate.set_state(TfBuilderUpdateMessage::RUNNING);

    std::scoped_lock lLock(mTfIdSizesLock);

    lUpdate.set_last_built_tf_id(mLastBuiltTfId);
    lUpdate.set_free_memory(mCurrentTfBufferSize);
    lUpdate.set_num_buffered_tfs(mNumBufferedTfs);
  } else {
    lUpdate.set_state(TfBuilderUpdateMessage::NOT_RUNNING);

    std::scoped_lock lLock(mTfIdSizesLock);
    lUpdate.set_free_memory(mCurrentTfBufferSize);
  }

  {
    static thread_local std::uint64_t sUpdateCnt = 0;
    if (++sUpdateCnt % 10 == 0) {
      LOG(DEBUG) << "Sending TfBuilder update, accepting: " << mAcceptingTfs << ", total: " << sUpdateCnt;
    }
  }

  auto lRet = mTfSchedulerRpcClient.TfBuilderUpdate(lUpdate);
  if (!lRet) {
    LOG(WARN) << "Sending TfBuilder update failed";
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
      LOG(ERROR) << "TimeFrame memory buffer overrun! Current size: 0, missing: " << lTfSize - mCurrentTfBufferSize;
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
      LOG(ERROR) << "TimeFrame buffer size increase error: No TimeFrame with ID " << pTfId;
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

::grpc::Status TfBuilderRpcImpl::BuildTfRequest(::grpc::ServerContext* /*context*/, const TfBuildingInformation* request, BuildTfResponse* response)
{
  if(!mRunning) {
    response->set_status(BuildTfResponse::ERROR_NOT_RUNNING);
    return ::grpc::Status::OK;
  }

  const auto &lTfId = request->tf_id();
  const auto &lTfSize = request->tf_size();

  // sanity checks for accepting new TFs
  {
    std::scoped_lock lLock(mTfIdSizesLock);

    if (lTfSize > mCurrentTfBufferSize) {
      LOG(ERROR) << "Request to build TF: " << lTfId
                 << ", size: " << lTfSize
                 << ". Available memory: " << mCurrentTfBufferSize;

      response->set_status(BuildTfResponse::ERROR_NOMEM);
      return ::grpc::Status::OK;
    }
  }

  // add request to the queue
  mTfBuildRequests->push(*request);

  return ::grpc::Status::OK;
}

}
} /* o2::DataDistribution */
