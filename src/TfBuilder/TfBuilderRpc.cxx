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

#include <MemoryUtils.h>
#include <DataDistLogger.h>
#include <DataDistMonitoring.h>

#include <condition_variable>
#include <stdexcept>
#include <random>
#include <array>

namespace o2::DataDistribution
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
  mBufferSize = pBufferSize;
  mCurrentTfBufferSize = pBufferSize;

  // Interact with the scheduler
  if (!mTfSchedulerRpcClient.should_retry_start()) {
    WDDLOG("TfSchedulerRpc: Failed to connect to scheduler. Exiting.");
    mTerminateRequested = true;
    return false;
  }

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


bool TfBuilderRpcImpl::sendTfBuilderUpdate()
{
  TfBuilderUpdateMessage lUpdate;
  const auto &lStatus = mDiscoveryConfig->status();
  static std::uint64_t sUpdateCnt = 0;
  std::uint64_t lNumTfsInBuilding = 0;

  *lUpdate.mutable_info() = lStatus.info();
  *lUpdate.mutable_partition() = lStatus.partition();

  lUpdate.mutable_info()->set_process_state(mAcceptingTfs ? BasicInfo::RUNNING : BasicInfo::NOT_RUNNING);

  {
    std::scoped_lock lLock(mTfIdSizesLock);

    const std::uint64_t lFreeMem = std::min(std::size_t(mCurrentTfBufferSize), mMemI.freeData());

    lUpdate.set_free_memory(lFreeMem);
    lUpdate.set_num_buffered_tfs(mNumBufferedTfs);
    lUpdate.set_last_built_tf_id(mLastBuiltTfId);
    lUpdate.set_num_tfs_in_building(mNumTfsInBuilding);

    lNumTfsInBuilding = mNumTfsInBuilding;
  }

  sUpdateCnt++;
  DDDLOG_RL(5000, "Sending TfBuilder update. accepting={} total={}", mAcceptingTfs, sUpdateCnt);

  auto lRet = mTfSchedulerRpcClient.TfBuilderUpdate(lUpdate);
  if (!lRet) {
    EDDLOG_RL(1000, "Sending TfBuilder status update failed.");
  }

  DDMON("tfbuilder", "buffer.tf_cnt", mNumBufferedTfs);
  DDMON("tfbuilder", "buffer.tf_size", mBufferSize - mCurrentTfBufferSize);
  DDMON("tfbuilder", "merge.tfs_in_building", lNumTfsInBuilding);

  DDMON("tfbuilder", "buffer.dr_free", mMemI.freeData());
  DDMON("tfbuilder", "buffer.hr_free", mMemI.freeHeader());

  DDMON("tfbuilder", "buffer.dr_used", mMemI.sizeData() - mMemI.freeData());
  DDMON("tfbuilder", "buffer.hr_used", mMemI.sizeHeader() - mMemI.freeHeader());

  return lRet;
}

bool TfBuilderRpcImpl::recordTfBuilt(const SubTimeFrame &pTf)
{
  if (!mRunning) {
    return false;
  }
  const auto lTfSize = pTf.getDataSize();
  const auto lTfId = pTf.id();

  {
    std::scoped_lock lLock(mTfIdSizesLock);

    if (mCurrentTfBufferSize >= lTfSize) {
      mCurrentTfBufferSize -= lTfSize;
    } else {
      EDDLOG("TimeFrame memory buffer overrun! size=0 missing={}", (lTfSize - mCurrentTfBufferSize));
      mCurrentTfBufferSize = 0;
    }

    // Let the scheduler know that we can have more "in building" TFs
    mNumTfsInBuilding = std::max(0, mNumTfsInBuilding - 1);

    assert (mTfIdSizes.count(lTfId) == 0);

    // save the size and id to increment the state later
    mTfIdSizes[lTfId] = lTfSize;
    mNumBufferedTfs++;
    mLastBuiltTfId = std::max(mLastBuiltTfId, lTfId);

    DDMON("tfbuilder", "buffer.tf_cnt", mNumBufferedTfs);
    DDMON("tfbuilder", "buffer.tf_size", mBufferSize - mCurrentTfBufferSize);
    DDMON("tfbuilder", "buffer.dr_used", mMemI.sizeData() - mMemI.freeData());
  }
  mUpdateCondition.notify_one();

  { // record the current TP
    std::unique_lock lLock(mStfDurationMapLock);
    mStfReqDuration.erase(lTfId);
  }

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

    DDMON("tfbuilder", "buffer.tf_cnt", mNumBufferedTfs);
    DDMON("tfbuilder", "buffer.tf_size", mBufferSize - mCurrentTfBufferSize);
    DDMON("tfbuilder", "buffer.dr_used", mMemI.sizeData() - mMemI.freeData());
  }

  mUpdateCondition.notify_one();

  return true;
}

::grpc::Status TfBuilderRpcImpl::BuildTfRequest(::grpc::ServerContext* /*context*/,
  const TfBuildingInformation* request, BuildTfResponse* response)
{
  static std::atomic_uint64_t sNumTfRequests = 0;

  if (!mRunning || mTerminateRequested) {
    response->set_status(BuildTfResponse::ERROR_NOT_RUNNING);
    return ::grpc::Status::OK;
  }

  const auto &lTfId = request->tf_id();
  const auto &lTfSize = request->tf_size();
  const auto &lTfSource = request->tf_source();

  // sanity checks for accepting new TFs
  {
    std::scoped_lock lLock(mTfIdSizesLock);

    if (lTfSize > mCurrentTfBufferSize) {
      EDDLOG_GRL(1000, "Request to build a TimeFrame: Not enough free memory! tf_id={} tf_size={} buffer_size={} stf_type={}",
        lTfId,lTfSize, mCurrentTfBufferSize, StfSource_Name(lTfSource));

      response->set_status(BuildTfResponse::ERROR_NOMEM);
      return ::grpc::Status::OK;
    }

    // count how many TFs are in "building". Decremented in recordTfBuilt()
    mNumTfsInBuilding += 1;
  }

  sNumTfRequests++;
  DDDLOG_RL(5000, "Requesting SubTimeFrames. tf_id={} tf_size={} total_requests={}", lTfId, lTfSize, sNumTfRequests);

  StfDataRequestMessage lStfRequest;
  const auto &lTfBuilderId = mDiscoveryConfig->status().info().process_id();
  lStfRequest.set_tf_builder_id(lTfBuilderId);

  std::vector<StfRequests> lStfRequestVector;

  for (auto &lStfDataIter : request->stf_size_map()) {
    const auto &lStfSenderId = lStfDataIter.first;
    const auto &lStfSize = lStfDataIter.second;

    lStfRequest.set_stf_id(lTfId);

    lStfRequestVector.emplace_back(lStfSenderId, lStfSize, lStfRequest);
  }

  // sort to get largest STFs last
  std::sort(lStfRequestVector.begin(), lStfRequestVector.end(), [](const auto & a, const auto & b) -> bool {
    return a.mStfDataSize < b.mStfDataSize;
  });

  // add the vector to the stf request map
  {
    std::scoped_lock lLock(mStfReqMapLock);

    assert (mStfRequestMap.count(lTfId) == 0);

    mStfRequestMap.emplace(lTfId, std::move(lStfRequestVector));
  }

  mStfReqMapCV.notify_one();

  response->set_status(BuildTfResponse::OK);
  return ::grpc::Status::OK;
}

void TfBuilderRpcImpl::StfRequestThread()
{
  using namespace std::chrono_literals;
  using hres_clock = std::chrono::steady_clock;

  std::random_device lRd;
  std::mt19937 lGen(lRd());

  DDDLOG("Starting Stf requesting thread.");

  TfBuildingInformation lTfInfo;

  while (mRunning) {
    StfRequests lStfRequest;
    {
      std::unique_lock lLock(mStfReqMapLock);

      DDMON("tfbuilder", "merge.num_stf_in_flight", mNumReqInFlight);

      if (!mRunning || mStfRequestMap.empty() || mNumReqInFlight >= mMaxNumReqInFlight) {
        mStfReqMapCV.wait_for(lLock, 500ms);
        continue; // reevaluate the conditions
      }

      auto &lReqVector = mStfRequestMap.begin()->second;
      assert (!lReqVector.empty());

      // save request for outside the lock
      std::size_t lIdx = lReqVector.size() - 1;
      if (lIdx > 4) {
        std::array<double, 2> i{ 0, double(lReqVector.size()) };
        std::array<double, 2> w{ double(lReqVector.begin()->mStfDataSize + 1), double(lReqVector.rbegin()->mStfDataSize + 1) };
        std::piecewise_linear_distribution<> d(i.begin(), i.end(), w.begin());

        lIdx = std::min(std::size_t(std::floor(d(lGen))), lReqVector.size() - 1);
      }

      lStfRequest = std::move(lReqVector[lIdx]);
      lReqVector.erase(lReqVector.cbegin() + lIdx);

      mNumReqInFlight += 1;

      // are we done with the TF
      if (lReqVector.empty()) {
        mStfRequestMap.erase(mStfRequestMap.cbegin());
      }
    }

    const auto lTfId = lStfRequest.mRequest.stf_id();

    { // record the current TP
      std::unique_lock lLock(mStfDurationMapLock);
      mStfReqDuration[lTfId][lStfRequest.mStfSenderId] = hres_clock::now();
    }

    StfDataResponse lStfResponse;
    grpc::Status lStatus = StfSenderRpcClients()[lStfRequest.mStfSenderId]->StfDataRequest(lStfRequest.mRequest, lStfResponse);
    if (!lStatus.ok()) {
      // gRPC problem... continue asking for other STFs
      EDDLOG("StfSender gRPC connection problem. stfs_id={} code={} error={} stf_size={}",
        lStfRequest.mStfSenderId, lStatus.error_code(), lStatus.error_message(), lStfRequest.mStfDataSize);
      {
        std::unique_lock lLock(mStfReqMapLock);
        mNumReqInFlight -= 1;
      }
      continue;
    }

    if (lStfResponse.status() != StfDataResponse::OK) {
      EDDLOG("StfSender did not sent data. stfs_id={} reason={}",
        lStfRequest.mStfSenderId, StfDataResponse_StfDataStatus_Name(lStfResponse.status()));
      {
        std::unique_lock lLock(mStfReqMapLock);
        mNumReqInFlight -= 1;
      }
      continue;
    }
  }
  // send disconnect update
  assert (!mRunning);
  DDDLOG("Exiting Stf requesting thread.");
}

bool TfBuilderRpcImpl::recordStfReceived(const std::string &pStfSenderId, const std::uint64_t pTfId)
{
  using hres_clock = std::chrono::steady_clock;

  { // unblock the next request
    std::scoped_lock lLock(mStfReqMapLock);
    mNumReqInFlight -= 1;
  }
  mStfReqMapCV.notify_one();

  // record completion time
  double lStfFetchDurationMs = 0.0;
  {
    std::scoped_lock lLock(mStfDurationMapLock);
    const auto &lStfReqTp = mStfReqDuration[pTfId][pStfSenderId];

    const std::chrono::duration<double, std::milli> lDuration = hres_clock::now() - lStfReqTp;
    lStfFetchDurationMs = lDuration.count();
  }

  if (lStfFetchDurationMs > 0.0) {
    DDMON("tfbuilder", "merge.stf_fetch_ms", lStfFetchDurationMs);
  }

  return true;
}


::grpc::Status TfBuilderRpcImpl::TerminatePartition(::grpc::ServerContext* /*context*/,
  const ::o2::DataDistribution::PartitionInfo* /*request*/, ::o2::DataDistribution::PartitionResponse* response)
{
  IDDLOG("TerminatePartition request received.");
  mTerminateRequested = true;

  response->set_partition_state(PartitionState::PARTITION_TERMINATING);

  return ::grpc::Status::OK;
}

namespace bpo = boost::program_options;

bpo::options_description TfBuilderRpcImpl::getTfBuilderRpcProgramOptions() {
  bpo::options_description lStfBuildingOptions("Options controlling TimeFrame building", 120);

  lStfBuildingOptions.add_options() (
    OptionKeyMaxNumTransfers,
    bpo::value<std::int64_t>()->default_value(32),
    "Maximal number of simultaneous transfers.");

  return lStfBuildingOptions;
}

} /* o2::DataDistribution */
