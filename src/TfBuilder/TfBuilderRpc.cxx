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
#include "TfBuilderInputDefs.h"
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

bool TfBuilderRpcImpl::start(const std::uint64_t pBufferSize, std::shared_ptr<ConcurrentQueue<std::string> > pReqQueue,
  std::shared_ptr<ConcurrentQueue<ReceivedStfMeta> > pRecvQueue)
{
  mBufferSize = pBufferSize;
  mCurrentTfBufferSize = pBufferSize;
  mStfInputQueue = pReqQueue;
  mReceivedDataQueue = pRecvQueue;

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

  reset_run_counters();
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
  if (!lRet && mRunning) {
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
      EDDLOG_RL(5000, "TimeFrame memory buffer overrun! size=0 missing={}", (lTfSize - mCurrentTfBufferSize));
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

  { // record the current TF
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
  DDDLOG_GRL(5000, "Requesting SubTimeFrames. tf_id={} tf_size={} total_requests={}", lTfId, lTfSize, sNumTfRequests);

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

  // setup renaming of topological Stfs
  auto lTopoStfId = mTopoStfId;
  if (request->tf_source() == TOPOLOGICAL) {
    auto &lStfSenderId = request->stf_size_map().begin()->first;

    std::scoped_lock lLock(mTopoTfIdLock);

    assert (mTopoTfIdRenameMap[lStfSenderId].count(lTfId) == 0);

    mTopoTfIdRenameMap[lStfSenderId][lTfId] = lTopoStfId;

    // notify Input stage about new Stf (renamed)
    mReceivedDataQueue->push(ReceivedStfMeta(ReceivedStfMeta::MetaType::ADD, lTopoStfId));

    mTopoStfId += 1;
  } else {
    lTopoStfId = lTfId; // set to the actual tf id if not topo
    // notify Input stage about new Stf (regular)
    mReceivedDataQueue->push(ReceivedStfMeta(ReceivedStfMeta::MetaType::ADD, lTfId));
  }

  // add the vector to the stf request map
  mStfRequestQueue.push(lTfId, (request->tf_source() == TOPOLOGICAL), lTopoStfId, std::move(lStfRequestVector));

  response->set_status(BuildTfResponse::OK);
  return ::grpc::Status::OK;
}

void TfBuilderRpcImpl::StfRequestThread()
{
  using namespace std::chrono_literals;
  using clock = std::chrono::steady_clock;

  std::random_device lRd;
  std::mt19937_64 lGen(lRd());

  DDDLOG("Starting Stf requesting thread.");

  TfBuildingInformation lTfInfo;

  while (mRunning) {
    StfRequests lStfRequest;
    {
      std::optional<std::tuple<std::uint64_t, bool, std::uint64_t, std::vector<StfRequests>> > lReqOpt;
      if ((lReqOpt = mStfRequestQueue.pop_wait_for(100ms)) == std::nullopt) {
        continue;
      }

      assert (lReqOpt);
      const auto lTfId = std::get<0>(lReqOpt.value());
      const auto lIsTopo = std::get<1>(lReqOpt.value());
      const auto lTfRenamedId = std::get<2>(lReqOpt.value());
      auto &lReqVector = std::get<3>(lReqOpt.value());

      std::string lStfSenderIdTopo;
      if (lIsTopo) {
        assert (lReqVector.size() == 1);
        lStfSenderIdTopo = lReqVector.front().mStfSenderId;
      }

      mMaxNumReqInFlight = std::clamp(mDiscoveryConfig->getUInt64Param(MaxNumStfTransfersKey, MaxNumStfTransferDefault),
        std::uint64_t(8), std::uint64_t(500));

      std::uint64_t lNumExpectedStfs = lReqVector.size();
      setNumberOfStfs(lTfId, lNumExpectedStfs);

      while (mRunning && !lReqVector.empty()) {
        // wait for the stf slots to become free
        if (mNumReqInFlight.load() >= mMaxNumReqInFlight) {
          std::this_thread::sleep_for(500us);
          continue; // reevaluate the max TF conditions
        }

        // select the stfsender to contact first based on the stf size
        std::size_t lIdx = 0;

        const auto lIdx3_3 = lReqVector.size();
        const auto lIdx1_3 = lIdx3_3 / 3;
        const auto lIdx2_3 = 2 * lIdx1_3;

        if (lIdx > 8) {
          std::array<double, 4> i{
            double(0.0),
            double(lIdx1_3),
            double(lIdx2_3),
            double(lIdx3_3)        // return index from [ 0, lReqVector.size )
          };

          std::array<double, 4> w{
            double(lReqVector[0].mStfDataSize + 1.0),
            double(lReqVector[lIdx1_3].mStfDataSize + 1.0),
            double(lReqVector[lIdx2_3].mStfDataSize + 1.0),
            double(lReqVector[lIdx3_3].mStfDataSize + 1.0)
          };

          std::piecewise_linear_distribution<> lDist(i.begin(), i.end(), w.begin());

          lIdx = std::min(std::size_t(lDist(lGen)), lReqVector.size() - 1);
        } else {
          lIdx = lGen() % lReqVector.size();
        }

        DDMON("tfbuilder", "merge.request_idx", double(lIdx3_3) / double(lIdx + 1));

        lStfRequest = std::move(lReqVector[lIdx]);
        lReqVector.erase(lReqVector.cbegin() + lIdx);

        { // record the current TP
          std::unique_lock lLock(mStfDurationMapLock);
          mStfReqDuration[lTfId][lStfRequest.mStfSenderId] = clock::now();
        }

        auto lStartStfReqTime = clock::now();

        StfDataResponse lStfResponse;
        grpc::Status lStatus = StfSenderRpcClients()[lStfRequest.mStfSenderId]->StfDataRequest(lStfRequest.mRequest, lStfResponse);
        if (!lStatus.ok()) {
          lNumExpectedStfs -= 1;
          // gRPC problem... continue asking for other STFs
          EDDLOG("StfSender gRPC connection problem. stfs_id={} code={} error={} stf_size={}",
            lStfRequest.mStfSenderId, lStatus.error_code(), lStatus.error_message(), lStfRequest.mStfDataSize);
          continue;
        }

        if (lStfResponse.status() != StfDataResponse::OK) {
          lNumExpectedStfs -= 1;
          EDDLOG("StfSender did not sent data. stfs_id={} reason={}",
            lStfRequest.mStfSenderId, StfDataResponse_StfDataStatus_Name(lStfResponse.status()));
          continue;
        }

        DDMON("tfbuilder", "merge.stf_data_req_ms", since<std::chrono::milliseconds>(lStartStfReqTime));

        // Notify input about incoming STF
        mStfInputQueue->push(lStfRequest.mStfSenderId);

        mNumReqInFlight += 1;
        DDMON("tfbuilder", "merge.num_stf_in_flight", mNumReqInFlight);
      }

      // set the number of STFs for merging thread
      if (!lIsTopo) {
        setNumberOfStfs(lTfId, lNumExpectedStfs);
      } else {
        setNumberOfStfs(lTfRenamedId, lNumExpectedStfs);
      }

      // cleanup if we reached no StfSenders
      if (lNumExpectedStfs == 0) {
        if (lIsTopo) {
          // Topological: indicate that we're deleting topological (renamed) Id
          std::scoped_lock lLock(mTopoTfIdLock);
          assert (mTopoTfIdRenameMap[lStfSenderIdTopo].count(lTfId) == 1);
          assert (lTfRenamedId == mTopoTfIdRenameMap[lStfSenderIdTopo][lTfId]);

          mTopoTfIdRenameMap[lStfSenderIdTopo].erase(lTfId);

          // notify Input stage about new Stf (renamed)
          mReceivedDataQueue->push(ReceivedStfMeta(ReceivedStfMeta::MetaType::DELETE, lTfRenamedId));
        } else {
          // notify Input stage not to wait for STFs if we reached none of StfSender
          mReceivedDataQueue->push(ReceivedStfMeta(ReceivedStfMeta::MetaType::DELETE, lTfId));
        }
      }
    }
  }
  // send disconnect update
  assert (!mRunning);
  DDDLOG("Exiting Stf requesting thread.");
}

bool TfBuilderRpcImpl::recordStfReceived(const std::string &pStfSenderId, const std::uint64_t pTfId)
{
  using hres_clock = std::chrono::steady_clock;
  // unblock the next request
  mNumReqInFlight -= 1;

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


} /* o2::DataDistribution */
