// Copyright 2019-2022 CERN and copyright holders of ALICE O2.
// See https://alice-o2.web.cern.ch/copyright for details of the copyright holders.
// All rights not expressly granted are reserved.
//
// This software is distributed under the terms of the GNU General Public
// License v3 (GPL Version 3), copied verbatim in the file "COPYING".
//
// In applying this license CERN does not waive the privileges and immunities
// granted to it by virtue of its status as an Intergovernmental Organization
// or submit itself to any jurisdiction.

/// \author Gvozden Nešković, Frankfurt Institute for Advanced Studies and Goethe University Frankfurt

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
    IDDLOG_RL(10000, "TfSchedulerRpc: Failed to connect to scheduler. Exiting.");
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
  auto lGrpcPoolSize = mDiscoveryConfig->getUInt64Param(StfSenderGrpcThreadPoolSizeKey, StfSenderGrpcThreadPoolSizeDefault);
  lGrpcPoolSize = std::clamp(lGrpcPoolSize, std::uint64_t(1), std::uint64_t(std::thread::hardware_concurrency()));
  for (unsigned i = 0; i < lGrpcPoolSize; i++) {
    std::string lThreadName = "tfb_grpc_req_" + std::to_string(i);
    mStfRequestThreadPool.emplace_back(std::move(create_thread_member(lThreadName.c_str(), &TfBuilderRpcImpl::StfRequestGrpcThread, this)));
  }

  mStfRequestThread = create_thread_member("tfb_sched_req", &TfBuilderRpcImpl::StfRequestThread, this);

  // Periodically poll on new consul parameters
  UpdateConsulParams();

  return true;
}

void TfBuilderRpcImpl::stop()
{
  mRunning = false;

  stopAcceptingTfs();

  if (mUpdateThread.joinable()) {
    mUpdateThread.join();
  }

  { // Stop STF request threads
    if (mTfBuildRequests) {
      mTfBuildRequests->stop();
    }
    if (mStfRequestThread.joinable()) {
      mStfRequestThread.join();
    }

    mGrpcStfRequestQueue.stop();
    for (auto &lThread : mStfRequestThreadPool) {
      if (lThread.joinable()) {
        lThread.join();
      }
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

void TfBuilderRpcImpl::UpdateConsulParams()
{
  using namespace std::chrono_literals;
  std::thread([this]()
  {
    while (mRunning) {
      {
        auto lNewMethod = eRandom;

        auto lMethodStr = mDiscoveryConfig->getStringParam(StfSenderIdxSelectionMethodKey, StfSenderIdxSelectionMethodDefault);
        boost::trim(lMethodStr);

        if (boost::iequals(lMethodStr, "random")) {
          lNewMethod = eRandom;
        } else if (boost::iequals(lMethodStr, "linear")) {
          lNewMethod = eLinear;
        } else if (boost::iequals(lMethodStr, "stfsize")) {
          lNewMethod = eStfSize;
        } else {
          WDDLOG_RL(60000, "StfSenderIdxSelMethod option in consul is invalid. val={} allowed=[linear|random|stfsize]", lMethodStr);
        }

        if (lNewMethod != mStfSenderIdxSelMethod) {
          IDDLOG("StfSenderIdxSelMethod changed. new={} old={}", sStfRequestIdxSelNames[lNewMethod], sStfRequestIdxSelNames[mStfSenderIdxSelMethod]);
          mStfSenderIdxSelMethod = lNewMethod;
        }

        DDDLOG_ONCE("StfSenderIdxSelMethod method={}", sStfRequestIdxSelNames[mStfSenderIdxSelMethod]);
      }

      {
        auto lNewMaxNumReqInFlight = std::clamp(mDiscoveryConfig->getUInt64Param(MaxNumStfTransfersKey, MaxNumStfTransferDefault),
          std::uint64_t(2), std::uint64_t(5000));

        std::unique_lock lLock(mNumInFlightLock);
        if (lNewMaxNumReqInFlight != mMaxNumReqInFlight) {
          IDDLOG("MaxNumStfTransfers changed. new={} old={}", lNewMaxNumReqInFlight, mMaxNumReqInFlight);
          mMaxNumReqInFlight = lNewMaxNumReqInFlight;
          mNumInFlightCond.notify_all();
        }
        DDDLOG_ONCE("MaxNumStfTransfers value={}", mMaxNumReqInFlight);
      }

      {
        auto lNewGrpcReqTimeoutMs = std::chrono::milliseconds(mDiscoveryConfig->getUInt64Param(StfDataRequestGrpcTimeoutMsKey,
          StfDataRequestGrpcTimeoutMsDefault));

        if (lNewGrpcReqTimeoutMs != mRequestTimeoutMs.load()) {
          IDDLOG("StfDataRequestGrpcTimeoutMs changed. new={} old={}", lNewGrpcReqTimeoutMs.count(), mRequestTimeoutMs.load().count());
          mRequestTimeoutMs = lNewGrpcReqTimeoutMs;
        }
        DDDLOG_ONCE("StfDataRequestGrpcTimeoutMs value={}", mRequestTimeoutMs.load().count());
      }

      {
        static bool sMonitorRpc = DataDistMonitorRpcDurationDefault;
        auto lNewMonitorRpc = mDiscoveryConfig->getBoolParam(DataDistMonitorRpcDurationKey, DataDistMonitorRpcDurationDefault);

        if (lNewMonitorRpc != sMonitorRpc) {
          IDDLOG("DataDistMonitorRpcDuration changed. new={} old={}", lNewMonitorRpc, sMonitorRpc);
          sMonitorRpc = lNewMonitorRpc;
          mStfSenderRpcClients.setMonitorDuration(lNewMonitorRpc);
        }
        DDDLOG_ONCE("DataDistMonitorRpcDuration value={}", sMonitorRpc);
      }

      std::this_thread::sleep_for(1s);
    }
  }).detach();
}

// make sure these are sent immediately
void TfBuilderRpcImpl::startAcceptingTfs() {
  std::unique_lock lLock(mUpdateLock);

  if (getNumFailedRpcConnections() == 0) {
    mAcceptingTfs = true;
    sendTfBuilderUpdate();
  } else {
    EDDLOG_RL(10000, "TfBuilderRpc::Not enabling TfBuilder because some gRPC connections are not working. failed_cnt={}",
      getNumFailedRpcConnections());
    throw std::runtime_error("TfBuilder failed to connect to FLPs, exiting");
  }
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
  static int errCount = 0;
  if (!lRet && !mTerminateRequested && mRunning) {
    WDDLOG_RL(10000, "Sending TfBuilder status update failed.");
    if (errCount++ > 5) {
      throw std::runtime_error("TfBuilder had 5 consecutive failed status updates, exiting");
    }
  } else {
    errCount = 0;
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

  assert (lTfId != 0);

  sNumTfRequests++;
  DDDLOG_GRL(5000, "Requesting SubTimeFrames. tf_id={} tf_size={} total_requests={}", lTfId, lTfSize, sNumTfRequests);

  StfDataRequestMessage lStfRequest;
  const auto &lTfBuilderId = mDiscoveryConfig->status().info().process_id();
  lStfRequest.set_tf_builder_id(lTfBuilderId);

  std::vector<StfRequest> lStfRequestVector;

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

std::size_t TfBuilderRpcImpl::getFetchIdxStfDataSize(const std::vector<StfRequest> &pReqVector) const
{
  static thread_local std::random_device lRd;
  static thread_local std::mt19937_64 lGen(lRd());

  // select the stfsender to contact first based on the stf size
  std::size_t lIdx = 0;

  const auto lIdx3_3 = pReqVector.size();
  const auto lIdx1_3 = lIdx3_3 / 3;
  const auto lIdx2_3 = 2 * lIdx1_3;

  if (lIdx > 8) {
    std::array<double, 4> i{
      double(0.0),
      double(lIdx1_3),
      double(lIdx2_3),
      double(lIdx3_3)        // return index from [ 0, pReqVector.size )
    };

    std::array<double, 4> w{
      double(pReqVector[0].mStfDataSize + 1.0),
      double(pReqVector[lIdx1_3].mStfDataSize + 1.0),
      double(pReqVector[lIdx2_3].mStfDataSize + 1.0),
      double(pReqVector[lIdx3_3].mStfDataSize + 1.0)
    };

    std::piecewise_linear_distribution<> lDist(i.begin(), i.end(), w.begin());

    lIdx = std::min(std::size_t(lDist(lGen)), pReqVector.size() - 1);
  } else {
    lIdx = lGen() % pReqVector.size();
  }

  return lIdx;
}

std::size_t TfBuilderRpcImpl::getFetchIdxRandom(const std::vector<StfRequest> &pReqVector) const
{
  static thread_local std::random_device lRd;
  static thread_local std::mt19937_64 lGen(lRd());

  assert (pReqVector.size() > 0);
  if (pReqVector.size() <= 2) {
    return 0;
  }

  std::uniform_int_distribution<> lDist(0, (pReqVector.size() - 1));
  return lDist(lGen);
}

void TfBuilderRpcImpl::StfRequestThread()
{
  using namespace std::chrono_literals;
  using clock = std::chrono::steady_clock;

  DDDLOG("Starting Stf requesting thread.");

  TfBuildingInformation lTfInfo;

  while (mRunning) {
    StfRequest lStfRequest;

    // <tf id, is topo, renamed topo id, requests>
    std::optional<std::tuple<std::uint64_t, bool, std::uint64_t, std::vector<StfRequest>> > lReqOpt;
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

    const auto lNumStfs = lReqVector.size();
    std::atomic_size_t lNumExpectedStfs = 0;
    std::size_t lNumRequested = 0;

    // set initial guess of how many stfs to expect, refine later if we fail to contact all FLPs
    setNumberOfStfs(lTfId, lNumStfs);

    const auto lTimeStfReqStart = clock::now();

    while (mRunning && !lReqVector.empty()) {

      { // wait for the stf slots to become free
        std::unique_lock lInFlightLock(mNumInFlightLock);
        while (mRunning && (mNumReqInFlight >= mMaxNumReqInFlight)) {
          mNumInFlightCond.wait_for(lInFlightLock, 100ms);
        }
        if (!mRunning) {
          continue;
        }

        mNumReqInFlight += 1;
        DDMON("tfbuilder", "merge.num_stf_in_flight", mNumReqInFlight);
      }

      // select the order in which StfSenders are contacted (consul option)
      std::size_t lIdx = getFetchIdx(lReqVector);

      lStfRequest = std::move(lReqVector[lIdx]);
      lReqVector.erase(lReqVector.cbegin() + lIdx);

      // Fan out STF grpc requests because it's limiting on how fast one EPN can receive data from many FLPs
      // A single StfDataRequest request takes about 330 us, i.e. fetching 200 STFs -> 70 ms.
      // This is OK wih runs with many EPNs, but a single EPN should be able to request all STFs under 10 ms
      mGrpcStfRequestQueue.push(GrpcStfReqInfo {std::move(lStfRequest), lNumStfs, lNumRequested /*ref*/, lNumExpectedStfs });
    }

    // wait for all STF to be requested
    while (true) {
      std::unique_lock lLock(mNumStfsRequestedLock);
      mNumStfsRequestedCv.wait_for(lLock, 5ms);
      if (lNumStfs == lNumRequested) {
        break;
      }
    }

    DDMON("tfbuilder", "merge.stf_data_req_total_ms", since<std::chrono::milliseconds>(lTimeStfReqStart));

    if (lNumExpectedStfs > 0) {
      // set the number of STFs for merging thread
      if (!lIsTopo) {
        setNumberOfStfs(lTfId, lNumExpectedStfs);
      } else {
        setNumberOfStfs(lTfRenamedId, lNumExpectedStfs);
      }
    } else {
      // cleanup if we reached no StfSenders
      { // Let the scheduler know that we are not building the current TF
        std::scoped_lock lLock(mTfIdSizesLock);
        mNumTfsInBuilding = std::max(0, mNumTfsInBuilding - 1);
        mLastBuiltTfId = std::max(mLastBuiltTfId, lTfId);
      }

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
      mUpdateCondition.notify_one();
    }
  }
  // send disconnect update
  assert (!mRunning);
  DDDLOG("Exiting Stf requesting thread.");
}

// Fanout threads for running grpc StfDataRequest() over all FLPs
void TfBuilderRpcImpl::StfRequestGrpcThread()
{
  while (mRunning) {
    auto lGrpcReqOpt = mGrpcStfRequestQueue.pop();
    if (!lGrpcReqOpt) {
      continue;
    }

    auto &lGrpcReq = lGrpcReqOpt.value();
    auto &lStfRequest = lGrpcReq.mStfRequest;

    bool lReqFailed = true;
    StfDataResponse lStfResponse;

    grpc::Status lStatus;
    do {
      lStatus = StfSenderRpcClients().StfDataRequestWithTimeout(lStfRequest.mStfSenderId,
        mRequestTimeoutMs.load(), lStfRequest.mRequest, lStfResponse);
      if (!lStatus.ok() && (lStatus.error_code() == grpc::StatusCode::DEADLINE_EXCEEDED)) {
        DDDLOG_RL(1000, "StfDataRequest timeout. stf_id={} stfs_id={} timeout_ms={}", lStfRequest.mRequest.stf_id(),
          lStfRequest.mStfSenderId, mRequestTimeoutMs.load().count());
      }
    } while (!lStatus.ok());

    if (!lStatus.ok()) {
      EDDLOG("StfSender gRPC connection problem. stfs_id={} code={} error={} stf_size={} timeout={}",
        lStfRequest.mRequest.stf_id(), lStfRequest.mStfSenderId, lStatus.error_code(), lStatus.error_message(), lStfRequest.mStfDataSize, (lStatus.error_code() == grpc::StatusCode::DEADLINE_EXCEEDED));
    } else if (lStfResponse.status() != StfDataResponse::OK) {
      EDDLOG("StfSender did not send data. stf_id={} stfs_id={} reason={}",
        lStfRequest.mStfSenderId, StfDataResponse_StfDataStatus_Name(lStfResponse.status()));
    } else {
      // Update the expected STF count
      lGrpcReq.mNumExpectedStfs += 1;
      lReqFailed = false;
    }

    if (lReqFailed) {
      std::unique_lock lInFlightLock(mNumInFlightLock);
      mNumReqInFlight -= 1;
    } else { // record time of the request to measure total time until STFs is fetched
      std::scoped_lock lLock(mStfDurationMapLock);
      mStfReqDuration[lStfRequest.mRequest.stf_id()][lStfRequest.mStfSenderId] = std::chrono::steady_clock::now();
    }

    { // notify the main request loop
      std::unique_lock lLock(mNumStfsRequestedLock);
      lGrpcReq.mNumRequested += 1;
      if (lGrpcReq.mNumStfSenders == lGrpcReq.mNumRequested) {
        mNumStfsRequestedCv.notify_one();
      }
    }

    if (!lReqFailed) {
      // Notify input stage about incoming STF
      mStfInputQueue->push(lStfRequest.mStfSenderId);
    }
  }
  DDDLOG("Exiting Stf grpc request thread.");
}

bool TfBuilderRpcImpl::recordStfReceived(const std::string &pStfSenderId, const std::uint64_t pTfId)
{
  { // unblock the next request (protect from async counter resets)
    std::unique_lock lInFlightLock(mNumInFlightLock);
    if (mNumReqInFlight > 0) {
      mNumReqInFlight -= 1;
    }
    mNumInFlightCond.notify_one();
    DDMON("tfbuilder", "merge.num_stf_in_flight", mNumReqInFlight);
  }

  // record completion time
  double lStfFetchDurationMs = 0.0;
  {
    std::scoped_lock lLock(mStfDurationMapLock);
    const auto &lStfReqTp = mStfReqDuration[pTfId][pStfSenderId];
    lStfFetchDurationMs = since<std::chrono::milliseconds>(lStfReqTp);
  }
  DDMON("tfbuilder", "merge.stf_fetch_ms", lStfFetchDurationMs);

  return true;
}


::grpc::Status TfBuilderRpcImpl::TerminatePartition(::grpc::ServerContext* /*context*/,
  const ::o2::DataDistribution::PartitionInfo* request, ::o2::DataDistribution::PartitionResponse* response)
{
  const std::string &lPartitionId = mDiscoveryConfig->status().partition().partition_id();

  if (request->partition_id() != lPartitionId) {
    response->set_partition_state(PartitionState::PARTITION_UNKNOWN);
    return Status::CANCELLED;
  }

  mTerminateRequested = true;

  IDDLOG("TerminatePartition request received. partition_id={}", request->partition_id());

  response->set_partition_state(PartitionState::PARTITION_TERMINATING);
  return Status::OK;
}


} /* o2::DataDistribution */
