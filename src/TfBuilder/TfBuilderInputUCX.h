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

#ifndef TF_BUILDER_INPUT_UCX_H_
#define TF_BUILDER_INPUT_UCX_H_

#include "TfBuilderInputDefs.h"

#include <ConfigConsul.h>
#include <discovery.pb.h>

#include <SubTimeFrameDataModel.h>
#include <ConcurrentQueue.h>
#include <DataDistributionOptions.h>

#include <UCXUtilities.h>
#include <UCXSendRecv.h>
#include <ucp/api/ucp.h>

#include <vector>
#include <map>
#include <boost/container/small_vector.hpp>
#include <boost/lockfree/stack.hpp>

#include <thread>
#include <shared_mutex>
#include <condition_variable>

namespace o2::DataDistribution
{
class TfBuilderInputUCX;
class TimeFrameBuilder;
class TfBuilderRpcImpl;

namespace bc = boost::container;

struct dd_ucp_listener_context_t {
  /// Self reference to handle new connections
  TfBuilderInputUCX *mInputUcx;
};

struct TfBuilderUCXConnInfo {
  /// Self reference to perform cleanup on errors
  TfBuilderInputUCX &mInputUCX;

  /// UCP worker and ep
  ucx::dd_ucp_worker  &mWorker;
  ucp_ep_h ucp_ep;

  /// Peer StfSender id
  std::string mPeerId;
  std::string mPeerIp;

  /// cache of unpacked remote rma keys
  std::shared_mutex mRemoteKeysLock;
    std::map<std::string, ucp_rkey_h> mRemoteKeys;

  /// Signal that peer connection has problems
  std::atomic_bool mConnError = false;

  TfBuilderUCXConnInfo() = delete;
  TfBuilderUCXConnInfo(TfBuilderInputUCX &pThis, ucx::dd_ucp_worker &pWorker)
  : mInputUCX(pThis), mWorker(pWorker) { }
};

class TfBuilderInputUCX
{
public:
  TfBuilderInputUCX() = delete;
  TfBuilderInputUCX(std::shared_ptr<ConsulTfBuilder> pConfig, std::shared_ptr<TfBuilderRpcImpl> pRpc, TimeFrameBuilder &pTfBldr,
                    ConcurrentQueue<std::string> &pStfReqQueue, ConcurrentQueue<ReceivedStfMeta> &pStfMetaQueue)
    : mConfig(pConfig),
      mRpc(pRpc),
      mTimeFrameBuilder(pTfBldr),
      mStfReqQueue(pStfReqQueue),
      mReceivedDataQueue(pStfMetaQueue)
  { }

  bool start();
  bool map_data_region();

  void stop();
  void reset() { }

  void StfPreprocessThread(const unsigned pThreadIdx);
  void TokenRequesterThread(const unsigned pThreadIdx);
  void DataHandlerThread(const unsigned pThreadIdx);
  void StfPostprocessThread(const unsigned pThreadIdx);

  struct StfMetaRdmaInfo {
    UCXIovStfHeader mStfMeta;
    std::vector<void*> mTxgPtrs;
    ucx::io::TokenRequest::BitFieldIdxType mStfSenderIdx = ucx::io::TokenRequest::BitfieldInvalidIdx;
    double mRdmaTimeMs;
  };

  bool createMetadata(const void *pPtr, const std::size_t pLen, UCXIovStfHeader &lMeta /* out */) {
    if (!lMeta.ParseFromArray(pPtr, pLen)) {
      EDDLOG("UCXDataHandlerThread: Failed to parse stf meta message. meta_size={}", pLen);
      return false;
    }
    return true;
  }

  void pushMetadata(UCXIovStfHeader &&pMeta) {
    mStfPreprocessQueue.push(std::move(pMeta));
  }

  void pushRdmaInfo(std::unique_ptr<StfMetaRdmaInfo> &&pInfo) {
    const auto &lStfSenderId = pInfo->mStfMeta.stf_sender_id();

    std::shared_lock lLock(mStfMetaWorkerQueuesMutex);

    assert (mStfMetaWorkerQueues[lStfSenderId] != nullptr);
    mStfMetaWorkerQueues[lStfSenderId]->push(std::move(pInfo));
  }

  void pushPostprocessMetadata(std::unique_ptr<StfMetaRdmaInfo> &&pInfo) {
    mStfPostprocessQueue.push(std::move(pInfo));
  }

  void handle_client_ep_error(TfBuilderUCXConnInfo *pConn, ucs_status_t pStatus) {

    if (pConn) {
      pConn->mConnError = true;

      IDDLOG_GRL(5000, "TfBuilderInputUCX: peer connection error. stfsender_ip={} stfsender_id={} err={}",
        pConn->mPeerIp, pConn->mPeerId, ucs_status_string(pStatus));
    }

    // TODO: survive loss of StfSender peers?
    mConnectionIssues = true;
  }

private:
  std::atomic<InputRunState> mState = CONFIGURING;
  std::atomic_bool mStopRequested = false;

  /// Consul discovery handle
  std::shared_ptr<ConsulTfBuilder> mConfig;

  /// RPC service
  std::shared_ptr<TfBuilderRpcImpl> mRpc;

  /// TimeFrameBuilder
  TimeFrameBuilder &mTimeFrameBuilder;

  // STF request queue
  ConcurrentQueue<std::string> &mStfReqQueue;
  std::size_t mThreadPoolSize;

  // STF preprocess threads
  ConcurrentQueue<UCXIovStfHeader> mStfPreprocessQueue;
  std::vector<std::thread> mPrepThreadPool;

  // STF RDMA get threads
  std::shared_mutex mStfMetaWorkerQueuesMutex;
    std::unordered_map<std::string, std::shared_ptr<ConcurrentQueue<std::unique_ptr<StfMetaRdmaInfo>> > >  mStfMetaWorkerQueues;
  std::vector<std::thread> mThreadPool;
  bool mRdmaPollingWait = UcxPollForRDMACompletionDefault;
  std::uint64_t mRdmaConcurrentStfSizeMax  = UcxMaxStfSizeForConcurrentFetchBDefault;

  // STF postprocessing threads
  ConcurrentQueue<std::unique_ptr<StfMetaRdmaInfo>> mStfPostprocessQueue;
  std::vector<std::thread> mPostThreadPool;

  /// Queue for received STFs
  ConcurrentQueue<ReceivedStfMeta> &mReceivedDataQueue;

  /// UCX context
  ucp_context_h ucp_context;
  boost::container::small_vector<ucx::dd_ucp_worker, 512> mDataWorkers;
  boost::container::small_vector<std::shared_ptr<ConcurrentQueue<std::unique_ptr<StfMetaRdmaInfo>>>, 512> mDataWorkersQueues;
  bool ucp_data_region_set = false;
  ucp_mem_h ucp_data_region;

  /// listener
  ucx::dd_ucp_worker listener_worker;
  ucp_listener_h ucp_listener;
  dd_ucp_listener_context_t dd_ucp_listen_context;
  std::atomic_bool mConnectionIssues = false;

  void ListenerThread();
  std::set<std::string> mExpectedStfSenderIds;
  std::thread mListenerThread;
  std::uint32_t mNumStfSenders;
  ConcurrentQueue<std::tuple<std::string, unsigned, ucp_conn_request_h> > mConnRequestQueue;
  std::shared_mutex mConnectionMapLock;
    std::map<std::string, std::unique_ptr<TfBuilderUCXConnInfo>> mConnMap;

  /// TfScheduler stf transfer token stuff
  bool mStfTokensEnabled = DataDistEnableStfTransferTokensDefault;

  TfBuilderUCXConnInfo *mTokenWorker = nullptr;
  std::vector<std::thread> mTokenThreadPool;

  std::unordered_map<std::string, ucx::io::TokenRequest> mStfBitFields;
  std::unordered_map<std::string, std::uint32_t> mStfIdToIdx;

  alignas(256)
  std::mutex mStfTokenWaitingMutex;
    std::condition_variable mStfWaitingCv;
    std::size_t mTokenRefCnt = 0;                 // indicate how many STFs will be requested while holding the token

    // Token book-keeping
    // thread -> stfs idx -> stfs
    struct StfWaitingInfo {
      std::size_t mNumStfsWaiting = 0;
      bc::small_vector<bc::small_vector<std::unique_ptr<StfMetaRdmaInfo>, 16>, 256> mStfs;
    };
    bc::small_vector<StfWaitingInfo, 16> mAvailableStfs; // keep the token book-keeping for each input thread


public:
  // UCX callbacks
  void new_conn_handle(ucp_conn_request_h conn_request);


  boost::lockfree::stack<std::uint32_t, boost::lockfree::capacity<1> > mReceivedToken;
};


} /* namespace o2::DataDistribution */

#endif /* TF_BUILDER_INPUT_UCX_H_ */
