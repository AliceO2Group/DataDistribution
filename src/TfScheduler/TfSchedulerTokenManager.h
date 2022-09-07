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

#ifndef TF_SCHEDULER_TOKEN_MANAGER_H_
#define TF_SCHEDULER_TOKEN_MANAGER_H_

#include <ConfigParameters.h>
#include <ConfigConsul.h>
#include <DataDistributionOptions.h>

#include <Utilities.h>

#include <UCXUtilities.h>
#include <UCXSendRecv.h>
#include <ucp/api/ucp.h>

#include <vector>
#include <map>
#include <thread>
#include <shared_mutex>

#include <boost/lockfree/stack.hpp>
#include <boost/lockfree/queue.hpp>

namespace o2::DataDistribution
{

class ConsulConfig;
class TfSchedulerTokenManager;

struct TokenRequestInfo {
  ucx::io::TokenRequest mRequest;
  ucp_ep_h              mReplyEp;
};

struct TfBuilderUCXConnInfo {
  TfSchedulerTokenManager &mTokenManager;
  ucx::dd_ucp_worker &mWorker;

  // peer name
  std::string mTfBuilderId;
  // peer lock (thread pool)
  std::mutex mTfBuilderLock;

  ucp_ep_h ucp_ep;

  std::atomic_bool mConnError = false;

  TfBuilderUCXConnInfo() = delete;
  TfBuilderUCXConnInfo(TfSchedulerTokenManager &pOutputUCX, ucx::dd_ucp_worker &pWorker, const std::string &pTfBuilderId)
  : mTokenManager(pOutputUCX),
    mWorker(pWorker),
    mTfBuilderId(pTfBuilderId)
  { }
};

class TfSchedulerTokenManager
{
 public:
  TfSchedulerTokenManager() = delete;
  TfSchedulerTokenManager(std::shared_ptr<ConsulTfScheduler> pConfig);

  ~TfSchedulerTokenManager() { }

  bool start();
  void stop();

  bool connectTfBuilder(const std::string &pTfBuilderId, const std::string &lTfBuilderIp, const unsigned lTfBuilderPort);

  void TokenManagerThread();
  void TokenHousekeepingThread();

 private:
  /// Discovery configuration
  std::shared_ptr<ConsulTfScheduler> mDiscoveryConfig;

  /// Scheduler threads
  std::atomic_bool mRunning = false;
  std::chrono::milliseconds mTokenResetTimeoutMs = std::chrono::milliseconds(TokenResetTimeoutMsDefault);
  std::atomic_uint mTokensPerStfSenderCnt = TokensPerStfSenderCntDefault;
  std::atomic_bool mUpdateConfig = true;
  std::atomic_uint mNewTokensPerStfSenderCnt = TokensPerStfSenderCntDefault;

  std::thread mTokenThread;
  std::thread mHousekeeperThread;

  /// UCX connection
  ucp_context_h ucp_context;
  ucx::dd_ucp_worker ucp_worker;

  std::shared_mutex mOutputMapLock;
    std::unordered_map<std::string, std::unique_ptr<TfBuilderUCXConnInfo>> mOutputMap;

public:
  // token management
  alignas(256)
  boost::lockfree::stack<ucx::io::TokenRequest::BitFieldIdxType, boost::lockfree::capacity<512> > mReleasedTokens;
  boost::lockfree::queue<TokenRequestInfo, boost::lockfree::capacity<512> > mTokenRequestQueue;
  alignas(256)
  ucx::io::TokenRequest::Bitfield mBaseTokens;
  ucx::io::TokenRequest::Bitfield mExtraTokens;
  unsigned mTokensPerStfSender[ucx::io::TokenRequest::Bitfield::size()];

  // token management
  alignas(256)
  std::atomic_uint64_t mSpinCounter = 0;
  std::atomic_uint64_t mNumReqSinceReset = 1000; // prevent throttling on init
  std::atomic_uint64_t mReqSuccessBase = 0;
  std::atomic_uint64_t mReqSuccessExtra = 0;
  std::atomic_uint64_t mReqFailed = 0;

};

} /* namespace o2::DataDistribution */

#endif /* TF_SCHEDULER_TOKEN_MANAGER_H_ */
