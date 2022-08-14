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
