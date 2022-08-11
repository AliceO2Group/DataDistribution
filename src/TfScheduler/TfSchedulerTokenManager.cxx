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

#include "TfSchedulerTokenManager.h"
#include "DataDistMonitoring.h"


namespace o2::DataDistribution
{

using namespace std::chrono_literals;


static ucs_status_t ucp_am_token_req_data_cb(void *arg, const void *header, size_t header_length,
                                  void *data, size_t length, const ucp_am_recv_param_t *param)
{
  TfSchedulerTokenManager *lTokenManager = reinterpret_cast<TfSchedulerTokenManager*>(arg);
  (void) data;
  (void) length;
  (void) param;
  (void) header_length;
  assert (header_length == sizeof (ucx::io::TokenRequest));
  assert (param->recv_attr & UCP_AM_RECV_ATTR_FIELD_REPLY_EP);

  TokenRequestInfo lReqInfo;
  // save the reply endpoint
  lReqInfo.mReplyEp = param->reply_ep;
  std::memcpy(&lReqInfo.mRequest, header, sizeof (ucx::io::TokenRequest));

  if (lReqInfo.mRequest.mTokensRequested) {
    lTokenManager->mTokenRequestQueue.push(lReqInfo);
  }

#ifndef NDEBUG
  DDDLOG_RL(1000, "ucp_am_token_req_data_cb: req_popcnt={}", lReqInfo.mRequest.mTokensRequested.popcnt());
#endif

  return UCS_OK;
}

static ucs_status_t ucp_am_token_rel_data_cb(void *arg, const void *header, size_t header_length,
                                  void *data, size_t length, const ucp_am_recv_param_t *param)
{
  TfSchedulerTokenManager *lTokenManager = reinterpret_cast<TfSchedulerTokenManager*>(arg);
  (void) data;
  (void) length;
  (void) param;
  (void) header_length;
  assert (header_length == sizeof (ucx::io::TokenRequest::BitFieldIdxType));

  ucx::io::TokenRequest::BitFieldIdxType lRelToken;
  std::memcpy(&lRelToken, header, sizeof(ucx::io::TokenRequest::BitFieldIdxType));

  assert (lRelToken != ucx::io::TokenRequest::BitfieldInvalidIdx);

#ifndef NDEBUG
  DDDLOG_RL(1000, "ucp_am_token_rel_data_cb: rel_token={}", (lRelToken - 1));
#endif

  lTokenManager->mReleasedTokens.push(lRelToken);

  return UCS_OK;
}

static void client_ep_err_cb(void *arg, ucp_ep_h, ucs_status_t status)
{
  (void) status;
  TfBuilderUCXConnInfo *lConnInfo = reinterpret_cast<TfBuilderUCXConnInfo*>(arg);
  if (lConnInfo) {
    lConnInfo->mConnError = true;
  }
}

TfSchedulerTokenManager::TfSchedulerTokenManager(std::shared_ptr<ConsulTfScheduler> pConfig)
{
  mDiscoveryConfig = pConfig;
  mOutputMap.reserve(300);
}

bool TfSchedulerTokenManager::start()
{
  mTokenResetTimeoutMs = std::chrono::milliseconds(mDiscoveryConfig->getUInt64Param(TokenResetTimeoutMsKey,
    TokenResetTimeoutMsDefault));
  mTokensPerStfSenderCnt = std::max(std::uint64_t(1), mDiscoveryConfig->getUInt64Param(TokensPerStfSenderCntKey,
    TokensPerStfSenderCntDefault));

  // Create the UCX context
  if (!ucx::util::create_ucp_context(&ucp_context)) {
    EDDLOG("TfSchedulerTokenManager: failed to create UCX context");
    return false;
  }

  // create one worker
  if (!ucx::util::create_ucp_worker(ucp_context, &ucp_worker, "token manager worker")) {
    return false;
  }

  // register the am handler for token requests
  if (!ucx::util::register_am_callback(ucp_worker, ucx::io::AM_TOKEN_REQ, ucp_am_token_req_data_cb, this)) {
    return false;
  }
  // register the am handler for token releases
  if (!ucx::util::register_am_callback(ucp_worker, ucx::io::AM_TOKEN_REL, ucp_am_token_rel_data_cb, this)) {
    return false;
  }

  mRunning = true;
  mTokenThread = create_thread_member("nft_maker", &TfSchedulerTokenManager::TokenManagerThread, this);
  mHousekeeperThread = create_thread_member("nft_housekeeping", &TfSchedulerTokenManager::TokenHousekeepingThread, this);

  // add all tokens
  mTokens.set_all();

  DDDLOG("Started: TfSchedulerTokenManager");
  return true;
}

void TfSchedulerTokenManager::stop()
{
  mRunning = false;

  if (mTokenThread.joinable()) {
    mTokenThread.join();
  }

  if (mHousekeeperThread.joinable()) {
    mHousekeeperThread.join();
  }

  DDDLOG("Stopped: TfSchedulerTokenManager");
}

bool TfSchedulerTokenManager::connectTfBuilder(const std::string &pTfBuilderId, const std::string &lTfBuilderIp, const unsigned lTfBuilderPort)
{
  if (!mRunning) {
    EDDLOG_ONCE("TfSchedulerTokenManager::connectTfBuilder: backend is not started.");
    return false;
  }
  // Check if connection already exists
  {
    std::shared_lock lLock(mOutputMapLock);

    if (mOutputMap.count(pTfBuilderId) > 0) {
      EDDLOG("TfSchedulerTokenManager::connectTfBuilder: TfBuilder is already connected. tfb_id={}", pTfBuilderId);
      return true;
    }
  }

  DDDLOG("TfSchedulerTokenManager::connectTfBuilder: transport starting for tfbuilder_id={}", pTfBuilderId);

  auto lConnInfo = std::make_unique<TfBuilderUCXConnInfo>(*this, ucp_worker, pTfBuilderId);

  // create endpoint for TfBuilder connection
  DDDLOG("Connect to TfBuilder ip={} port={}", lTfBuilderIp, lTfBuilderPort);
  if (!ucx::util::create_ucp_client_ep(lConnInfo->mWorker, lTfBuilderIp, lTfBuilderPort,
    &lConnInfo->ucp_ep, client_ep_err_cb, lConnInfo.get(), pTfBuilderId)) {
    return false;
  }

  DDDLOG("TfSchedulerTokenManager::connectTfBuilder: ucx::io::ucx_send_string ip={} port={}", lTfBuilderIp, lTfBuilderPort);
  const std::string lTfSchedStr = "tfscheduler";
  if (!ucx::io::ucx_send_string(lConnInfo->mWorker, lConnInfo->ucp_ep, lTfSchedStr)) {
    EDDLOG("TfSchedulerTokenManager::connectTfBuilder: Sending of local id failed. ip={} port={}", lTfBuilderIp, lTfBuilderPort);
    return false;
  }

  // Add the connection to connection map
  {
    std::unique_lock lLock(mOutputMapLock);
    const auto lItOk = mOutputMap.try_emplace(pTfBuilderId, std::move(lConnInfo));
    if (!lItOk.second) {
      EDDLOG("connectTfBuilder: TfBuilder connection already exists tfbuilder_id={}", pTfBuilderId);
      return false;
    }
  }
  DDDLOG("TfSchedulerTokenManager::connectTfBuilder: transport started for tfbuilder_id={}", pTfBuilderId);
  return true;
}

void TfSchedulerTokenManager::TokenManagerThread()
{
  using clock = std::chrono::high_resolution_clock;
  clock::time_point lRefillTime = clock::now();

  while (mRunning) {
    mSpinCounter += 1;

    // Update config if consul value changed
    if (mUpdateConfig) {
      for (std::size_t i = 0; i < ucx::io::TokenRequest::Bitfield::size(); i++) {
        mTokensPerStfSender[i] = mTokensPerStfSenderCnt;
      }
      mTokens.set_all();
      mUpdateConfig = false;
    }

    // free all released locks
    mReleasedTokens.consume_all_atomic([&](const std::uint16_t idx) {
      mTokens.set(idx);
      mTokensPerStfSender[idx-1] = std::min(mTokensPerStfSenderCnt.load(), mTokensPerStfSender[idx-1] + 1);
      assert (mTokens.get(idx) == true);
    });

    // Refill all locks on a timer to prevent missing locks from failed TfBuilders
    if ((clock::now() - lRefillTime) >= mTokenResetTimeoutMs) {
      DDMON("tfscheduler", "tokens.used", (mTokens.size() - mTokens.popcnt()));
      mTokens.set_all();
      lRefillTime = clock::now();
    }

    // see if there are any requests
    ucx::io::TokenRequest::BitFieldIdxType lReplyTokenIdx = ucx::io::TokenRequest::BitfieldInvalidIdx;
    ucp_ep_h lReplyEp;

    const bool lHaveReq = mTokenRequestQueue.consume_one([&](TokenRequestInfo &lReqInfo) {
      lReqInfo.mRequest.mTokensRequested &= mTokens;
      lReplyTokenIdx = lReqInfo.mRequest.mTokensRequested.random_idx();
      assert ((lReplyTokenIdx == 0) || (lReplyTokenIdx && (lReqInfo.mRequest.mTokensRequested.get(lReplyTokenIdx) == true)));
      lReplyEp = lReqInfo.mReplyEp;
    });

    if (lHaveReq) {
      if (lReplyTokenIdx != ucx::io::TokenRequest::BitfieldInvalidIdx) {
        assert (mTokens.get(lReplyTokenIdx) == true);
        assert (mTokensPerStfSender[lReplyTokenIdx-1] > 0);

        mTokensPerStfSender[lReplyTokenIdx-1] -= 1;
        if (mTokensPerStfSender[lReplyTokenIdx-1] == 0) {
          assert (mTokens.get(lReplyTokenIdx) == true);
          mTokens.clr(lReplyTokenIdx);
          assert (mTokens.get(lReplyTokenIdx) == false);
        } else {
          assert (mTokensPerStfSender[lReplyTokenIdx-1] > 0);
          assert (mTokens.get(lReplyTokenIdx) == true);
        }

        mReqSuccess += 1;
      } else {
        mReqFailed += 1;
      }

      // send back the reply
      ucx::io::ucx_send_am_hdr(ucp_worker, lReplyEp, ucx::io::AM_TOKEN_REP, &lReplyTokenIdx, sizeof(lReplyTokenIdx));

      mNumReqSinceReset = mNumReqSinceReset + 1;
    }

    // prevents burning the cpu core in this thread
    while (ucp_worker_progress(ucp_worker.ucp_worker) > 0) { }

    // prevent burning CPU time when not active
    if (mNumReqSinceReset == 0) {
      std::this_thread::sleep_for(5ms);
    }
  }
}

void TfSchedulerTokenManager::TokenHousekeepingThread()
{
  while (mRunning) {
    std::this_thread::sleep_for(1s);

    // throttle down the cpu
    mNumReqSinceReset = mNumReqSinceReset / 2;

    DDMON("tfscheduler", "tokens.loops_ps", mSpinCounter);
    DDMON("tfscheduler", "tokens.req_success", mReqSuccess);
    DDMON("tfscheduler", "tokens.req_failed", mReqFailed);

    mSpinCounter = 0;
    mReqSuccess = 0;
    mReqFailed = 0;

    mTokenResetTimeoutMs = std::chrono::milliseconds(mDiscoveryConfig->getUInt64Param(TokenResetTimeoutMsKey,
      TokenResetTimeoutMsDefault));
    const auto lTokensPerStfSenderCntNew = std::max(std::uint64_t(1), mDiscoveryConfig->getUInt64Param(TokensPerStfSenderCntKey,
      TokensPerStfSenderCntDefault));
    if (lTokensPerStfSenderCntNew != mTokensPerStfSenderCnt) {
      mTokensPerStfSenderCnt = lTokensPerStfSenderCntNew;
      IDDLOG("New configuration value applied. TokensPerStfSenderCnt={}", lTokensPerStfSenderCntNew);
      mUpdateConfig = true;
    }
  }
}

} /* o2::DataDistribution */
