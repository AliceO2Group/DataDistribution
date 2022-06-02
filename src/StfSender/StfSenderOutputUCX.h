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

#ifndef STF_SENDER_OUTPUT_UCX_H_
#define STF_SENDER_OUTPUT_UCX_H_

#include "StfSenderOutputDefs.h"

#include <ConfigConsul.h>
#include <ConcurrentQueue.h>

#include <SubTimeFrameVisitors.h>

#include <UCXUtilities.h>
#include <UCXSendRecv.h>
#include <ucp/api/ucp.h>

#include <vector>
#include <map>
#include <thread>
#include <shared_mutex>
#include <boost/container/small_vector.hpp>

namespace o2::DataDistribution
{

class StfSenderOutputUCX;

typedef struct ucp_listener_context {
  StfSenderOutputUCX *mOutputUCX;

} dd_ucp_listener_context_t;


struct StfSenderUCXConnInfo {
  StfSenderOutputUCX &mOutputUCX;
  ucx::dd_ucp_worker &mWorker;

  // peer name
  std::string mTfBuilderId;
  // peer lock (thread pool)
  std::mutex mTfBuilderLock;

  ucp_ep_h      ucp_ep;

  std::atomic_bool mConnError = false;

  StfSenderUCXConnInfo() = delete;
  StfSenderUCXConnInfo(StfSenderOutputUCX &pOutputUCX, ucx::dd_ucp_worker &pWorker, const std::string &pTfBuilderId)
  : mOutputUCX(pOutputUCX),
    mWorker(pWorker),
    mTfBuilderId(pTfBuilderId)
  { }
};

class StfSenderOutputUCX : public ISubTimeFrameConstVisitor {
public:

  StfSenderOutputUCX(std::shared_ptr<ConsulStfSender> pDiscoveryConfig, StdSenderOutputCounters &pCounters);

  bool start();
  void stop();

  /// RPC requests
  ConnectStatus connectTfBuilder(const std::string &pTfBuilderId, const std::string &lTfBuilderIp, const unsigned lTfBuilderPort);
  bool disconnectTfBuilder(const std::string &pTfBuilderId);

  bool sendStfToTfBuilder(const std::string &pTfBuilderId, ScheduledStfInfo &&pStfInfo);

  // void MetaHandlerThread(unsigned pThreadIdx);
  void DataHandlerThread(unsigned pThreadIdx);
  void StfAckThread();
  void StfDeallocThread();

  void pushStfAck(const std::uint64_t pStfId) {
    mStfAckQueue.push(pStfId);
  }

  void handle_client_ep_error(StfSenderUCXConnInfo *pUCXConnInfo, ucs_status_t status) {
    // TfBuilder disconnected?
    if (pUCXConnInfo) {
      pUCXConnInfo->mConnError = true;
      IDDLOG_GRL(5000, "UCXConnection: TfBuilder connection terminated. tfbuilder_id={} err={}", pUCXConnInfo->mTfBuilderId, ucs_status_string(status));
      disconnectTfBuilder(pUCXConnInfo->mTfBuilderId);
    }
  }

  /// SHM Region registration
  struct UCXMemoryRegionInfo {
    void *mPtr = nullptr;
    std::size_t mSize = 0;

    ucp_mem_h ucp_mem = nullptr;
    void      *ucp_rkey_buf = nullptr;
    std::size_t ucp_rkey_buf_size = 0;

    bool operator==(const UCXMemoryRegionInfo &a) const { return (mPtr == a.mPtr); }
  };

  mutable std::mutex mRegionListLock;
    std::vector<UCXMemoryRegionInfo> mRegions;

  void registerSHMRegion(void *pPtr, const std::size_t pSize, const bool pManaged, const std::uint64_t pFlags) {
    if (!mRunning) {
      WDDLOG("OutputUCX::registerSHMRegion: Skipping region mapping. UCX output is not running. size={} managed={} flags={}",
        pSize, pManaged, pFlags);
      return;
    }

    UCXMemoryRegionInfo lMemInfo;
    lMemInfo.mPtr = pPtr;
    lMemInfo.mSize = pSize;

    // Map the memory region for reading
    if (!ucx::util::create_rkey_for_region(ucp_context, pPtr, pSize, true /*ro*/, &lMemInfo.ucp_mem, &lMemInfo.ucp_rkey_buf,
      &lMemInfo.ucp_rkey_buf_size)) {
      EDDLOG("StfSenderOutputUCX: Cannot register region with ucx. size={}", pSize);
      return;
    }

    IDDLOG("OutputUCX::registerSHMRegion: New region mapped. size={} managed={} flags={} rkey_size={}",
      pSize, pManaged, pFlags, lMemInfo.ucp_rkey_buf_size);

    {
      std::scoped_lock lLock(mRegionListLock);
      mRegions.push_back(lMemInfo);
    }
  }

  const UCXMemoryRegionInfo* regionLookup(std::uint64_t pPtr, const std::size_t pSize) const {
    static thread_local boost::container::small_vector<UCXMemoryRegionInfo, 16> sRegions;
    bool lReloadCache = true;

    do {
      for (const auto &lRegion : sRegions) {
        // DDDLOG_RL(1000, "mesg={} len={} region={} len={}", pPtr, pSize, reinterpret_cast<std::uint64_t>(lRegion.mPtr), lRegion.mSize);

        if ((reinterpret_cast<char*>(pPtr) >= lRegion.mPtr) &&
          (reinterpret_cast<char*>(pPtr)+pSize) <= (reinterpret_cast<char*>(lRegion.mPtr)+lRegion.mSize)) {
          return &lRegion;
        }
      }
      if (!lReloadCache) {
        EDDLOG_RL(1000, "regionLookup: region not found: msg_ptr={} len={}", pPtr, pSize);
        throw std::runtime_error("no region matched");
      }

      sRegions.clear();
      {
        std::scoped_lock lLock(mRegionListLock);
        std::copy(mRegions.begin(), mRegions.end(), std::back_inserter(sRegions));
      }
      std::sort(sRegions.begin(), sRegions.end(), [](auto &a, auto &b) { return a.mSize > b.mSize; } );
      lReloadCache = false;
    } while (true);

    throw std::runtime_error("no region matched");
  }


protected:
  virtual void visit(const SubTimeFrame&, void*) final override;

private:
  /// Running flag
  std::atomic_bool mRunning = false;

  /// Discovery configuration
  std::shared_ptr<ConsulStfSender> mDiscoveryConfig;
  const std::string mStfSenderId;

  /// Runtime options
  std::size_t mRmaGap;
  std::size_t mThreadPoolSize;

  // Global stf counters
  StdSenderOutputCounters &mCounters;

  mutable std::shared_mutex mOutputMapLock;
    std::map<std::string, std::unique_ptr<StfSenderUCXConnInfo> > mOutputMap;

  /// UCX objects

  /// context and listener
  ucp_context_h ucp_context;
  boost::container::small_vector<ucx::dd_ucp_worker, 512> mDataWorkers;

  dd_ucp_listener_context_t ucp_listen_context;

  // make UCXIovStfHeader
  void prepareStfMetaHeader(const SubTimeFrame &pStf, UCXIovStfHeader *pStfUCXMeta);

  // thread pool for stf requests
  std::vector<std::thread> mThreadPool;
  struct SendStfInfo {
    std::unique_ptr<SubTimeFrame> mStf;
    std::string mTfBuilderId;

    SendStfInfo(std::unique_ptr<SubTimeFrame> &&pStf, const std::string &pTfBuilderId)
    : mStf(std::move(pStf)), mTfBuilderId(pTfBuilderId) { }
  };

  ConcurrentFifo<std::unique_ptr<SendStfInfo>> mSendRequestQueue;

  /// map of STFs waiting on transfers
  std::mutex mStfsInFlightMutex;
    std::map<std::uint64_t, std::unique_ptr<SendStfInfo>> mStfsInFlight;
    std::set<std::string> mDisconnectedTfBuilders;
  ConcurrentQueue<std::uint64_t> mStfAckQueue;
  std::thread mStfAckThread;

  // stf dealloc thread
  std::thread mDeallocThread;
  ConcurrentQueue<std::unique_ptr<SubTimeFrame>> mStfDeleteQueue;
};

} /* namespace o2::DataDistribution */

#endif /* STF_SENDER_OUTPUT_UCX_H_ */
