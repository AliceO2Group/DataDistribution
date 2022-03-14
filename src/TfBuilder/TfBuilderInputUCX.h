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

#ifndef TF_BUILDER_INPUT_UCX_H_
#define TF_BUILDER_INPUT_UCX_H_

#include "TfBuilderInputDefs.h"

#include <ConfigConsul.h>
#include <discovery.pb.h>

#include <SubTimeFrameDataModel.h>
#include <ConcurrentQueue.h>

#include <UCXUtilities.h>
#include <ucp/api/ucp.h>

#include <vector>
#include <map>

#include <thread>

namespace o2::DataDistribution
{
class TfBuilderInputUCX;
class TimeFrameBuilder;
class TfBuilderRpcImpl;


struct dd_ucp_listener_context_t {
  /// Self reference to handle new connections
  TfBuilderInputUCX *mInputUcx;
};

struct dd_ucx_conn_info {
  /// Self reference to perform cleanup on errors
  TfBuilderInputUCX* mInputUCX;

  /// Peer StfSender id
  std::string mStfSenderId;

  /// UCP worker and ep
  ucx::dd_ucp_worker  worker;
  ucp_ep_h ucp_ep;

  /// Lock to ensure only one thread is using the endpoint at any time
  std::mutex mStfSenderIoLock;

  /// cache of unpacked remote rma keys
  std::map<std::string, ucp_rkey_h> mRemoteKeys;

  /// Signal that peer connection has problems
  std::atomic_bool mConnError = false;

  dd_ucx_conn_info() = delete;
  dd_ucx_conn_info(TfBuilderInputUCX *pThis) : mInputUCX(pThis) { }
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
  {
  }

  bool start();
  bool map_data_region();

  void stop();
  void reset() { }

  void DataHandlerThread(const unsigned pThreadIdx);

  void handle_client_ep_error(dd_ucx_conn_info *pConn, ucs_status_t pStatus) {

    if (pConn) {
      pConn->mConnError = true;
      EDDLOG("TfBuilderInputUCX: peer connection error. stfsender_id={} err={}", pConn->mStfSenderId, ucs_status_string(pStatus));
    }

    // TODO: survive loss of StfSender peer?
    bool lExpectedStop = false;
    if (mStopRequested.compare_exchange_strong(lExpectedStop, true)) {
      std::thread([&](){ stop(); }).detach();
    }
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
  std::uint64_t mNumRmaOps;
  std::vector<std::thread> mThreadPool;

  /// Queue for received STFs
  ConcurrentQueue<ReceivedStfMeta> &mReceivedDataQueue;

private:
  /// UCX context
  ucp_context_h ucp_context;
  bool ucp_data_region_set = false;
  ucp_mem_h ucp_data_region;

  /// listener
  ucx::dd_ucp_worker listener_worker;
  ucp_listener_h ucp_listener;
  dd_ucp_listener_context_t dd_ucp_listen_context;

  void ListenerThread();
  std::thread mListenerThread;
  ConcurrentQueue<std::tuple<std::string, unsigned, ucp_conn_request_h> > mConnRequestQueue;
  std::mutex mConnectionMapLock;
    std::map<std::string, std::unique_ptr<dd_ucx_conn_info>> mConnMap;

public:
  // UCX callbacks
  void new_conn_handle(ucp_conn_request_h conn_request);
};


} /* namespace o2::DataDistribution */

#endif /* TF_BUILDER_INPUT_UCX_H_ */
