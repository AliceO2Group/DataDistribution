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

#ifndef DATADIST_UCX_SENDRECV_H_
#define DATADIST_UCX_SENDRECV_H_

#include "UCXUtilities.h"
#include <Utilities.h>

#include <ucp/api/ucp.h>

#include <new>

namespace o2::DataDistribution {

namespace ucx::io::impl {

using o2::DataDistribution::ucx::util::UCX_REQUEST_SIZE;

static constexpr ucp_tag_t STRING_TAG       = 1;
static constexpr ucp_tag_t STRING_SIZE_TAG  = 2;

} /* ucx::impl */

namespace ucx::io {

static constexpr unsigned AM_STF_META  = 23;
static constexpr unsigned AM_STF_ACK   = 99;
// token AMs
static constexpr unsigned AM_TOKEN_REQ = 200;
static constexpr unsigned AM_TOKEN_REL = 201;
static constexpr unsigned AM_TOKEN_REP = 202;

struct TokenRequest {

  using Bitfield = TokenBitfield<220>;
  using BitFieldIdxType = Bitfield::TokenBitfieldIndexType;
  static const constexpr auto BitfieldInvalidIdx = Bitfield::sInvalidIdx;

  Bitfield mTokensRequested;
};

struct dd_ucp_multi_req {
  alignas(128)
  std::atomic_uint64_t mSlotsUsed = 0;
  const std::uint64_t mSlotsCount = 1;
  std::atomic_bool mFinished = false;

  dd_ucp_multi_req() = delete;
  explicit dd_ucp_multi_req(const std::uint64_t pSlotsCount) : mSlotsCount(pSlotsCount)  { }
  dd_ucp_multi_req(const dd_ucp_multi_req&) = delete;
  dd_ucp_multi_req(dd_ucp_multi_req&&) = delete;

  ~dd_ucp_multi_req() { }

  inline bool done() const {
    if (!mFinished) {
      // let rma progress as long as there are free slots
      return (mSlotsUsed.load() < mSlotsCount);
    } else {
      // done when slots used is zero
      return (mSlotsUsed.load() == 0);
    }
  }

  inline bool add_request(void *req) {
    if (UCS_PTR_IS_ERR(req)) {
      EDDLOG("Failed run ucp_get_nbx ucx_err={}", ucs_status_string(UCS_PTR_STATUS(req)));
      return false;
    }
    // operation returned request
    if (req && UCS_PTR_IS_PTR(req)) {
      mSlotsUsed += 1;
    }
    return true;
  }

  inline bool remove_request(void *req) {
    // operation returned request
    if (req && UCS_PTR_IS_PTR(req)) {
      mSlotsUsed -= 1;
      ucp_request_free(req);
    }
    return true;
  }

  void mark_finished() { mFinished = true; }
};


struct dd_ucp_multi_req_v2 {
  alignas(128)
  volatile uint64_t mReqsStarted = 0;
  std::atomic_uint64_t mReqsDone = 0;

  dd_ucp_multi_req_v2() { }
  dd_ucp_multi_req_v2(const dd_ucp_multi_req&) = delete;
  dd_ucp_multi_req_v2(dd_ucp_multi_req&&) = delete;

  ~dd_ucp_multi_req_v2() { }

  inline bool done() const {
    if (mReqsStarted > mReqsDone) {
      return false;
    }
    assert (mReqsStarted == mReqsDone);
    return true;
  }

  inline bool add_request(void *req) {
    if (UCS_PTR_IS_ERR(req)) {
      return false;
    }
    // operation returned request
    if (req && UCS_PTR_IS_PTR(req)) {
      mReqsStarted += 1;
      return true;
    }
    return false;
  }

  inline bool remove_request(void *req) {
    // operation returned request
    if (req && UCS_PTR_IS_PTR(req)) {
      mReqsDone += 1;
      ucp_request_free(req);
    }
    return true;
  }

  inline
  bool wait(dd_ucp_worker &pDDCtx, const bool pPollingWait) const {
    if (pPollingWait) {
      for (;;) {
        // check if request is done
        if (done()) {
          return true;
        }
        while (ucp_worker_progress(pDDCtx.ucp_worker) > 0) { }
      }
      return true;
    } else {
      // blocking wait
      for (;;) {
        // check if request is done
        if (done()) {
          return true;
        } else if (ucp_worker_progress(pDDCtx.ucp_worker)) {
          continue;
        }

        // block on the worker
        auto status = ucp_worker_arm(pDDCtx.ucp_worker);

        if (UCS_OK == status) {
          int epoll_ret;
          do {
            epoll_ret = epoll_wait(pDDCtx.epoll_fd, &pDDCtx.ev, 1, 100);
          } while ((epoll_ret == -1) && (errno == EINTR || errno == EAGAIN));

          if (epoll_ret == -1) {
            EDDLOG("Failed ucp_advance epoll. errno={}", errno);
            return done();
          }
        } else if (UCS_ERR_BUSY == status) {
          continue; // could not arm, recheck the request
        }
          // epoll returned or timeout, recheck the request
      }
      return done();
    }
  }
};


static inline
bool ucp_wait(dd_ucp_worker &pDDCtx, dd_ucp_multi_req &pReq)
{
  for (;;) {
    // check if request is done
    if (pReq.done()) {
      return true;
    } else if (ucp_worker_progress(pDDCtx.ucp_worker)) {
      continue;
    }

    // block on the worker
    auto status = ucp_worker_arm(pDDCtx.ucp_worker);

    if (UCS_OK == status) {
      int epoll_ret;
      do {
        epoll_ret = epoll_wait(pDDCtx.epoll_fd, &pDDCtx.ev, 1, 100);
      } while ((epoll_ret == -1) && (errno == EINTR || errno == EAGAIN));

      if (epoll_ret == -1) {
        EDDLOG("Failed ucp_advance epoll. errno={}", errno);
        return pReq.done();
      }
    } else if (UCS_ERR_BUSY == status) {
      continue; // could not arm, recheck the request
    }
      // epoll returned or timeout, recheck the request
  }
  return pReq.done();
}

static inline
bool ucp_wait_poll(dd_ucp_worker &pDDCtx, dd_ucp_multi_req_v2 &pReq)
{
  for (;;) {
    // check if request is done
    if (pReq.done()) {
      return true;
    }
    while (ucp_worker_progress(pDDCtx.ucp_worker) > 0) { }
  }
  return true;
}

static
void send_multi_cb(void *req, ucs_status_t status, void *user_data)
{
  if (UCS_OK == status) {
    dd_ucp_multi_req *dd_req = reinterpret_cast<dd_ucp_multi_req*>(user_data);
    assert (dd_req->mSlotsUsed.load() > 0);

    // signal completion of one call
    dd_req->remove_request(req);
  }
}

static
void recv_multi_cb(void *req, ucs_status_t status, const ucp_tag_recv_info_t *, void *user_data)
{
  if (UCS_OK == status) {
    dd_ucp_multi_req *dd_req = reinterpret_cast<dd_ucp_multi_req*>(user_data);
    assert (dd_req->mSlotsUsed.load() > 0);

    // signal completion of one call
    dd_req->remove_request(req);
  }
}


static inline
bool send_tag_blocking(dd_ucp_worker &worker, ucp_ep_h ep, const void *data, const std::size_t size, const ucp_tag_t tag)
{
  ucp_request_param_t param;
  void *ucp_request; // ucp allocated request
  // dd_ucp_req dd_request;
  dd_ucp_multi_req dd_request(1);

  param.op_attr_mask = UCP_OP_ATTR_FIELD_CALLBACK |
                       UCP_OP_ATTR_FIELD_DATATYPE |
                       UCP_OP_ATTR_FIELD_USER_DATA;
  param.cb.send      = send_multi_cb;
  param.datatype     = ucp_dt_make_contig(1);
  param.user_data    = &dd_request;
  ucp_request = ucp_tag_send_nbx(ep, data, size, tag, &param);
  if (ucp_request == NULL) {
    return true;
  }
  dd_request.add_request(ucp_request);
  dd_request.mark_finished();

  if (UCS_PTR_IS_ERR(ucp_request)) {
    EDDLOG("Failed send_tag_blocking. tag={} err={}", tag, ucs_status_string(UCS_PTR_STATUS(ucp_request)));
    return false;
  } else {
    ucp_wait(worker, dd_request);
    bool ok = dd_request.done();
    if (!ok) {
      EDDLOG("Failed send_tag_blocking. flag={} tag={} err={}", dd_request.done(), tag, ucs_status_string(UCS_PTR_STATUS(ucp_request)));
    }
  }

  return dd_request.done();
}

static inline
bool receive_tag_blocking(dd_ucp_worker &worker, void *data, const std::size_t size, const ucp_tag_t tag)
{
  ucp_request_param_t param;
  void *ucp_request; // ucp allocated request
  dd_ucp_multi_req dd_request(1);

  param.op_attr_mask = UCP_OP_ATTR_FIELD_CALLBACK |
                       UCP_OP_ATTR_FIELD_DATATYPE |
                       UCP_OP_ATTR_FIELD_USER_DATA;
  param.cb.recv      = recv_multi_cb;
  param.datatype     = ucp_dt_make_contig(1);
  param.user_data    = &dd_request;
  ucp_request = ucp_tag_recv_nbx(worker.ucp_worker, data, size, tag, 0, &param);
  if (ucp_request == NULL) {
    return true;
  }
  dd_request.add_request(ucp_request);
  dd_request.mark_finished();

  if (UCS_PTR_IS_ERR(ucp_request)) {
    EDDLOG("Failed receive_tag_blocking. tag={} err={}", tag, ucs_status_string(UCS_PTR_STATUS(ucp_request)));
    return false;
  } else {
    ucp_wait(worker, dd_request);
  }

  return dd_request.done();
}


static inline
bool get(ucp_ep_h ep, void *buffer, const std::size_t size, const std::uint64_t rptr, ucp_rkey_h rkey, dd_ucp_multi_req_v2 *dd_req)
{
  ucp_request_param_t param;

  param.op_attr_mask = UCP_OP_ATTR_FIELD_CALLBACK |
                       UCP_OP_ATTR_FIELD_DATATYPE |
                       UCP_OP_ATTR_FIELD_USER_DATA;
  param.cb.send      = send_multi_cb;
  param.datatype     = ucp_dt_make_contig(1);
  param.user_data    = dd_req;

  void *req = ucp_get_nbx(ep, buffer, size, rptr, rkey, &param);
  return dd_req->add_request(req);
}

static inline
bool ucx_send_string(dd_ucp_worker &worker, ucp_ep_h ep, const std::string &lString)
{
  // send size
  const std::uint64_t size_buffer = lString.size();

  if (!send_tag_blocking(worker, ep, &size_buffer, sizeof(std::uint64_t), impl::STRING_SIZE_TAG) ) {
    return false;
  }
  // send actual string data
  return send_tag_blocking(worker, ep, lString.data(), size_buffer, impl::STRING_TAG);
}

static inline
bool ucx_send_data(dd_ucp_worker &worker, ucp_ep_h ep, const void *pData, const std::uint64_t *pSize)
{
  const std::uint64_t pSizeOrig = *pSize;

  if (!send_tag_blocking(worker, ep, pSize, sizeof(std::uint64_t), impl::STRING_SIZE_TAG) ) {
    return false;
  }
  // send actual string data
  return send_tag_blocking(worker, ep, pData, pSizeOrig, impl::STRING_TAG);
}

static inline
bool ucx_send_am(dd_ucp_worker &worker, ucp_ep_h ep, const unsigned id, const void *pData, const std::size_t pSize)
{
  ucp_request_param_t param;
  void *ucp_request; // ucp allocated request
  dd_ucp_multi_req_v2 dd_request;

  param.op_attr_mask = UCP_OP_ATTR_FIELD_CALLBACK    |
                       UCP_OP_ATTR_FIELD_DATATYPE    |
                       UCP_OP_ATTR_FIELD_USER_DATA   |
                       UCP_OP_ATTR_FIELD_MEMORY_TYPE |
                       UCP_OP_ATTR_FIELD_FLAGS;
  param.cb.send      = send_multi_cb;
  param.datatype     = ucp_dt_make_contig(1);
  param.user_data    = &dd_request;
  param.memory_type  = ucs_memory_type_t::UCS_MEMORY_TYPE_HOST;
  param.flags        = UCP_AM_SEND_FLAG_EAGER | UCP_AM_SEND_FLAG_REPLY;

  ucp_request = ucp_am_send_nbx(ep, id, NULL, 0, pData, pSize, &param);

  if (ucp_request == NULL) {
    return true;
  }

  if (UCS_PTR_IS_ERR(ucp_request)) {
    EDDLOG("Failed ucx_send_am. id={} err={}", id, ucs_status_string(UCS_PTR_STATUS(ucp_request)));
    return false;
  } else {
    dd_request.add_request(ucp_request);

    const bool ok = dd_request.wait(worker, true); // polling wait
    if (!ok) {
      EDDLOG("Failed ucx_send_am. flag={} id={} err={}", dd_request.done(), id, ucs_status_string(UCS_PTR_STATUS(ucp_request)));
    }
  }

  return true;
}

static inline
bool ucx_send_am_hdr(dd_ucp_worker &worker, ucp_ep_h ep, const unsigned id, const void *pHdrData, const std::size_t pHdrSize)
{
  ucp_request_param_t param;
  void *ucp_request; // ucp allocated request
  dd_ucp_multi_req_v2 dd_request;

  param.op_attr_mask = UCP_OP_ATTR_FIELD_CALLBACK    |
                       UCP_OP_ATTR_FIELD_DATATYPE    |
                       UCP_OP_ATTR_FIELD_USER_DATA   |
                       UCP_OP_ATTR_FIELD_MEMORY_TYPE |
                       UCP_OP_ATTR_FIELD_FLAGS;
  param.cb.send      = send_multi_cb;
  param.datatype     = ucp_dt_make_contig(1);
  param.user_data    = &dd_request;
  param.memory_type  = ucs_memory_type_t::UCS_MEMORY_TYPE_HOST;
  param.flags        = UCP_AM_SEND_FLAG_EAGER | UCP_AM_SEND_FLAG_REPLY;

  ucp_request = ucp_am_send_nbx(ep, id, pHdrData, pHdrSize, NULL, 0, &param);

  if (ucp_request == NULL) {
    return true;
  }

  if (UCS_PTR_IS_ERR(ucp_request)) {
    EDDLOG("Failed ucx_send_am. id={} err={}", id, ucs_status_string(UCS_PTR_STATUS(ucp_request)));
    return false;
  } else {
    dd_request.add_request(ucp_request);

    const bool ok = dd_request.wait(worker, true); // polling wait
    if (!ok) {
      EDDLOG("Failed ucx_send_am. flag={} id={} err={}", dd_request.done(), id, ucs_status_string(UCS_PTR_STATUS(ucp_request)));
    }
  }

  return true;
}

static inline
std::optional<std::string> ucx_receive_string(dd_ucp_worker &worker)
{
  // receive the size
  std::uint64_t size_rcv = 0;

  if (!receive_tag_blocking(worker, &size_rcv, sizeof(std::uint64_t), impl::STRING_SIZE_TAG) ) {
    return std::nullopt;
  }

  std::string lRetStr(size_rcv, 0);
  if (!receive_tag_blocking(worker, lRetStr.data(), lRetStr.size(), impl::STRING_TAG) ) {
    return std::nullopt;
  }

  return lRetStr;
}

static inline
std::int64_t ucx_receive_tag(dd_ucp_worker &worker, void *pData, const std::size_t pSize, std::uint64_t *pReqSize)
{
  assert (pSize >= sizeof(std::uint64_t));
  assert (pReqSize != nullptr);

  // receive the size
  std::uint64_t size_rcv = 0;

  if (!receive_tag_blocking(worker, pData, sizeof(std::uint64_t), impl::STRING_SIZE_TAG) ) {
    return -1;
  }

  std::memcpy(&size_rcv, pData, sizeof(std::uint64_t));
  if (size_rcv > pSize) {
    // we need a larger buffer
    *pReqSize = size_rcv;
    return 0;
  }

  if (!receive_tag_blocking(worker, pData, size_rcv, impl::STRING_TAG) ) {
    return -1;
  }
  *pReqSize = pSize;
  return size_rcv;
}

static inline
std::int64_t ucx_receive_tag_data(dd_ucp_worker &worker, void *pData, const std::uint64_t pReqSize)
{
  if (!receive_tag_blocking(worker, pData, pReqSize, impl::STRING_TAG) ) {
    return -1;
  }

  return pReqSize;
}


static inline
std::optional<std::string> ucx_receive_string_data(dd_ucp_worker &worker)
{
  // receive the size
  std::uint64_t size_rcv = 0;

  if (!receive_tag_blocking(worker, &size_rcv, sizeof(std::uint64_t), impl::STRING_SIZE_TAG) ) {
    return std::nullopt;
  }

  std::string lRetStr(size_rcv, 0);
  if (!receive_tag_blocking(worker, lRetStr.data(), lRetStr.size(), impl::STRING_TAG) ) {
    return std::nullopt;
  }

  return lRetStr;
}


} /* ucx::io */


}
#endif // DATADIST_UCX_SENDRECV_H_