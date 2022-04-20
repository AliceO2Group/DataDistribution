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

#ifndef DATADIST_UCX_UTILITIES_H_
#define DATADIST_UCX_UTILITIES_H_

#include <DataDistLogger.h>

#include <sys/socket.h>
#include <arpa/inet.h>

#include <ucp/api/ucp.h>

#include <sys/epoll.h>

#include <string>
#include <cstring>

namespace o2::DataDistribution {

namespace ucx {

struct dd_ucp_worker {
  ucp_worker_h ucp_worker;

  int worker_efd;

  int epoll_fd;
  struct epoll_event ev;
};

}

namespace ucx::util {

extern std::size_t UCX_REQUEST_SIZE;

static inline
void ucp_empty_cb(void *request, ucs_status_t status)
{
  (void) request; (void) status;
}

static inline
void set_sockaddr_ip_port(sockaddr_storage *pSockAddr, const std::string &pIpv4, const unsigned pPort = 0)
{
  struct sockaddr_in *sa_in = reinterpret_cast<struct sockaddr_in *>(pSockAddr);
  std::memset(pSockAddr, 0, sizeof(sockaddr_storage));
  // Set IPv4
  sa_in->sin_family = AF_INET;
  // Set IP
  if (!pIpv4.empty()) {
    inet_pton(AF_INET, pIpv4.c_str(), &sa_in->sin_addr);
  } else {
    sa_in->sin_addr.s_addr = INADDR_ANY;
  }
  // Set port
  sa_in->sin_port   = htons(pPort);
}

static inline
std::string sockaddr_to_string(const sockaddr_storage *pSockAddr)
{
  struct sockaddr_in  addr_in;
  char ip_addr[128];
  memcpy(&addr_in, pSockAddr, sizeof(struct sockaddr_in));
  inet_ntop(AF_INET, &addr_in.sin_addr, ip_addr, 128);
  return std::string(ip_addr);
}

static inline
unsigned sockaddr_to_port(const sockaddr_storage *pSockAddr)
{
  struct sockaddr_in  addr_in;
  memcpy(&addr_in, pSockAddr, sizeof(struct sockaddr_in));
  return ntohs(addr_in.sin_port);
}


bool create_ucp_context(ucp_context_h *ucp_ctx);


static inline
bool create_ucp_worker(ucp_context_h ucp_ctx, dd_ucp_worker *worker, const std::string_view &pMsg)
{
  ucp_worker_params_t worker_params;
  std::memset(&worker_params, 0, sizeof(worker_params));
  worker_params.field_mask  = UCP_WORKER_PARAM_FIELD_THREAD_MODE;
  worker_params.thread_mode = UCS_THREAD_MODE_MULTI;

  auto status = ucp_worker_create(ucp_ctx, &worker_params, &worker->ucp_worker);
  if (status != UCS_OK) {
    EDDLOG("Failed to create listener worker={} err={}", pMsg, std::string(ucs_status_string(status)));
    return false;
  }

  // create worker epoll structures
  status = ucp_worker_get_efd(worker->ucp_worker, &worker->worker_efd);
  if (status != UCS_OK) {
    EDDLOG("Failed to create epool fd for worker={} err={}", pMsg, std::string(ucs_status_string(status)));
    return false;
  }

  // setup epoll event
  worker->epoll_fd = epoll_create(1);

  worker->ev.data.u64 = 0;
  worker->ev.data.fd = worker->worker_efd;
  worker->ev.events = EPOLLIN;

  auto err = epoll_ctl(worker->epoll_fd, EPOLL_CTL_ADD, worker->worker_efd, &worker->ev);
  if (err != 0) {
    EDDLOG("Failed to add epoll fd for worker={} errno={}", pMsg, errno);
    return false;
  }

  return true;
}

static inline
bool create_ucp_listener(ucp_worker_h ucp_worker, const std::string &pIp, ucp_listener_h *ucp_listener, ucp_listener_conn_callback_t ucp_listener_cb, void *cb_arg)
{
  struct sockaddr_storage listen_addr;
  ucp_listener_params_t listen_params;
  std::memset(&listen_params, 0, sizeof(listen_params));

  set_sockaddr_ip_port(&listen_addr, pIp);

  listen_params.field_mask         = UCP_LISTENER_PARAM_FIELD_SOCK_ADDR |
                                     UCP_LISTENER_PARAM_FIELD_CONN_HANDLER;

  listen_params.sockaddr.addr      = (const struct sockaddr*)&listen_addr;
  listen_params.sockaddr.addrlen   = sizeof(listen_addr);
  listen_params.conn_handler.cb    = ucp_listener_cb;
  listen_params.conn_handler.arg   = cb_arg;

  // Create a connection listener
  auto lStatus = ucp_listener_create(ucp_worker, &listen_params, ucp_listener);
  if (lStatus != UCS_OK) {
    EDDLOG("Failed to create UCX listener. err={}", ucs_status_string(lStatus));
    return false;
  }
  return true;
}

static inline
bool register_am_callback(dd_ucp_worker &worker, unsigned id, ucp_am_recv_callback_t ucp_am_cb, void *cb_arg)
{
  ucp_am_handler_param_t am_param;
  am_param.field_mask = UCP_AM_HANDLER_PARAM_FIELD_ID |
                        UCP_AM_HANDLER_PARAM_FIELD_CB |
                        UCP_AM_HANDLER_PARAM_FIELD_ARG;
  am_param.id         = id;
  am_param.cb         = ucp_am_cb;
  am_param.arg        = cb_arg;

  auto status = ucp_worker_set_am_recv_handler(worker.ucp_worker, &am_param);
  if (status != UCS_OK) {
    EDDLOG("Failed to register UCX AM callback. err={}", ucs_status_string(status));
    return false;
  }
  return true;
}

static inline
bool create_ucp_ep(ucp_worker_h worker, ucp_conn_request_h conn_req, ucp_ep_h *ep, ucp_err_handler_cb_t pErrCb, void* pErrCbArg, const std::string_view &pMsg)
{
  ucp_ep_params_t ep_params;
  ep_params.field_mask      = UCP_EP_PARAM_FIELD_ERR_HANDLING_MODE |
                              UCP_EP_PARAM_FIELD_ERR_HANDLER       |
                              UCP_EP_PARAM_FIELD_CONN_REQUEST;

  ep_params.err_mode        = UCP_ERR_HANDLING_MODE_PEER;
  ep_params.err_handler.cb  = pErrCb;
  ep_params.err_handler.arg = pErrCbArg;
  ep_params.conn_request    = conn_req;

  const auto lStatus = ucp_ep_create(worker, &ep_params, ep);
  if (lStatus != UCS_OK) {
    EDDLOG("Failed to create an endpoint. ep={} err={}", pMsg, ucs_status_string(lStatus));
    return false;
  }
  return true;
}

static inline
bool create_ucp_client_ep(dd_ucp_worker &worker, const std::string &pIpv4, const unsigned pPort, ucp_ep_h *ep,
  ucp_err_handler_cb_t pErrCb, void* pErrCbArg, const std::string_view &pMsg)
{
  ucp_ep_params_t ep_params;
  struct sockaddr_storage connect_addr;

  // setup endpoint ip and port
  set_sockaddr_ip_port(&connect_addr, pIpv4, pPort);

  // setup ep params
  ep_params.field_mask       = UCP_EP_PARAM_FIELD_FLAGS              |
                               UCP_EP_PARAM_FIELD_SOCK_ADDR          |
                               UCP_EP_PARAM_FIELD_ERR_HANDLER        |
                               UCP_EP_PARAM_FIELD_ERR_HANDLING_MODE;

  ep_params.err_mode         = UCP_ERR_HANDLING_MODE_PEER;
  ep_params.err_handler.cb   = pErrCb;
  ep_params.err_handler.arg  = pErrCbArg;
  ep_params.flags            = UCP_EP_PARAMS_FLAGS_CLIENT_SERVER;
  ep_params.sockaddr.addr    = (struct sockaddr*)&connect_addr;
  ep_params.sockaddr.addrlen = sizeof(connect_addr);


  const auto lStatus = ucp_ep_create(worker.ucp_worker, &ep_params, ep);
  if (lStatus != UCS_OK) {
    EDDLOG("Failed to create new client endpoint. ep={} err={}", pMsg, ucs_status_string(lStatus));
    return false;
  }
  return true;
}

static inline
bool flush_ep_blocking(ucp_worker_h worker, ucp_ep_h ep)
{
  auto lFlushReq = ucp_ep_flush_nb(ep, 0, ucp_empty_cb);

  if (lFlushReq == NULL) {
    return true;
  } else if (UCS_PTR_IS_ERR(lFlushReq)) {
    return false;
  } else {

    ucs_status_t lProgressStatus;

    do {
      ucp_worker_progress(worker);
      lProgressStatus = ucp_request_check_status(lFlushReq);
    } while (UCS_INPROGRESS == lProgressStatus);

    ucp_request_free(lFlushReq);
  }
  return true;
}

static inline
void close_ep_connection(dd_ucp_worker &worker, ucp_ep_h ep)
{
  ucp_request_param_t param;
  param.op_attr_mask = UCP_OP_ATTR_FIELD_FLAGS;
  param.flags        = UCP_EP_CLOSE_MODE_FORCE;

  auto close_req = ucp_ep_close_nbx(ep, &param);
  if (UCS_PTR_IS_PTR(close_req)) {
    ucs_status_t status;
    do {
      ucp_worker_progress(worker.ucp_worker);
      status = ucp_request_check_status(close_req);
    } while (status == UCS_INPROGRESS);

    ucp_request_free(close_req);
  }
}

bool create_rkey_for_region(ucp_context_h ctx, void *addr, const std::uint64_t size, bool rdonly,
  ucp_mem_h *ucp_mem, void **ucp_rkey_buf, std::size_t *ucp_rkey_size);

char* get_mem_address(ucp_mem_h ucp_mem);

bool destroy_rkey_for_region(ucp_context_h ctx, ucp_mem_h ucp_mem, void *ucp_rkey_buf);

} /* ucx::util */

}
#endif // DATADIST_UCX_UTILITIES_H_