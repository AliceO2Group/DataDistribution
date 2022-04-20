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

#include "UCXUtilities.h"

namespace o2::DataDistribution
{

////////////////////////////////////////////////////////////////////////////////
/// UCX Utilities
////////////////////////////////////////////////////////////////////////////////
namespace ucx::util {

// Initialize by create_ucp_context
std::size_t UCX_REQUEST_SIZE = 0;


bool create_ucp_context(ucp_context_h *ucp_ctx)
{
  ucp_context_attr_t ucp_ctx_attr;
  ucp_params_t ucp_params;
  std::memset(&ucp_params, 0, sizeof(ucp_params));

  // Init UCP context
  ucp_params.field_mask        = UCP_PARAM_FIELD_FEATURES |
                                 UCP_PARAM_FIELD_MT_WORKERS_SHARED |
                                 UCP_PARAM_FIELD_ESTIMATED_NUM_EPS;

  ucp_params.features          = UCP_FEATURE_AM | UCP_FEATURE_TAG | UCP_FEATURE_RMA | UCP_FEATURE_WAKEUP;
  ucp_params.mt_workers_shared = 1;
  ucp_params.estimated_num_eps = 256; // Number of TfBuilders

  const auto lStatus = ucp_init(&ucp_params, NULL, ucp_ctx);

  if (lStatus != UCS_OK) {
    EDDLOG("Failed to perform ucp_init {}", std::string(ucs_status_string(lStatus)));
    return false;
  }

  // query the required requst size
  ucp_ctx_attr.field_mask = 0;
  ucp_ctx_attr.field_mask |= UCP_ATTR_FIELD_REQUEST_SIZE;

  if (UCS_OK != ucp_context_query(*ucp_ctx, &ucp_ctx_attr)) {
    return false;
  }

  // Save the important values
  UCX_REQUEST_SIZE = ucp_ctx_attr.request_size;
  DDDLOG("UCX attr request_size={}", UCX_REQUEST_SIZE);

  return true;
}

bool create_rkey_for_region(ucp_context_h ctx, void *addr, const std::uint64_t size, bool rdonly,
  ucp_mem_h *ucp_mem, void **ucp_rkey_buf, std::size_t *ucp_rkey_size)
{
  ucp_mem_map_params_t params;
  params.field_mask = UCP_MEM_MAP_PARAM_FIELD_ADDRESS |
                      UCP_MEM_MAP_PARAM_FIELD_LENGTH |
                      UCP_MEM_MAP_PARAM_FIELD_PROT;

  params.prot       = (UCP_MEM_MAP_PROT_LOCAL_READ | UCP_MEM_MAP_PROT_REMOTE_READ);
  params.prot       = params.prot | (rdonly ? 0 : (UCP_MEM_MAP_PROT_LOCAL_WRITE | UCP_MEM_MAP_PROT_REMOTE_WRITE));
  params.address    = addr;
  params.length     = size;

  auto status = ucp_mem_map(ctx, &params, ucp_mem);
  if (UCS_OK != status) {
    EDDLOG("UCX Region mapping failed. size={} mode={}", size, (rdonly ? "rd" : "wr"));
    return false;
  }

  if (ucp_rkey_buf) {
    // Create remote key for RMA peers
    status = ucp_rkey_pack(ctx, *ucp_mem, ucp_rkey_buf, ucp_rkey_size);
    if (UCS_OK != status) {
      EDDLOG("Region rkey creation failed. size={} mode={} err={}", size, (rdonly ? "rd" : "wr"), std::string(ucs_status_string(status)));
      return false;
    }
  }
  return true;
}

char* get_mem_address(ucp_mem_h ucp_mem)
{
  ucp_mem_attr_t attr;
  attr.field_mask = UCP_MEM_ATTR_FIELD_ADDRESS;

  auto status = ucp_mem_query(ucp_mem, &attr);
  if (status != UCS_OK) {
    return nullptr;
  }
  return reinterpret_cast<char*>(attr.address);
}

bool destroy_rkey_for_region(ucp_context_h ctx, ucp_mem_h ucp_mem, void *ucp_rkey_buf)
{
  // delete rkey and unmap the region
  ucp_rkey_buffer_release(ucp_rkey_buf);
  return (UCS_OK == ucp_mem_unmap(ctx, ucp_mem));
}


} /* ucx::util */


} /* o2::DataDistribution */
