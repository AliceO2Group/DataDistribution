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

#ifndef DATADIST_OPTIONS_H_
#define DATADIST_OPTIONS_H_

#include <string_view>

namespace o2::DataDistribution
{

////////////////////////////////////////////////////////////////////////////////
/// Global DataDistribution
/// NOTE: the global key must start with "DataDist"
////////////////////////////////////////////////////////////////////////////////

// Select transport to use: "ucx" or "fmq"
static constexpr std::string_view DataDistNetworkTransportKey = "DataDistNetworkTransport";
static constexpr std::string_view DataDistNetworkTransportDefault = "ucx";


////////////////////////////////////////////////////////////////////////////////
/// StfBuilder
////////////////////////////////////////////////////////////////////////////////

// Page aggregation for topological runs. Larger number of pages decreases FLP-EPN interaction rate (better performance)
static constexpr std::string_view NumPagesInTopologicalStfKey = "NumPagesInTopologicalStf";
static constexpr std::uint64_t NumPagesInTopologicalStfDefault = 128;
// Topological STFs: Force starting STFs on new Orbit only
static constexpr std::string_view StartTopologicalStfOnNewOrbitfKey = "StartTopologicalStfOnNewOrbit";
static constexpr bool StartTopologicalStfOnNewOrbitDefault = true;



////////////////////////////////////////////////////////////////////////////////
/// StfSender
////////////////////////////////////////////////////////////////////////////////

// Define size of DataDist buffer
static constexpr std::string_view StfBufferSizeMBKey = "StfBufferSizeMB";
static constexpr std::uint64_t StfBufferSizeMBDefault = (32ULL << 10);

// Standalone: Chance the stf will be deleted on arrival
static constexpr std::string_view StandaloneStfDeleteChanceKey = "StandaloneStfDeleteChance";
static constexpr std::uint64_t StandaloneStfDeleteChanceDefault = 50;

// Standalone: Amount of data to keep while running
static constexpr std::string_view StandaloneStfDataBufferSizeMBKey = "StandaloneStfDataBufferSizeMB";
static constexpr std::uint64_t StandaloneStfDataBufferSizeMBDefault = StfBufferSizeMBDefault;

/// UCX transport
// Allowed gap between two messages of the same region when creating RMA txgs
static constexpr std::string_view UcxRdmaGapBKey = "UcxRdmaGapB";
static constexpr std::uint64_t UcxRdmaGapBDefault = 8192;

// Size of sender treadpool. Default 0 (number of cpu cores)
static constexpr std::string_view UcxSenderThreadPoolSizeKey = "UcxStfSenderThreadPoolSize";
static constexpr std::uint64_t UcxStfSenderThreadPoolSizeDefault = 0;


////////////////////////////////////////////////////////////////////////////////
/// TfBuilder
////////////////////////////////////////////////////////////////////////////////

// Define maximum number of concurrent STF transfers
static constexpr std::string_view MaxNumStfTransfersKey = "MaxNumStfTransfers";
static constexpr std::uint64_t MaxNumStfTransferDefault = 100;


/// UCX transport
// Size of receiver treadpool. Default 0 (number of cpu cores)
static constexpr std::string_view UcxTfBuilderThreadPoolSizeKey = "UcxTfBuilderThreadPoolSize";
static constexpr std::uint64_t UcxTfBuilderThreadPoolSizeDefault = 0;

// Number of rma_get operation in flight, per ucx thread
static constexpr std::string_view UcxNumConcurrentRmaGetOpsKey = "UcxNumConcurrentRmaGetOps";
static constexpr std::uint64_t UcxNumConcurrentRmaGetOpsDefault = 8;


////////////////////////////////////////////////////////////////////////////////
/// TfScheduler
////////////////////////////////////////////////////////////////////////////////

// Define maximum number of concurrent TFs in building per TfBuilder
static constexpr std::string_view MaxNumTfsInBuildingKey = "MaxNumTfsInBuilding";
static constexpr std::uint64_t MaxNumTfsInBuildingDevault = 25;

// Decision wether to build or drop incomplete (stale) TFs
static constexpr std::string_view BuildIncompleteTfsKey = "BuildIncompleteTfs";
static constexpr bool BuildIncompleteTfsValue = true;

// An incomplete TF is considered stale when the following timeout expires after the last STF is reported.
static constexpr std::string_view StaleStfTimeoutMsKey = "StaleTfTimeoutMs";
static constexpr std::uint64_t StaleStfTimeoutMsValue = 1000;

// Max number of incomplete TFs to keep before considering them stale
static constexpr std::string_view IncompleteTfsMaxCntKey = "IncompleteTfsMaxCnt";
static constexpr std::uint64_t IncompleteTfsMaxCntValue = 100;


} /* o2::DataDistribution */

#endif /* DATADIST_OPTIONS_H_ */
