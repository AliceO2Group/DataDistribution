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

// Monitor duration of gRPC calls
static constexpr std::string_view DataDistMonitorRpcDurationKey = "DataDistMonitorRpcDuration";
static constexpr bool DataDistMonitorRpcDurationDefault = false;

// Enable the token manager for stf transfers
static constexpr std::string_view DataDistEnableStfTransferTokensKey = "DataDistEnableStfTransferTokens";
static constexpr bool DataDistEnableStfTransferTokensDefault = true;


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

// Time to wait until an STF is claimed by a TfBuilder
static constexpr std::string_view StaleStfTimeoutMsKey = "StaleStfTimeoutMs";
static constexpr std::uint64_t StaleStfTimeoutMsDefault = 60000;

// Standalone: Chance the stf will be deleted on arrival
static constexpr std::string_view StandaloneStfDeleteChanceKey = "StandaloneStfDeleteChance";
static constexpr std::uint64_t StandaloneStfDeleteChanceDefault = 25;

// Standalone: Amount of data to keep while running
static constexpr std::string_view StandaloneStfDataBufferSizeMBKey = "StandaloneStfDataBufferSizeMB";
static constexpr std::uint64_t StandaloneStfDataBufferSizeMBDefault = 128;

/// UCX transport
// Allowed gap between two messages of the same region when creating RMA txgs
static constexpr std::string_view UcxRdmaGapBKey = "UcxRdmaGapB";
static constexpr std::uint64_t UcxRdmaGapBDefault = 8192;

// Size of sender treadpool. Default 0 (number of cpu cores)
static constexpr std::string_view UcxSenderThreadPoolSizeKey = "UcxStfSenderThreadPoolSize";
static constexpr std::uint64_t UcxStfSenderThreadPoolSizeDefault = 8;


////////////////////////////////////////////////////////////////////////////////
/// TfBuilder
////////////////////////////////////////////////////////////////////////////////

// Define maximum number of concurrent STF transfers
// The value should be greater than max number of FLPs.
static constexpr std::string_view MaxNumStfTransfersKey = "MaxNumStfTransfers";
static constexpr std::uint64_t MaxNumStfTransferDefault = 300;

// Stf Request selection method: "random", "linear", "stfsize"
static constexpr std::string_view StfSenderIdxSelectionMethodKey = "StfSenderIdxSelectionMethod";
static constexpr std::string_view StfSenderIdxSelectionMethodDefault = "random";

// Stf Request grpc timeout in milliseconds, after the call is retried
static constexpr std::string_view StfDataRequestGrpcTimeoutMsKey = "StfDataRequestGrpcTimeoutMs";
static constexpr std::uint64_t StfDataRequestGrpcTimeoutMsDefault = 100;

// Stf Request thread pool. Parallelize the grpc calls to all StfSenders,
static constexpr std::string_view StfSenderGrpcThreadPoolSizeKey = "StfSenderGrpcThreadPoolSize";
static constexpr std::uint64_t StfSenderGrpcThreadPoolSizeDefault = 8;


/// UCX transport
// Size of receiver treadpool. Default 1, works best. Should not be set over 2, to avoid congestion on the receiver.
static constexpr std::string_view UcxTfBuilderThreadPoolSizeKey = "UcxTfBuilderThreadPoolSize";
static constexpr std::uint64_t UcxTfBuilderThreadPoolSizeDefault = 1;

// Use polling or blocking waiting method for RDMA completion.
static constexpr std::string_view UcxPollForRDMACompletionKey = "UcxPollForRDMACompletion";
static constexpr bool UcxPollForRDMACompletionDefault = false;

// Allow smaller STFs to be fetched concurrently to improve TF building time
static constexpr std::string_view UcxMaxStfSizeForConcurrentFetchBKey = "UcxMaxStfSizeForConcurrentFetchB";
static constexpr std::uint64_t UcxMaxStfSizeForConcurrentFetchBDefault = std::uint64_t(2) << 20;


////////////////////////////////////////////////////////////////////////////////
/// TfScheduler
////////////////////////////////////////////////////////////////////////////////

// Define maximum number of concurrent TFs in building per TfBuilder
static constexpr std::string_view MaxNumTfsInBuildingKey = "MaxNumTfsInBuilding";
static constexpr std::uint64_t MaxNumTfsInBuildingDevault = 5;

// Decision wether to build or drop incomplete (stale) TFs
static constexpr std::string_view BuildIncompleteTfsKey = "BuildIncompleteTfs";
static constexpr bool BuildIncompleteTfsValue = true;

// An incomplete TF is considered stale when the following timeout expires after the last STF is reported.
static constexpr std::string_view StaleTfTimeoutMsKey = "StaleTfTimeoutMs";
static constexpr std::uint64_t StaleTfTimeoutMsDefault = 750;

// Token reset timeout. All tokens are returned to the scheduler, as a protection from failed EPNs.
static constexpr std::string_view TokenResetTimeoutMsKey = "TokenResetTimeoutMs";
static constexpr std::uint64_t TokenResetTimeoutMsDefault = 5000;

// Number of tokens assigned to each FLP. Increasing the cnt will allow multiple receivers in parallel.
static constexpr std::string_view TokensPerStfSenderCntKey = "TokensPerStfSenderCnt";
static constexpr std::uint64_t TokensPerStfSenderCntDefault = 2;

} /* o2::DataDistribution */

#endif /* DATADIST_OPTIONS_H_ */
