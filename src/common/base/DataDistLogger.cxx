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

#include "DataDistLogger.h"


namespace o2::DataDistribution
{

// per thread log name
thread_local char* DataDistLogger::sThisThreadName = nullptr;

// InfoLoggerFacility
std::string DataDistLogger::sInfoLoggerFacility;

std::chrono::steady_clock::time_point
DataDistLogger::sRateLimitLast = std::chrono::steady_clock::now();

volatile bool impl::DataDistLoggerCtx::sRunning = false;
std::thread impl::DataDistLoggerCtx::sRateUpdateThread;
std::thread impl::DataDistLoggerCtx::mInfoLoggerThread;


std::unique_ptr<ConcurrentFifo<std::tuple<AliceO2::InfoLogger::InfoLogger::Severity, std::string>>>
DataDistLogger::sInfoLogQueue = nullptr;


// Keep the desired stdout Severity level
DataDistSeverity DataDistLogger::sConfigSeverity = DataDistSeverity::debug;

// Keep the desired InfoLogger Severity level
bool DataDistLogger::sInfologgerEnabled = false;
DataDistSeverity DataDistLogger::sInfologgerSeverity = DataDistSeverity::nolog;
std::uint64_t DataDistLogger::sRunNumber = 0;
std::string DataDistLogger::sRunNumberStr = "0";
std::string DataDistLogger::sPartitionIdStr = "";


// this is a static object that will interpose early into FairLogger
impl::DataDistLoggerCtx sLoggerCtx;

}
