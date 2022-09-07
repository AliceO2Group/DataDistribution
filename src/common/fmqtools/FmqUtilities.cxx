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

#include "FmqUtilities.h"

#include <Config.h>
#include <DataDistLogger.h>
#include <DataDistMonitoring.h>

#include <fairmq/FairMQDevice.h>
#include <fairmq/DeviceRunner.h>

#include<boost/algorithm/string.hpp>

namespace o2::DataDistribution::fmqtools {

static bool checkInfoLoggerOptions() {

  DDDLOG("DataDistLogger: Checking O2_INFOLOGGER_MODE variable");

  const char *cMode = getenv("O2_INFOLOGGER_MODE");

  if (cMode == nullptr) {
      IDDLOG("DataDistLogger: O2_INFOLOGGER_MODE backend is not set.");
      return false;
  }

  const std::string cModeStr = std::string(cMode);

  if (cModeStr.length() == 0) {
      WDDLOG("DataDistLogger: O2_INFOLOGGER_MODE variable is empty.");
      return false;
  }

  if (cModeStr != "infoLoggerD") {
      EDDLOG("DataDistLogger: O2_INFOLOGGER_MODE mode is not supported "
      "(only infoLoggerD mode). O2_INFOLOGGER_MODE={}", cModeStr);
      return false;
  }

  IDDLOG("DataDistLogger: enabling InfoLogger in infoLoggerD mode.");

  return true;
}


static void handleSeverity(const std::string &pOptKey, const std::string &pOptVal)
{
  if (pOptKey == "severity") {
    // we never allow FairMQ console or file backends!
    fair::Logger::SetFileSeverity(DataDistSeverity::nolog);
    fair::Logger::SetConsoleSeverity(fair::Severity::nolog);

    // set the DD log console severity
    if (fair::Logger::fSeverityMap.count(pOptVal)) {
      const auto newLevel = fair::Logger::fSeverityMap.at(pOptVal);
      if (newLevel == fair::Severity::nolog) {
        DataDistLogger::sConfigSeverity = fair::Severity::fatal;
      } else {
        DataDistLogger::sConfigSeverity = newLevel;
      }
    }

  } else if (pOptKey == "severity-infologger") {
    // check the InfoLogger mode. Only infoLoggerD is supported.
    if(!checkInfoLoggerOptions()) {
      WDDLOG("DataDistLogger: Invalid O2_INFOLOGGER_MODE. Ignoring severity-infologger={}",
        pOptVal);
      DataDistLogger::sInfologgerSeverity = DataDistSeverity::nolog;
      DataDistLogger::sInfologgerEnabled = false;
      return;
    }

    // set the InfoLogger log severity
    if (fair::Logger::fSeverityMap.count(pOptVal)) {
      const auto newLevel = fair::Logger::fSeverityMap.at(pOptVal);
      if (newLevel == fair::Severity::nolog) {
        DataDistLogger::sInfologgerSeverity = fair::Severity::fatal;
        DataDistLogger::sInfologgerEnabled = false;
      } else {
        DataDistLogger::sInfologgerSeverity = newLevel;
        DataDistLogger::sInfologgerEnabled = true;
        impl::DataDistLoggerCtx::InitInfoLogger();
      }
    }
  } else if (pOptKey == "severity-file") {
      EDDLOG("DataDistLogger: FMQ File logger is not supported.");
  }
}

static auto handlePartitionId(const std::string &pOptKey, const std::string &pOptVal)
{
  if (pOptKey == "partition_id" || pOptKey == "partition-id" || pOptKey == "environment-id" || pOptKey == "environment_id") {

    if (!DataDistLogger::sPartitionIdStr.empty() && (DataDistLogger::sPartitionIdStr != pOptVal)) {
      WDDLOG_RL(1000, "Partition if is already set. current_value={} new_val={}", DataDistLogger::sPartitionIdStr, pOptVal);
      return;
    }
    DDDLOG("Config::PartitionIdSubscribe received key-value pair. {}=<{}>", pOptKey, pOptVal);
    DataDistLogger::sPartitionIdStr = pOptVal;

    impl::DataDistLoggerCtx::InitInfoLogger();
    DataDistMonitor::enable_datadist(DataDistLogger::sRunNumber, DataDistLogger::sPartitionIdStr);
  }
}

static auto handleRunNumber(const std::string &pOptKey, const std::string &pOptVal)
{
  if (pOptKey == "runNumber") {
    DDDLOG("NEW RUN NUMBER. run_number={}", pOptVal);

    DataDistLogger::sRunNumberStr = pOptVal;
    boost::algorithm::trim(DataDistLogger::sRunNumberStr);
    while (DataDistLogger::sRunNumberStr.size() > 1 && boost::algorithm::starts_with(DataDistLogger::sRunNumberStr, "0") ) {
      boost::algorithm::erase_first(DataDistLogger::sRunNumberStr, "0");
    }

    try {
      DataDistLogger::sRunNumber = boost::lexical_cast<std::uint64_t>(DataDistLogger::sRunNumberStr);
    } catch( boost::bad_lexical_cast const &e) {
      DataDistLogger::sRunNumber = -1;
      EDDLOG("NEW RUN NUMBER is not numeric. run_number_str={} what={}", DataDistLogger::sRunNumberStr, e.what());
    }

    impl::DataDistLoggerCtx::InitInfoLogger();
    DataDistMonitor::enable_datadist(DataDistLogger::sRunNumber, DataDistLogger::sPartitionIdStr);
  }
}

void HandleFMQOptions(fair::mq::DeviceRunner &pFMQRunner)
{
  fair::mq::ProgOptions& lFMQConfig = pFMQRunner.fConfig;

  // disable fairlogger file backend
  lFMQConfig.SetProperty<std::string>("file-severity", "nolog");

  try {
    pFMQRunner.UnsubscribeFromConfigChange();
    // add fake handlers to avoid exception at exit
    lFMQConfig.Subscribe<bool>("device-runner", [](const std::string&, const bool) { });
    lFMQConfig.Subscribe<std::string>("device-runner", [&](const std::string&, const std::string&) {});
  } catch(...) { }

  // subscribe to notifications
  lFMQConfig.Subscribe<std::string>("dd-log-config", handleSeverity);
  lFMQConfig.Subscribe<std::string>("partition-id-config", handlePartitionId);
  lFMQConfig.Subscribe<std::string>("run-number-config", handleRunNumber);

  // read and apply the current value
  handleSeverity("severity", lFMQConfig.GetProperty<std::string>("severity"));

  // read and apply the current value
  handleSeverity("severity-infologger", lFMQConfig.GetProperty<std::string>("severity-infologger"));

  // set the default run number
  handleRunNumber("runNumber", "0");
}

}
