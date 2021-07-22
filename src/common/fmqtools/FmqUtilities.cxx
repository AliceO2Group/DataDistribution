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

#include "FmqUtilities.h"
#include <DataDistLogger.h>
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

static auto handleRunNumber(const std::string &pOptKey, const std::string &pOptVal)
{
  if (pOptKey == "runNumber") {
    IDDLOG("NEW RUN NUMBER. run_number={}", pOptVal);

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
  lFMQConfig.Subscribe<std::string>("run-number-config", handleRunNumber);

  // read and apply the current value
  handleSeverity("severity", lFMQConfig.GetProperty<std::string>("severity"));

  // read and apply the current value
  handleSeverity("severity-infologger", lFMQConfig.GetProperty<std::string>("severity-infologger"));

  // set a degfault run number
  handleRunNumber("runNumber", "1");
}

}
