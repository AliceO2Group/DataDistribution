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

#ifndef ALICEO2_DATADIST_FMQUTILITIES_H_
#define ALICEO2_DATADIST_FMQUTILITIES_H_

#include <fairmq/FairMQDevice.h>
#include <fairmq/DeviceRunner.h>

#include <type_traits>
#include <memory>
#include <thread>
#include <functional>

#include <array>
#include <numeric>
#include "DataDistLogger.h"

namespace o2::DataDistribution::fmqtools {

// react to FMQ program options
static void HandleFMQOptions(fair::mq::DeviceRunner &pFMQRunner) {


  // check the InfoLogger mode. Only infoLoggerD is supported.
  auto checkInfoLoggerOptions = []() -> bool {

    DDDLOG("DataDistLogger: Checking INFOLOGGER_MODE variable");

    const char *cMode = getenv("INFOLOGGER_MODE");

    if (cMode == nullptr) {
      IDDLOG("DataDistLogger: INFOLOGGER_MODE backend is not set.");
      return false;
    }

    const std::string cModeStr = std::string(cMode);

    if (cModeStr.length() == 0) {
      WDDLOG("DataDistLogger: INFOLOGGER_MODE variable is empty.");
      return false;
    }

    if (cModeStr != "infoLoggerD") {
      EDDLOG("DataDistLogger: INFOLOGGER_MODE mode is not supported "
        "(only infoLoggerD mode). INFOLOGGER_MODE={}", cModeStr);
      return false;
    }

    IDDLOG("DataDistLogger: enabling InfoLogger in infoLoggerD mode.");

    return true;
  };

  fair::mq::ProgOptions& lFMQConfig = pFMQRunner.fConfig;

  try {
    pFMQRunner.UnsubscribeFromConfigChange();
  } catch(...) { }

  // disable fairlogger file backend
  lFMQConfig.SetProperty<std::string>("file-severity", "nolog");
  lFMQConfig.SetProperty<std::string>("log-to-file", "");

  auto lSetSeverity = [&](const std::string &pOptKey, const std::string &pOptVal) {
    // we never allow FairMQ console or file backends!
    fair::Logger::SetFileSeverity(DataDistSeverity::nolog);
    fair::Logger::SetConsoleSeverity(fair::Severity::nolog);

    if (pOptKey == "severity") {
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
        WDDLOG("DataDistLogger: Invalid INFOLOGGER_MODE. Ignoring severity-infologger={}",
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
    } else if (pOptKey == "runNumber") {
      IDDLOG("NEW RUN NUMBER. run_number={} fmq_state={}",
        pOptVal, std::string( (pFMQRunner.fDevice) ? pFMQRunner.fDevice->GetCurrentStateName() : "<NO-DEVICE>"));
      DataDistLogger::sRunNumberStr = pOptVal;
      impl::DataDistLoggerCtx::InitInfoLogger();
    }
  };

  // subscribe to notifications
  lFMQConfig.Subscribe<std::string>("dd-log-config", [&](const std::string& key, const std::string& val) {

    try {
      pFMQRunner.UnsubscribeFromConfigChange();
    } catch(...) { }

    lSetSeverity(key, val);
  });

  // read and apply the current value
  lSetSeverity("severity", lFMQConfig.GetProperty<std::string>("severity"));

  // read and apply the current value
  lSetSeverity("severity-infologger", lFMQConfig.GetProperty<std::string>("severity-infologger"));
}

}
#endif // ALICEO2_DATADIST_FMQUTILITIES_H_