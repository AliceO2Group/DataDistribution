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

#ifndef DATADIST_LOGGER_H_
#define DATADIST_LOGGER_H_

#include <type_traits> // enable_if, conjuction
#include "ConcurrentQueue.h"

#include <fmt/format.h>
#include <fmt/core.h>

#include <spdlog/spdlog.h>
#include <spdlog/async.h>
#include <spdlog/sinks/stdout_color_sinks.h>

#include <fairlogger/Logger.h>
#include <fairmq/DeviceRunner.h>
#include <options/FairMQProgOptions.h>

#include <InfoLogger/InfoLogger.hxx>

#include <boost/program_options/options_description.hpp>

namespace o2
{
namespace DataDistribution
{

using DataDistSeverity = fair::Severity;


class DataDistLogger {
  static constexpr std::size_t cInfoLoggerQueueSize = 1024;

public:
  // rate logging updater
  static std::chrono::steady_clock::time_point sRateLimitLast;

  static DataDistSeverity sConfigSeverity;
  static DataDistSeverity sInfologgerSeverity;
  static std::string sInfoLoggerFacility;
  static bool sInfologgerEnabled;

  static spdlog::logger& I() {
    static std::shared_ptr<spdlog::logger> sTheLogger = nullptr;

    if (!sTheLogger) {
      spdlog::init_thread_pool(32768, 1);
      spdlog::set_level(spdlog::level::debug);

      sTheLogger = spdlog::create_async_nb<spdlog::sinks::stdout_color_sink_mt>("console_nb");
      sTheLogger->flush_on(spdlog::level::warn);
      sTheLogger->set_pattern("[%Y-%m-%d %T.%e][%^%L%$] %v");
    }

    return *sTheLogger;
  }

  static inline AliceO2::InfoLogger::InfoLogger& InfoLog() {
    static std::unique_ptr<AliceO2::InfoLogger::InfoLogger> sInfoLogger =
      std::make_unique<AliceO2::InfoLogger::InfoLogger>("outputModeFallback=none");

    return *sInfoLogger;
  }

private:
  static thread_local char* sThisThreadName;

  inline void do_vformat(fmt::string_view format, fmt::format_args args) {
    fmt::vformat_to(mLogMessage, format, args);
  }

public:
  struct log_fmt {}; // tag for fmt logging
  struct log_fmq {}; // tag for logging from fmq

  static void SetThreadName(const std::string& sThrName) {
    sThisThreadName = strdup(sThrName.c_str());
  }

  template<typename... Args>
  DataDistLogger(const DataDistSeverity pSeverity, const log_fmt&, const char* format, const Args&... pArgs)
  : mSeverity(pSeverity) {
    fair::Logger::SetConsoleSeverity(fair::Severity::nolog);

    if (mSeverity <= DataDistSeverity::debug && sThisThreadName) {
      fmt::format_to(mLogMessage, "<{}> ", sThisThreadName);
    }

    do_vformat(format, fmt::make_format_args(pArgs...));
  }

  template<typename... Args>
  DataDistLogger(const DataDistSeverity pSeverity, const log_fmt&, const std::string &format, const Args&... pArgs)
  : DataDistLogger(pSeverity, log_fmt{}, format.c_str(), pArgs...)
  {}

  template<typename... Args>
  DataDistLogger(const DataDistSeverity pSeverity, const log_fmq&, const std::string &format, const Args&... pArgs)
  : DataDistLogger(pSeverity, log_fmt{}, ("[FMQ] " + format).c_str(), pArgs...)
  {}

  template<class... Args>
  DataDistLogger(const DataDistSeverity pSeverity, const Args&... pArgs)
  : mSeverity(pSeverity) {
    fair::Logger::SetConsoleSeverity(fair::Severity::nolog);

    if (mSeverity <= DataDistSeverity::debug && sThisThreadName) {
      fmt::format_to(mLogMessage, "<{}> ", sThisThreadName);
    }

    if constexpr (sizeof...(Args) > 0) {
      static_assert(!std::is_same_v<typename std::tuple_element<0, std::tuple<Args...>>::type, log_fmt>,
        "First parameter to DDLOGF must be format string (const char*).");

      (fmt::format_to(mLogMessage, "{}", pArgs), ...);
    }
  }

  ~DataDistLogger() {
    switch(mSeverity) {
      case DataDistSeverity::fatal:
        if (StdoutEnabled(mSeverity)) {
          I().critical(std::string_view(mLogMessage.begin(), mLogMessage.size()));
        }

        if (InfoLogEnabled(mSeverity)) {
          sInfoLogQueue->push_capacity(cInfoLoggerQueueSize,
            std::make_tuple(AliceO2::InfoLogger::InfoLogger::Severity::Fatal,
            std::string(std::string_view(mLogMessage.begin(), mLogMessage.size()))
          ));
        }
        break;
      case DataDistSeverity::error:
        if (StdoutEnabled(mSeverity)) {
          I().error(std::string_view(mLogMessage.begin(), mLogMessage.size()));
        }

        if (InfoLogEnabled(mSeverity)) {
          sInfoLogQueue->push_capacity(cInfoLoggerQueueSize,
            std::make_tuple(AliceO2::InfoLogger::InfoLogger::Severity::Error,
            std::string(std::string_view(mLogMessage.begin(), mLogMessage.size()))
          ));
        }

        break;
      case DataDistSeverity::warn:
        if (StdoutEnabled(mSeverity)) {
          I().warn(std::string_view(mLogMessage.begin(), mLogMessage.size()));
        }

        if (InfoLogEnabled(mSeverity)) {
          sInfoLogQueue->push_capacity(cInfoLoggerQueueSize,
            std::make_tuple(AliceO2::InfoLogger::InfoLogger::Severity::Warning,
            std::string(std::string_view(mLogMessage.begin(), mLogMessage.size()))
          ));
        }

        break;
      case DataDistSeverity::state:
        if (StdoutEnabled(mSeverity)) {
          I().info("[STATE]" + std::string(mLogMessage.begin(), mLogMessage.end()));
        }

        if (InfoLogEnabled(mSeverity)) {
          sInfoLogQueue->push_capacity(cInfoLoggerQueueSize,
            std::make_tuple(AliceO2::InfoLogger::InfoLogger::Severity::Info,
            std::string(std::string_view(mLogMessage.begin(), mLogMessage.size()))
          ));
        }

        break;
      case DataDistSeverity::info:
        if (StdoutEnabled(mSeverity)) {
          I().info(std::string_view(mLogMessage.begin(), mLogMessage.size()));
        }

        if (InfoLogEnabled(mSeverity)) {
          sInfoLogQueue->push_capacity(cInfoLoggerQueueSize,
            std::make_tuple(AliceO2::InfoLogger::InfoLogger::Severity::Info,
            std::string(std::string_view(mLogMessage.begin(), mLogMessage.size()))
          ));
        }

        break;
      case DataDistSeverity::debug:
        if (StdoutEnabled(mSeverity)) {
          I().debug(std::string_view(mLogMessage.begin(), mLogMessage.size()));
        }

        if (InfoLogEnabled(mSeverity)) {
          sInfoLogQueue->push_capacity(cInfoLoggerQueueSize,
            std::make_tuple(AliceO2::InfoLogger::InfoLogger::Severity::Debug,
            std::string(std::string_view(mLogMessage.begin(), mLogMessage.size()))
          ));
        }

        break;
      case DataDistSeverity::trace:
        if (StdoutEnabled(mSeverity)) {
          I().trace(std::string_view(mLogMessage.begin(), mLogMessage.size()));
        }

        if (InfoLogEnabled(mSeverity)) {
          sInfoLogQueue->push_capacity(cInfoLoggerQueueSize,
            std::make_tuple(AliceO2::InfoLogger::InfoLogger::Severity::Debug,
            std::string(std::string_view(mLogMessage.begin(), mLogMessage.size()))
          ));
        }

        break;

      default:
        if (StdoutEnabled(mSeverity)) {
          I().info("[???] " + std::string(mLogMessage.begin(), mLogMessage.end()));
        }
        break;
    }
  }

  template<typename T>
  DataDistLogger& operator<<(const T& pTObj) {
    fmt::format_to(mLogMessage, "{}", pTObj);
    return *this;
  }

  DataDistLogger& operator<<(const char* cstr) {
    if (cstr != NULL) {
        fmt::format_to(mLogMessage, cstr);
    }
    return *this;
  }

  static inline
  bool LogEnabled(const DataDistSeverity pSevVal) { return (InfoLogEnabled(pSevVal) || StdoutEnabled(pSevVal)); }

  // InfoLogger queue
  static std::unique_ptr<ConcurrentFifo<std::tuple<AliceO2::InfoLogger::InfoLogger::Severity, std::string>>> sInfoLogQueue;

private:

  static inline
  bool InfoLogEnabled(const DataDistSeverity pSevVal) { return sInfologgerEnabled && (pSevVal >= sInfologgerSeverity); }

  static inline
  bool StdoutEnabled(const DataDistSeverity pSevVal) { return pSevVal >= sConfigSeverity; }

  fmt::memory_buffer mLogMessage;
  DataDistSeverity mSeverity;
};

// Log with values
#define DDLOGV(severity, ...) \
  if (DataDistLogger::LogEnabled(severity)) DataDistLogger(severity, __VA_ARGS__)

// Log with fmt
#define DDLOGF(severity, ...) \
  if (DataDistLogger::LogEnabled(severity)) DataDistLogger(severity, DataDistLogger::log_fmt{}, __VA_ARGS__)

// Log with fmt for FMQ messages
#define DDLOGF_FMQ(severity, ...) \
  if (DataDistLogger::LogEnabled(severity)) DataDistLogger(severity, DataDistLogger::log_fmq{}, __VA_ARGS__)

// Log with streams
#define DDLOG(severity) \
  if(DataDistLogger::LogEnabled(severity)) DataDistLogger(severity)


#define DDDLOG(...) DDLOGF(DataDistSeverity::debug, __VA_ARGS__)
#define IDDLOG(...) DDLOGF(DataDistSeverity::info, __VA_ARGS__)
#define WDDLOG(...) DDLOGF(DataDistSeverity::warn, __VA_ARGS__)
#define EDDLOG(...) DDLOGF(DataDistSeverity::error, __VA_ARGS__)

// Log with fmt using ratelimiting (per thread)
#define DDLOGF_RL(intervalMs, severity, ...)                                                                          \
do {                                                                                                                  \
  static thread_local auto sRateLimit__NoShadow = std::chrono::steady_clock::time_point::min();                       \
  static thread_local unsigned sRateLimitCnt__NoShadow = 0;                                                           \
  if (DataDistLogger::LogEnabled(severity) &&                                                                         \
    (sRateLimit__NoShadow + std::chrono::milliseconds(intervalMs)) < DataDistLogger::sRateLimitLast) {                \
    o2::DataDistribution::DataDistLogger(severity, o2::DataDistribution::DataDistLogger::log_fmt{}, __VA_ARGS__) <<   \
      ((sRateLimitCnt__NoShadow > 0) ? fmt::format(" <msgs_suppressed={}>", sRateLimitCnt__NoShadow) : "");           \
    sRateLimit__NoShadow = DataDistLogger::sRateLimitLast;                                                            \
    sRateLimitCnt__NoShadow = 0;                                                                                      \
  } else {                                                                                                            \
    sRateLimitCnt__NoShadow++;                                                                                        \
  }                                                                                                                   \
} while(0)


#define DDDLOG_RL(intervalMs, ...) DDLOGF_RL(intervalMs, DataDistSeverity::debug, __VA_ARGS__)
#define IDDLOG_RL(intervalMs, ...) DDLOGF_RL(intervalMs, DataDistSeverity::info, __VA_ARGS__)
#define WDDLOG_RL(intervalMs, ...) DDLOGF_RL(intervalMs, DataDistSeverity::warn, __VA_ARGS__)
#define EDDLOG_RL(intervalMs, ...) DDLOGF_RL(intervalMs, DataDistSeverity::error, __VA_ARGS__)

// Log with fmt using ratelimiting (global)
#define DDLOGF_GRL(intervalMs, severity, ...)                                                                          \
do {                                                                                                                  \
  static auto sRateLimit__NoShadow = std::chrono::steady_clock::time_point::min();                       \
  static unsigned sRateLimitCnt__NoShadow = 0;                                                           \
  if (DataDistLogger::LogEnabled(severity) &&                                                                         \
    (sRateLimit__NoShadow + std::chrono::milliseconds(intervalMs)) < DataDistLogger::sRateLimitLast) {                \
    o2::DataDistribution::DataDistLogger(severity, o2::DataDistribution::DataDistLogger::log_fmt{}, __VA_ARGS__) <<   \
      ((sRateLimitCnt__NoShadow > 0) ? fmt::format(" <msgs_suppressed={}>", sRateLimitCnt__NoShadow) : "");           \
    sRateLimit__NoShadow = DataDistLogger::sRateLimitLast;                                                            \
    sRateLimitCnt__NoShadow = 0;                                                                                      \
  } else {                                                                                                            \
    sRateLimitCnt__NoShadow++;                                                                                        \
  }                                                                                                                   \
} while(0)


#define DDDLOG_GRL(intervalMs, ...) DDLOGF_GRL(intervalMs, DataDistSeverity::debug, __VA_ARGS__)
#define IDDLOG_GRL(intervalMs, ...) DDLOGF_GRL(intervalMs, DataDistSeverity::info, __VA_ARGS__)
#define WDDLOG_GRL(intervalMs, ...) DDLOGF_GRL(intervalMs, DataDistSeverity::warn, __VA_ARGS__)
#define EDDLOG_GRL(intervalMs, ...) DDLOGF_GRL(intervalMs, DataDistSeverity::error, __VA_ARGS__)

namespace impl {
struct DataDistLoggerCtx {

  static volatile bool sRunning;
  static std::thread sRateUpdateThread;
  static std::thread mInfoLoggerThread;

  DataDistLoggerCtx() {
    if (sRunning) {
      EDDLOG("DataDistLoggerCtx: Already initialized! Static init was already done.");
      return;
    }

    sRunning = true;

    DataDistLogger::sInfoLogQueue =
      std::make_unique<ConcurrentFifo<std::tuple<AliceO2::InfoLogger::InfoLogger::Severity, std::string>>>();

    fair::Logger::SetConsoleSeverity(fair::Severity::nolog);
    fair::Logger::SetFileSeverity(fair::Severity::nolog);
    fair::Logger::AddCustomSink("DDTraceSink",
      DataDistSeverity::trace, [](const std::string& content, const fair::LogMetaData& meta) {
      DDLOGF_FMQ(meta.severity, content);
    });

    sRateUpdateThread = std::thread([&]() {
      while (sRunning) {
        DataDistLogger::sRateLimitLast = std::chrono::steady_clock::now();

        using namespace std::chrono_literals;
        std::this_thread::sleep_for(100ms);
      }
    });

    mInfoLoggerThread = std::thread([&]() {
      std::tuple<AliceO2::InfoLogger::InfoLogger::Severity, std::string> lLogVal;

      while (sRunning) {
        if( DataDistLogger::sInfoLogQueue->pop(lLogVal) ){
          DataDistLogger::InfoLog().log(std::get<0>(lLogVal), "%s", std::get<1>(lLogVal).c_str());
        }
      }
    });
  }

  ~DataDistLoggerCtx() {
    DataDistLogger::sInfoLogQueue->stop();
    sRunning = false;
    // check if the FairLogger is still alive and remove spdlog's sink
    // NOTE: this is tricky, depends on static global variable destruction
    if (! fair::Logger::fIsDestructed) {
      // DDDLOG("Switching the logging back to FairMQLogger.");
      try {
        fair::Logger::RemoveCustomSink("DDTraceSink");
        fair::Logger::SetConsoleSeverity(fair::Severity::trace);
      } catch(...) { }
    }

    if (sRateUpdateThread.joinable()) {
      sRateUpdateThread.join();
    }

    if (mInfoLoggerThread.joinable()) {
      mInfoLoggerThread.join();
    }
  }

  // react to FMQ program options
  static void HandleFMQOptions(fair::mq::DeviceRunner &pFMQRunner) {

    fair::mq::ProgOptions& lFMQConfig = pFMQRunner.fConfig;

    // disable fairlogger file backend
    lFMQConfig.SetProperty<std::string>("file-severity", "nolog");
    lFMQConfig.SetProperty<std::string>("log-to-file", "");

    auto lSetSeverity = [](const std::string &pSevKey, const std::string &pSevVal) {
      // we never allow FairMQ console or file backends!
      fair::Logger::SetFileSeverity(DataDistSeverity::nolog);
      fair::Logger::SetConsoleSeverity(fair::Severity::nolog);

      if (pSevKey == "severity") {
        fair::Logger::SetConsoleSeverity(fair::Severity::nolog);

        // set the DD log console severity
        if (fair::Logger::fSeverityMap.count(pSevVal)) {
          const auto newLevel = fair::Logger::fSeverityMap.at(pSevVal);
          if (newLevel == fair::Severity::nolog) {
            DataDistLogger::sConfigSeverity = fair::Severity::fatal;
          } else {
            DataDistLogger::sConfigSeverity = newLevel;
          }
        }

      } else if (pSevKey == "severity-infologger") {
        // check the InfoLogger mode. Only infoLoggerD is supported.
        if(!checkInfoLoggerOptions()) {
          WDDLOG("DataDistLogger: Invalid INFOLOGGER_MODE. Ignoring severity-infologger={}",
            pSevVal);
          DataDistLogger::sInfologgerSeverity = DataDistSeverity::nolog;
          DataDistLogger::sInfologgerEnabled = false;
          return;
        }

        // set the InfoLogger log severity
        if (fair::Logger::fSeverityMap.count(pSevVal)) {
          const auto newLevel = fair::Logger::fSeverityMap.at(pSevVal);
          if (newLevel == fair::Severity::nolog) {
            DataDistLogger::sInfologgerSeverity = fair::Severity::fatal;
            DataDistLogger::sInfologgerEnabled = false;
          } else {
            DataDistLogger::sInfologgerSeverity = newLevel;
            DataDistLogger::sInfologgerEnabled = true;
            impl::DataDistLoggerCtx::InitInfoLogger();
          }
        }

      } else if (pSevKey == "severity-file") {
          EDDLOG("DataDistLogger: FMQ File logger is not supported.");
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

  // InfoLogger Options
  static boost::program_options::options_description getProgramOptions() {
    namespace bpo = boost::program_options;

    bpo::options_description lInfoLoggerOpts("InfoLogger options", 120);
    // ConfigParamsHelper::populateBoostProgramOptions(optsDesc, spec.options, gHiddenDeviceOptions);
    lInfoLoggerOpts.add_options()
      ("severity-infologger",
      bpo::value<std::string>()->default_value("warning"),
      "Minimum severity to send to InfoLoggerD.");

    return lInfoLoggerOpts;
  };

private:

  // check the InfoLogger mode. Only infoLoggerD is supported.
  static inline bool checkInfoLoggerOptions() {

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
  }

static inline void InitInfoLogger() {
  if (!DataDistLogger::sInfologgerEnabled) {
    return;
  }

  auto &lIlogger = DataDistLogger::InfoLog();

  AliceO2::InfoLogger::InfoLoggerContext lInfoLoggerCtx;
  lInfoLoggerCtx.setField(AliceO2::InfoLogger::InfoLoggerContext::FieldName::Facility,
    DataDistLogger::sInfoLoggerFacility);
  lIlogger.setContext(lInfoLoggerCtx);

  DDDLOG("DataDistLogger: infoLoggerD settings are updated.");
}
};

}
}
} /* o2::DataDistribution */

#endif /* DATADIST_LOGGER_H_ */
