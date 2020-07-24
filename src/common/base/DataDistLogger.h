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

#include <fmt/format.h>
#include <fmt/core.h>

#include <spdlog/spdlog.h>
#include <spdlog/async.h>
#include <spdlog/sinks/stdout_color_sinks.h>

#include <fairlogger/Logger.h>

namespace o2
{
namespace DataDistribution
{

class DataDistLogger {

  static thread_local char* sThisThreadName;

  spdlog::logger static I() {
    static std::shared_ptr<spdlog::logger> sTheLogger = nullptr;

    if (!sTheLogger) {
      spdlog::init_thread_pool(32768, 1);
      spdlog::set_level(spdlog::level::debug);

      sTheLogger = spdlog::create_async_nb<spdlog::sinks::stdout_color_sink_mt>("console_nb");
      sTheLogger->flush_on(spdlog::level::warn);
      sTheLogger->set_pattern("[%Y-%m-%d %T.%e][%^%L%$] %v");
    }

    return *(sTheLogger.get());
  }

  inline void do_vformat(fmt::string_view format, fmt::format_args args) {
    fmt::vformat_to(mLogMessage, format, args);
  }

public:
  struct log_fmt {}; // tag for fmt logging

  static void SetThreadName(const std::string& sThrName) {
    sThisThreadName = strdup(sThrName.c_str());
  }

  template<typename... Args>
  DataDistLogger(const fair::Severity pSeverity, const log_fmt&, const char* format, const Args&... pArgs)
  : mSeverity(pSeverity) {
    fair::Logger::SetConsoleSeverity(fair::Severity::nolog);

    if (log_enabled()) {
      if (mSeverity <= fair::Severity::debug && sThisThreadName) {
        fmt::format_to(mLogMessage, "<{:s}> ", sThisThreadName);
      }

      do_vformat(format, fmt::make_format_args(pArgs...));
    }
  }

  template<typename... Args>
  DataDistLogger(const fair::Severity pSeverity, const log_fmt&, const std::string &format, const Args&... pArgs)
  : DataDistLogger(pSeverity, log_fmt{}, format.c_str(), pArgs...)
  {}

  template<class... Args>
  DataDistLogger(const fair::Severity pSeverity, const Args&... pArgs)
  : mSeverity(pSeverity) {

    fair::Logger::SetConsoleSeverity(fair::Severity::nolog);

    if (log_enabled()) {
      if (mSeverity <= fair::Severity::debug && sThisThreadName) {
        fmt::format_to(std::back_inserter(mLogMessage), "[{:s}] ", sThisThreadName);
      }

      if constexpr (sizeof...(Args) > 0) {
        static_assert(!std::is_same_v<typename std::tuple_element<0, std::tuple<Args...>>::type, log_fmt>,
          "First parameter to DDLOGF must be format string (const char*).");

        (fmt::format_to(mLogMessage, "{}", pArgs), ...);
      }
    }
  }

  ~DataDistLogger() {
    switch(mSeverity) {
      case fair::Severity::fatal:
        I().critical(std::string_view(mLogMessage.begin(), mLogMessage.size()));
        break;
      case fair::Severity::error:
        I().error(std::string_view(mLogMessage.begin(), mLogMessage.size()));
        break;
      case fair::Severity::warn:
        I().warn(std::string_view(mLogMessage.begin(), mLogMessage.size()));
        break;
      case fair::Severity::state:
        I().info("[STATE] " + std::string(mLogMessage.begin(), mLogMessage.end()));
        break;
      case fair::Severity::info:
        I().info(std::string_view(mLogMessage.begin(), mLogMessage.size()));
        break;
      case fair::Severity::debug:
        I().debug(std::string_view(mLogMessage.begin(), mLogMessage.size()));
        break;
      case fair::Severity::trace:
        I().trace(std::string_view(mLogMessage.begin(), mLogMessage.size()));
        break;

      default:
        I().info("[???] " + std::string(mLogMessage.begin(), mLogMessage.end()));
        break;
    }
  }

  template<typename T>
  DataDistLogger& operator<<(const T& pTObj) {
    if (log_enabled()) {
      fmt::format_to(mLogMessage, "{}", pTObj);
    }
    return *this;
  }

  DataDistLogger& operator<<(const char* cstr) {
    if (cstr != NULL) {
      if (log_enabled()) {
        fmt::format_to(mLogMessage, cstr);
      }
    }
    return *this;
  }

private:
  // std::ostringstream mLogMessage;
  fmt::memory_buffer mLogMessage;
  fair::Severity mSeverity;

  // TODO: Implement verbosity knob
  bool log_enabled() const {
    return true;
  }
};

// Log with values
#define DDLOGV(severity, ...) \
  if (true) DataDistLogger(severity, __VA_ARGS__)

// Log with fmt
#define DDLOGF(severity, ...) \
  if (true) DataDistLogger(severity, DataDistLogger::log_fmt{}, __VA_ARGS__)


// Log with streams
#define DDLOG(severity) \
  if(true) DataDistLogger(severity)


// Log with fmt using ratelimiting
#define DDLOGF_RL(intervalMs, severity, ...)                                                                          \
do {                                                                                                                  \
  static thread_local auto sRateLimit__NoShadow = std::chrono::steady_clock::time_point::min();                       \
  static thread_local unsigned sRateLimitCnt__NoShadow = 0;                                                           \
  std::chrono::steady_clock::time_point lNowLoggerTime__NoShadow;                                                     \
  if ((sRateLimit__NoShadow + std::chrono::milliseconds(intervalMs)) <                                                \
      (lNowLoggerTime__NoShadow = std::chrono::steady_clock::now())) {                                                \
    o2::DataDistribution::DataDistLogger(severity, o2::DataDistribution::DataDistLogger::log_fmt{}, __VA_ARGS__) <<   \
      ((sRateLimitCnt__NoShadow > 0) ? fmt::format(" <msgs_suppressed={}>", sRateLimitCnt__NoShadow) : "");           \
    sRateLimit__NoShadow = lNowLoggerTime__NoShadow;                                                                  \
    sRateLimitCnt__NoShadow = 0;                                                                                      \
  } else {                                                                                                            \
    sRateLimitCnt__NoShadow++;                                                                                        \
  }                                                                                                                   \
} while(0)

namespace impl {
  struct DataDistLoggerCtx {
    DataDistLoggerCtx() {
      // DDLOGF(fair::Severity::DEBUG, "Switching the logging to spdlog's nonblocking async backend.");

      fair::Logger::SetConsoleSeverity(fair::Severity::nolog);

      fair::Logger::AddCustomSink("TraceSink", fair::Severity::trace, [](const std::string& content, const fair::LogMetaData& meta)
      {
        DataDistLogger(meta.severity, content);
      });
    }

    ~DataDistLoggerCtx() {
      // check if the FairLogger is still alive and remove spdlog's sink
      // NOTE: this is tricky, depends on static global variable destruction
      if (! fair::Logger::fIsDestructed) {
        // DDLOGF(fair::Severity::DEBUG, "Switching the logging back to FairMQLogger.");
        fair::Logger::RemoveCustomSink("TraceSink");
        fair::Logger::SetConsoleSeverity(fair::Severity::trace);
      }
    }
};
}



}
} /* o2::DataDistribution */

#endif /* DATADIST_LOGGER_H_ */
