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

#include <spdlog/spdlog.h>
#include <spdlog/async.h>
#include <spdlog/sinks/stdout_color_sinks.h>

#define FAIR_MIN_SEVERITY trace
#include <fairlogger/Logger.h>


namespace o2
{
namespace DataDistribution
{

class DataDistLogger {

  static thread_local char* sThisThreadName;

  auto static I() {
    static std::shared_ptr<spdlog::logger> sTheLogger = nullptr;
    static auto sThreadPool = std::make_shared<spdlog::details::thread_pool>(32768, 1);

    if (!sTheLogger) {
      spdlog::init_thread_pool(32768, 1);
      spdlog::set_level(spdlog::level::debug);

      sTheLogger = spdlog::create_async_nb<spdlog::sinks::stdout_color_sink_mt>("console_nb");
      sTheLogger->flush_on(spdlog::level::warn);
      sTheLogger->set_pattern("[%Y-%m-%d %T.%e][%^%L%$] %v");
    }

    return *(sTheLogger.get());
  }

public:

  static void SetThreadName(const std::string& sThrName) {
    sThisThreadName = strdup(sThrName.c_str());
  }

  template<class... Args>
  DataDistLogger(const fair::Severity pSeverity, Args&&... pArgs)
  : mSeverity(pSeverity) {

    fair::Logger::SetConsoleSeverity(fair::Severity::nolog);

    if (log_enabled()) {
      if (mSeverity >= fair::Severity::debug && sThisThreadName) {
        fmt::format_to(std::back_inserter(mLogMessage), "[{:s}] ", sThisThreadName);
      }

      (fmt::format_to(std::back_inserter(mLogMessage), "{}", pArgs), ...);
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
      fmt::format_to(std::back_inserter(mLogMessage), "{}", pTObj);
    }
    return *this;
  }

  DataDistLogger& operator<<(const char* cstr) {
    if (cstr != NULL) {
      if (log_enabled()) {
        fmt::format_to(std::back_inserter(mLogMessage), "{}", cstr);
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
  if (true) DataDistLogger(severity, fmt::format(__VA_ARGS__))

// Log with streams
#define DDLOG(severity) \
  if(true) DataDistLogger(severity)


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
