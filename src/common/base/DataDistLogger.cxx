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

#include "DataDistLogger.h"


namespace o2
{
namespace DataDistribution
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
}
