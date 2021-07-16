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

#include "DataDistMonitoring.h"

#include "ConcurrentQueue.h"

#include <Monitoring/MonitoringFactory.h>
#include <Monitoring/Metric.h>

#if defined(__linux__)
#include <unistd.h>
#endif

namespace o2::DataDistribution
{

DataDistMonitoring::DataDistMonitoring(const o2::monitoring::tags::Value pProc, const std::string &pUriList)
{
  using namespace o2::monitoring;

  mSubSystem = pProc;
  mUriList = pUriList;

  if (!mUriList.empty()) {
    mO2Monitoring = o2::monitoring::MonitoringFactory::Get(pUriList);
    mO2Monitoring->addGlobalTag(tags::Key::Subsystem, tags::Value::DataDistribution);
    mO2Monitoring->addGlobalTag(tags::Key::Rolename, mSubSystem);
    mO2Monitoring->enableBuffering(1024);
  }

  mRunning = true;
  mCollectionThread = create_thread_member("metric_c", &DataDistMonitoring::MetricCollectionThread, this);
  mMonitorThread = create_thread_member("metric_s", &DataDistMonitoring::MonitorThread, this);
}

DataDistMonitoring::~DataDistMonitoring()
{
  mRunning = false;
  mMetricsQueue.stop();

  if (mMonitorThread.joinable()) {
    mMonitorThread.join();
  }

  if (mCollectionThread.joinable()) {
    mCollectionThread.join();
  }

  if (mO2Monitoring) {
    mO2Monitoring->flushBuffer();
  }
}

void DataDistMonitoring::MetricCollectionThread()
{
  using namespace o2::monitoring;

  DDDLOG("Starting monitoring collection thread for {}...", mUriList);

  // nice the collection thread to decrease contention with sending threads
#if defined(__linux__)
  auto ignore_unused = [](auto /*param */) {

  };
  ignore_unused(nice(+10));
#endif

  while (mRunning) {
    auto lMetric = mMetricsQueue.pop_wait_for(std::chrono::milliseconds(250));
    if (!mActive || !lMetric) {
      std::scoped_lock lLock(mMetricLock);
      mMetricMap.clear();
      continue;
    }

    const std::string &lMetricName = std::get<0>(lMetric.value());
    const std::string &lKey = std::get<1>(lMetric.value());
    const double lValue = std::get<2>(lMetric.value());

    std::scoped_lock lLock(mMetricLock);

    auto &lMetricObj = mMetricMap[lMetricName];
    lMetricObj.mMetricName = lMetricName;
    // make sure not to overflow if the backend is not working
    if (lMetricObj.mKeyValueVectors[lKey].size() < (size_t(1) << 20)) {
      lMetricObj.mKeyValueVectors[lKey].push_back(lValue);
    }
  }

  DDDLOG("Exiting monitoring thread...");
}

void DataDistMonitoring::MonitorThread()
{
  using namespace o2::monitoring;

  DDDLOG("Starting monitoring sender thread for {}...", mUriList);

  while (mRunning) {
    std::this_thread::sleep_for(std::chrono::milliseconds(mMonitoringIntervalMs));
    if (!mActive) {
      std::scoped_lock lLock(mMetricLock);
      mMetricMap.clear();
      continue;
    }

    {
      std::scoped_lock lLock(mMetricLock);

      if (mMetricMap.empty()) {
        continue;
      }

      for (const auto &lMetricIter : mMetricMap) {
        const DataDistMetric &lMetricObj = lMetricIter.second;

        const std::string &lMetricName = lMetricObj.mMetricName;
        const auto &lKeyValMaps = lMetricObj.mKeyValueVectors;
        const auto &lTimeStamp = lMetricObj.mTimestamp;

        o2::monitoring::Metric lMetric(lMetricName, Metric::DefaultVerbosity, lTimeStamp);

        for (const auto &lKeyValsIter : lKeyValMaps) {

          const auto &lKey = lKeyValsIter.first;
          const auto &lValues = lKeyValsIter.second;

          if (lValues.empty()) {
            continue;
          }

          // create mean, min, max
          const auto lMean = std::accumulate( begin(lValues), end(lValues), 0.0) / lValues.size();
          lMetric.addValue(lMean, lKey);
          const auto lMinMax = std::minmax_element(begin(lValues), end(lValues));
          lMetric.addValue(*lMinMax.first, lKey + "_min");
          lMetric.addValue(*lMinMax.second, lKey + "_max");
        }

        // log
        if (mLogMetric) {
          fmt::memory_buffer lLine;
          fmt::format_to(std::back_inserter(lLine), "Metric={}", lMetric.getName());

          for (const auto &lIter : lMetric.getValues()) {
            const std::string &lName = lIter.first;
            const auto lVal = std::get<double>(lIter.second);
            fmt::format_to(std::back_inserter(lLine), " {}={:.3}", lName, lVal);
          }

          IDDLOG("{}", std::string(lLine.begin(), lLine.end()));
        }

        try {
          if (mO2Monitoring) {
            mO2Monitoring->send(std::move(lMetric));
          }
        } catch (...) {
          EDDLOG("mO2Monitoring exception.");
        }
      }

      mMetricMap.clear();
    }

    try {
      if (mO2Monitoring) {
        mO2Monitoring->flushBuffer();
      }
    } catch (...) {
      EDDLOG("mO2Monitoring flush exception.");
    }
  }

  DDDLOG("Exiting monitoring thread...");
}


std::unique_ptr<DataDistMonitoring> DataDistMonitor::mDataDistMon = nullptr;
std::unique_ptr<DataDistMonitoring> DataDistMonitor::mSchedMon = nullptr;

void DataDistMonitor::start_datadist(const o2::monitoring::tags::Value pProc, const std::string &pDatadistUris)
{
  mDataDistMon = std::make_unique<DataDistMonitoring>(pProc, pDatadistUris);
}
void DataDistMonitor::stop_datadist()
{
  mDataDistMon = nullptr;
}

void DataDistMonitor::start_scheduling(const o2::monitoring::tags::Value pProc, const std::string &pSchedulingUris)
{
  if (!pSchedulingUris.empty()) {
    mSchedMon = std::make_unique<DataDistMonitoring>(pProc, pSchedulingUris);
  }
}
void DataDistMonitor::stop_scheduling()
{
  mSchedMon = nullptr;
}

} /* namespace o2::DataDistribution */
