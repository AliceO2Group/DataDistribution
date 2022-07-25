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
    mO2Monitoring->enableBuffering(4096);
  }

  mRunning = true;
  mCollectionThread = create_thread_member("metric_c", &DataDistMonitoring::MetricCollectionThread, this);
  mRateCollectionThread = create_thread_member("rate_metric_c", &DataDistMonitoring::RateMetricCollectionThread, this);
  mMonitorThread = create_thread_member("metric_s", &DataDistMonitoring::MonitorThread, this);
}

DataDistMonitoring::~DataDistMonitoring()
{
  mRunning = false;
  mMetricsQueue.stop();
  mRateMetricsQueue.stop();

  if (mMonitorThread.joinable()) {
    mMonitorThread.join();
  }

  if (mCollectionThread.joinable()) {
    mCollectionThread.join();
  }

  if (mRateCollectionThread.joinable()) {
    mRateCollectionThread.join();
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
  if (nice(+10)) {}
#endif

  while (mRunning) {
    auto lMetric = mMetricsQueue.pop_wait_for(std::chrono::milliseconds(250));
    if (!mActive) {
      std::scoped_lock lLock(mMetricLock);
      mMetricMap.clear();
      continue;
    }

    if (!lMetric) {
      continue;
    }

    const std::string &lMetricName = std::get<0>(lMetric.value());
    const std::string &lKey = std::get<1>(lMetric.value());
    const double lValue = std::get<2>(lMetric.value());

    std::scoped_lock lLock(mMetricLock);

    auto &lMetricObj = mMetricMap[lMetricName];
    lMetricObj.mMetricName = lMetricName;
    // make sure not to overflow if the backend is not working
    lMetricObj.mKeyValueVectors[lKey].push_back(lValue);
  }

  DDDLOG("Exiting monitoring thread...");
}

void DataDistMonitoring::RateMetricCollectionThread()
{
  using namespace o2::monitoring;

  DDDLOG("Starting rate monitoring collection thread for {}...", mUriList);

  while (mRunning) {
    auto lMetric = mRateMetricsQueue.pop_wait_for(std::chrono::milliseconds(250));
    if (!lMetric) {
      continue;
    }

    const std::string &lMetricName = std::get<0>(lMetric.value());
    const std::string &lKey = std::get<1>(lMetric.value());
    const double lValue = std::get<2>(lMetric.value());

    std::scoped_lock lLock(mMetricLock);

    auto &lMetricObj = mRateMetricMap[lMetricName];
    auto &lVals = lMetricObj.mKeyValues[lKey];
    lVals.mCount += 1.0;
    lVals.mMeanAcc += lValue;
    lVals.mMax = std::max(lVals.mMax, lValue);
    lVals.mMin = std::min(lVals.mMin, lValue);
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

    // keep the timestamp when last rate was published
    double lRateIntervalS = std::numeric_limits<double>::max();

    // local copy of rate and value maps
    std::map<std::string, DataDistRateMetric> lRateMetricMap;
    std::map<std::string, DataDistMetric> lMetricMap;

    {
      std::scoped_lock lLock(mMetricLock);
      // copy and invalidate original
      lRateMetricMap = mRateMetricMap;
      for (auto &lRateMetricIter : mRateMetricMap) {
        for (auto &lKeyRateAccIter : lRateMetricIter.second.mKeyValues) {
          lKeyRateAccIter.second = DataDistRateMetric::RateVals();
        }
      }

      lRateIntervalS = since(mRateTimestamp);
      mRateTimestamp = std::chrono::steady_clock::now();

      // other metric
      lMetricMap = std::move(mMetricMap);
      mMetricMap.clear();
    }

    for (auto &lRateMetricIter : lRateMetricMap) {
      DataDistRateMetric &lRateMetricObj = lRateMetricIter.second;

      const std::string &lMetricName = lRateMetricIter.first;
      const auto &lKeyAccMap = lRateMetricObj.mKeyValues;

      o2::monitoring::Metric lMetric(lMetricName, Metric::DefaultVerbosity, roundTimeNow());
      for (auto &lKeyRateAccIter : lKeyAccMap) {
        const auto &lKey = lKeyRateAccIter.first;
        const auto &lAccValues = lKeyRateAccIter.second;

        if (lAccValues.mCount < std::numeric_limits<double>::epsilon()) {
          // don't publish size if there were no samples during the monitoring window
          lMetric.addValue(0.0, lKey + ".rate");        // rate
          lMetric.addValue(0.0, lKey + ".throughput");  // thr
        } else {
          lMetric.addValue(lAccValues.mMeanAcc / lAccValues.mCount, lKey + ".size");     // mean
          lMetric.addValue(lAccValues.mMin, lKey + ".size.min");                         // min
          lMetric.addValue(lAccValues.mMax, lKey + ".size.max");                         // max
          lMetric.addValue(lAccValues.mCount / lRateIntervalS, lKey + ".rate");          // rate
          lMetric.addValue(lAccValues.mMeanAcc / lRateIntervalS, lKey + ".throughput");  // thr
        }
      }

      // log
      if (mLogMetric) {
        fmt::memory_buffer lLine;
        fmt::format_to(fmt::appender(lLine), "RateMetric={}", lMetric.getName());

        for (const auto &lIter : lMetric.getValues()) {
          const std::string &lName = lIter.first;
          const auto lVal = std::get<double>(lIter.second);
          fmt::format_to(fmt::appender(lLine), " {}={:.3}", lName, lVal);
        }

        IDDLOG("{}", std::string(lLine.begin(), lLine.end()));
      }

      try {
        if (mO2Monitoring) {
          mO2Monitoring->send(std::move(lMetric));
        }
      } catch (...) {
        EDDLOG_ONCE("mO2Monitoring rate exception.");
      }
    }

    // value metric
    for (auto &lMetricIter : lMetricMap) {
      DataDistMetric &lMetricObj = lMetricIter.second;

      const std::string &lMetricName = lMetricObj.mMetricName;
      auto &lKeyValMaps = lMetricObj.mKeyValueVectors;
      const auto &lTimeStamp = lMetricObj.mTimestamp;

      o2::monitoring::Metric lMetric(lMetricName, Metric::DefaultVerbosity, roundTimeNow(lTimeStamp));

      for (auto &lKeyValsIter : lKeyValMaps) {

        const auto &lKey = lKeyValsIter.first;
        auto &lValues = lKeyValsIter.second;

        if (lValues.empty()) {
          continue;
        }

        // create mean, median, min, max
        const auto [lMin, lMax] = std::minmax_element(lValues.begin(), lValues.end());
        const auto lMean = std::accumulate(lValues.begin(), lValues.end(), 0.0) / double(lValues.size());

        lMetric.addValue(lMean, lKey);
        lMetric.addValue(*lMin, lKey + "_min");
        lMetric.addValue(*lMax, lKey + "_max");
      }

      // log
      if (mLogMetric) {
        fmt::memory_buffer lLine;
        fmt::format_to(fmt::appender(lLine), "Metric={}", lMetric.getName());

        for (const auto &lIter : lMetric.getValues()) {
          const std::string &lName = lIter.first;
          const auto lVal = std::get<double>(lIter.second);
          fmt::format_to(fmt::appender(lLine), " {}={:.3}", lName, lVal);
        }

        IDDLOG("{}", std::string(lLine.begin(), lLine.end()));
      }

      try {
        if (mO2Monitoring) {
          mO2Monitoring->send(std::move(lMetric));
        }
      } catch (...) {
        EDDLOG_ONCE("mO2Monitoring exception.");
      }
    }

    try {
      if (mO2Monitoring) {
        mO2Monitoring->flushBuffer();
      }
    } catch (...) {
      EDDLOG_ONCE("mO2Monitoring flush exception.");
    }
  }

  DDDLOG("Exiting monitoring thread...");
}


std::unique_ptr<DataDistMonitoring> DataDistMonitor::mDataDistMon = nullptr;

void DataDistMonitor::start_datadist(const o2::monitoring::tags::Value pProc, const std::string &pDatadistUris)
{
  mDataDistMon = std::make_unique<DataDistMonitoring>(pProc, pDatadistUris);
}
void DataDistMonitor::stop_datadist()
{
  mDataDistMon = nullptr;
}

} /* namespace o2::DataDistribution */
