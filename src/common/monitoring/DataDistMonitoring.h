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

#ifndef DATADIST_MONITORING_H_
#define DATADIST_MONITORING_H_

#include <Monitoring/Monitoring.h>
#include <Monitoring/Tags.h>
#include <Monitoring/Metric.h>
#include <Monitoring/DerivedMetrics.h>

#include "ConcurrentQueue.h"

#include <boost/program_options.hpp>
#include <memory>
#include <string>
#include <tuple>

namespace o2::DataDistribution
{

struct DataDistMetric {
  std::string                                 mMetricName;
  std::map<std::string, std::vector<double>>  mKeyValueVectors;

  std::chrono::time_point<std::chrono::system_clock> mTimestamp = std::chrono::system_clock::now();
};

struct DataDistRateMetric {
  struct RateVals {
    double mMin = std::numeric_limits<double>::max();
    double mMax = std::numeric_limits<double>::min();
    double mMeanAcc = 0.0;
    double mCount = 0.0;
  };
  std::map<std::string, RateVals>  mKeyValues; // cnt, acc
};


class DataDistMonitoring {
public:
  DataDistMonitoring() = delete;
  DataDistMonitoring(const o2::monitoring::tags::Value pProc, const std::string &pUriList);
  ~DataDistMonitoring();

  inline void push(const std::string_view &pName, const std::string_view &pKey, double pVal) {
    if (mLogMetric || mO2Monitoring) {
      mMetricsQueue.push_capacity(4096, std::make_tuple(pName, pKey, pVal));
    }
  }

  inline void push_rate(const std::string_view &pName, const std::string_view &pKey, double pVal) {
    if (mLogMetric || mO2Monitoring) {
      mRateMetricsQueue.push_capacity(4096, std::make_tuple(pName, pKey, pVal));
    }
  }

  void set_partition_id(const std::string_view &pPartId) {
    if (mO2Monitoring) {
      mO2Monitoring->addGlobalTag("partition", pPartId);
    }
  }
  void set_run_number(const std::uint32_t pRunNum) {
    if (mO2Monitoring) {
      mO2Monitoring->setRunNumber(pRunNum);
    }
  }
  void set_active(bool pActive) { mActive = pActive; }
  void set_interval(const unsigned pIntMs) { mMonitoringIntervalMs = pIntMs; }
  void set_log(bool pLog) { mLogMetric = pLog; }

private:

  std::mutex mMetricLock;
    std::map<std::string, DataDistMetric> mMetricMap;
    std::map<std::string, DataDistRateMetric> mRateMetricMap;
    std::chrono::steady_clock::time_point mRateTimestamp = std::chrono::steady_clock::time_point::min();

  ConcurrentFifo<std::tuple<std::string, std::string, double>>  mMetricsQueue;
  ConcurrentFifo<std::tuple<std::string, std::string, double>>  mRateMetricsQueue;

  void MetricCollectionThread();
  std::thread mCollectionThread;

  void RateMetricCollectionThread();
  std::thread mRateCollectionThread;

  void MonitorThread();
  std::thread mMonitorThread;

  bool mRunning;
  bool mActive = false;

  o2::monitoring::tags::Value mSubSystem;

  std::unique_ptr<o2::monitoring::Monitoring> mO2Monitoring;

  // options
  std::string mUriList;
  unsigned mMonitoringIntervalMs = 1000;
  bool mLogMetric = false;
};


#define DDMON(name, key, val) do {                                    \
  if (DataDistMonitor::mDataDistMon) {                                \
    DataDistMonitor::mDataDistMon->push(name, key, val);              \
  }                                                                   \
} while (0)


#define DDMON_RATE(name, key, val) do {                               \
  if (DataDistMonitor::mDataDistMon) {                                \
    DataDistMonitor::mDataDistMon->push_rate(name, key, val);         \
  }                                                                   \
} while (0)

class DataDistMonitor {
public:
  static void start_datadist(const o2::monitoring::tags::Value pProc, const std::string &pDatadistUris);
  static void stop_datadist();

  static void start_scheduling(const o2::monitoring::tags::Value pProc, const std::string &pSchedulingUris);
  static void stop_scheduling();

  static void enable_datadist(const std::uint32_t pRunNum, const std::string_view &pPartId) {
    if (mDataDistMon) {
      mDataDistMon->set_run_number(std::max(pRunNum, uint32_t(1)));
      mDataDistMon->set_partition_id(pPartId);
      mDataDistMon->set_active(true);
    }
  }

  static void set_interval(const float pInterval) {
    if (mDataDistMon) {
      if (pInterval <= std::numeric_limits<float>::epsilon()) {
        mDataDistMon->set_active(false);
      } else {
        mDataDistMon->set_interval(std::round(double(pInterval) * 1000.0));
      }
    }
  }

  static void set_log(bool pLog) {
    if (mDataDistMon) {
      mDataDistMon->set_log(pLog);
    }
  }

  static std::unique_ptr<DataDistMonitoring> mDataDistMon;
  static std::unique_ptr<DataDistMonitoring> mSchedMon;

public:
  // Monitoring Options
  static boost::program_options::options_description getProgramOptions() {
    namespace bpo = boost::program_options;

    bpo::options_description lMonitoringOpts("Monitoring options", 120);
    lMonitoringOpts.add_options()
      ("monitoring-backend", bpo::value<std::string>()->default_value(""), "Monitoring url.")
      ("monitoring-interval", bpo::value<float>()->default_value(2.0), "Monitoring metric interval (seconds).")
      ("monitoring-log", bpo::bool_switch()->default_value(false), "Log Monitoring metric.");

    return lMonitoringOpts;
  };
};

} /* namespace o2::DataDistribution */

#endif /* DATADIST_MONITORING_H_ */
