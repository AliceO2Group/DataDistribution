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

#ifndef STF_SENDER_OUTPUT_FMQ_H_
#define STF_SENDER_OUTPUT_FMQ_H_

#include "StfSenderOutputDefs.h"

#include <ConfigConsul.h>
#include <ConcurrentQueue.h>

#include <SubTimeFrameVisitors.h>

#include <vector>
#include <map>
#include <thread>

namespace o2::DataDistribution
{

class StfSenderOutputFairMQ {
public:

  StfSenderOutputFairMQ(std::shared_ptr<ConsulStfSender> pDiscoveryConfig, StdSenderOutputCounters &pCounters)
  : mDiscoveryConfig(pDiscoveryConfig),
    mCounters(pCounters)
  {

  }

  void start(std::shared_ptr<fair::mq::TransportFactory> pZMQTransportFactory);
  void stop();

  /// RPC requests
  ConnectStatus connectTfBuilder(const std::string &pTfBuilderId, const std::string &lEndpoint);
  bool disconnectTfBuilder(const std::string &pTfBuilderId, const std::string &lEndpoint);

  bool sendStfToTfBuilder(const std::string &pTfBuilderId, std::unique_ptr<SubTimeFrame> &&pStf);

  void DataHandlerThread(const std::string pTfBuilderId);

private:

  /// Running flag
  std::atomic_bool mRunning = false;

  /// Discovery configuration
  std::shared_ptr<ConsulStfSender> mDiscoveryConfig;
  std::shared_ptr<fair::mq::TransportFactory>  mZMQTransportFactory;

  // Global stf counters
  StdSenderOutputCounters &mCounters;

  /// Threads for output channels (to EPNs)
  struct OutputChannelObjects {
    std::string mTfBuilderEndpoint;
    std::unique_ptr<fair::mq::Channel> mChannel;
    std::unique_ptr<IovSerializer> mStfSerializer;
    std::unique_ptr<ConcurrentFifo<std::unique_ptr<SubTimeFrame>>> mStfQueue;
    std::thread mThread;
  };

  mutable std::mutex mOutputMapLock;
    std::map<std::string, OutputChannelObjects> mOutputMap;
};

} /* namespace o2::DataDistribution */

#endif /* STF_SENDER_OUTPUT_FMQ_H_ */
