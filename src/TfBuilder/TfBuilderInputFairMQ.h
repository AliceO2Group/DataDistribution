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

#ifndef TF_BUILDER_INPUT_FAIRMQ_H_
#define TF_BUILDER_INPUT_FAIRMQ_H_

#include "TfBuilderInputDefs.h"

#include <ConfigConsul.h>
#include <discovery.pb.h>

#include <SubTimeFrameDataModel.h>
#include <ConcurrentQueue.h>

#include <vector>
#include <map>

#include <thread>

namespace o2::DataDistribution
{

class TimeFrameBuilder;
class TfBuilderRpcImpl;

class TfBuilderInputFairMQ
{
 public:
  TfBuilderInputFairMQ() = delete;
  TfBuilderInputFairMQ(std::shared_ptr<TfBuilderRpcImpl> pRpc, TimeFrameBuilder &pTfBldr,
                        ConcurrentQueue<std::string> &pStfReqQueue, ConcurrentQueue<ReceivedStfMeta> &pStfMetaQueue)
    : mRpc(pRpc),
      mTimeFrameBuilder(pTfBldr),
      mStfReqQueue(pStfReqQueue),
      mReceivedDataQueue(pStfMetaQueue)
  {
    mStfReqQueue.stop(); // not used in FMQ transport
  }

  bool start(std::shared_ptr<ConsulTfBuilder> pConfig, std::shared_ptr<fair::mq::TransportFactory> pZMQTransportFactory);
  void stop(std::shared_ptr<ConsulTfBuilder> pConfig);
  void reset() { }

  void DataHandlerThread(const std::uint32_t pFlpIndex, const std::string pStfSenderId);

 private:
  std::atomic<InputRunState> mState = CONFIGURING;

  /// RPC service
  std::shared_ptr<TfBuilderRpcImpl> mRpc;

  /// TimeFrameBuilder
  TimeFrameBuilder &mTimeFrameBuilder;

  // Partition info
  std::uint32_t mNumStfSenders = 0;

  /// StfBuilder channels
  std::vector<std::unique_ptr<fair::mq::Channel>> mStfSenderChannels;

  /// Threads for input channels (per FLP)
  std::map<std::string, std::thread> mInputThreads;

  /// Queue for received STFs
  ConcurrentQueue<std::string> &mStfReqQueue;
  ConcurrentQueue<ReceivedStfMeta> &mReceivedDataQueue;
};

} /* namespace o2::DataDistribution */

#endif /* TF_BUILDER_INPUT_FAIRMQ_H_ */
