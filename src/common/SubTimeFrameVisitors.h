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

#ifndef ALICEO2_SUBTIMEFRAME_VISITORS_H_
#define ALICEO2_SUBTIMEFRAME_VISITORS_H_

#include "SubTimeFrameDataModel.h"
#include "SubTimeFrameBuilder.h"

#include <Headers/DataHeader.h>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include <discovery.pb.h>
#pragma GCC diagnostic pop

#include <vector>

#include <fairmq/FwdDecls.h>

namespace o2::DataDistribution
{


////////////////////////////////////////////////////////////////////////////////
/// IovSerializer
////////////////////////////////////////////////////////////////////////////////

class IovSerializer : public ISubTimeFrameVisitor
{
 public:

  IovSerializer() = delete;
  IovSerializer(fair::mq::Channel& pChan)
    : mChan(pChan)
  {
    mData.reserve(25600);
  }

  virtual ~IovSerializer() = default;

  void serialize(std::unique_ptr<SubTimeFrame>&& pStf);

  void stop() { mRunning = false; }

 protected:
  void visit(SubTimeFrame& pStf, void*) override;

 private:
  std::atomic_bool mRunning = true;

  std::vector<fair::mq::MessagePtr> mData;

  IovStfHeader mIovHeader;

  fair::mq::Channel& mChan;
};


////////////////////////////////////////////////////////////////////////////////
/// IovDeserializer
////////////////////////////////////////////////////////////////////////////////

class IovDeserializer : public ISubTimeFrameVisitor
{
 public:
  IovDeserializer() = delete;
  IovDeserializer(TimeFrameBuilder &pTfBld)
  : mTfBld(pTfBld) { }
  virtual ~IovDeserializer() = default;

  std::unique_ptr<SubTimeFrame> deserialize(const IovStfHdrMeta &pHdrMeta, std::vector<fair::mq::MessagePtr>& pDataMsgs);

  SubTimeFrame::Header peek_tf_header(const IovStfHdrMeta &pHdrMeta) const;

  bool copy_to_region(std::vector<fair::mq::MessagePtr>& pMsgs /* in/out */);

 protected:
  std::unique_ptr<SubTimeFrame> deserialize_impl();
  void visit(SubTimeFrame& pStf, void*) override;

 private:
  // IovStfHeader mIovHeader;
  IovStfHdrMeta mIovStfHeader;

  std::vector<fair::mq::MessagePtr> mHdrs;
  std::vector<fair::mq::MessagePtr> mData;

  TimeFrameBuilder &mTfBld;
};



} /* o2::DataDistribution */

#endif /* ALICEO2_SUBTIMEFRAME_VISITORS_H_ */
