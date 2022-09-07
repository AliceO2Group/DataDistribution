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

#ifndef ALICEO2_DATADIST_FMQUTILITIES_H_
#define ALICEO2_DATADIST_FMQUTILITIES_H_

#include <fairmq/FairMQDevice.h>
#include <fairmq/DeviceRunner.h>

#include <thread>

namespace o2::DataDistribution {

class DataDistDevice : public FairMQDevice {

public:

  void WaitForRunningState() const {
    while (GetCurrentState() < fair::mq::State::Running) {
      using namespace std::chrono_literals;
      std::this_thread::sleep_for(20ms);
    }
  }

  bool IsRunningState() const {
    return (GetCurrentState() == fair::mq::State::Running);
  }

  bool IsReadyOrRunningState() const {
    return ((GetCurrentState() == fair::mq::State::Running) || (GetCurrentState() == fair::mq::State::Ready));
  }
};


namespace fmqtools {

// react to FMQ program options
void HandleFMQOptions(fair::mq::DeviceRunner &pFMQRunner);


}

}
#endif // ALICEO2_DATADIST_FMQUTILITIES_H_