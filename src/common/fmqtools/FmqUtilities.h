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

#ifndef ALICEO2_DATADIST_FMQUTILITIES_H_
#define ALICEO2_DATADIST_FMQUTILITIES_H_

#include <fairmq/Device.h>
#include <fairmq/DeviceRunner.h>

#include <thread>

namespace o2::DataDistribution {

class DataDistDevice : public fair::mq::Device {

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
