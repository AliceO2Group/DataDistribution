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

#ifndef ALICEO2_DATADIST_CONFIG_PARAMETERS_H_
#define ALICEO2_DATADIST_CONFIG_PARAMETERS_H_

#include <string>
#include <vector>
#include <cassert>

namespace o2
{
namespace DataDistribution
{

enum TfBuilderState {
  Initialized,
  Connecting,
  Connected,
  Running
};

inline
std::string to_string(const TfBuilderState s) {
  switch (s) {
    case Initialized: return "initialized";
    case Connecting: return "connecting";
    case Connected: return "connected";
    case Running: return "running";
    default: return "unknown";
  }
}

struct PartitionRequest {
  std::string   mPartitionId;
  std::vector<std::string> mStfSenderIdList;
};


}
} /* namespace o2::DataDistribution */

#endif /* ALICEO2_DATADIST_CONFIG_PARAMETERS_H_ */
