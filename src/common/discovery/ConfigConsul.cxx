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

#include "ConfigParameters.h"
#include "ConfigConsul.h"

#include <boost/lexical_cast.hpp>
#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/trim.hpp>

#include <ppconsul/kv.h>

#include <string>
#include <chrono>
#include <iomanip>
#include <set>
#include <tuple>


namespace o2::DataDistribution
{

ProcessTypePB to_ProcessTypePB(const ProcessType pType) {
  static const std::map<ProcessType, ProcessTypePB> cProcessTypeMap =
  {
    { ProcessType::StfBuilder, StfBuilder },
    { ProcessType::StfSender, StfSender },
    { ProcessType::TfBuilder, TfBuilder },
    { ProcessType::TfScheduler, TfScheduler }
  };

  return cProcessTypeMap.at(pType);
}

namespace ConsulImpl {

std::string to_string(const ProcessTypePB pType) {

  switch (pType) {
    case StfBuilder: return "StfBuilder";
    case StfSender: return "StfSender";
    case TfBuilder: return "TfBuilder";
    case TfScheduler: return "TfScheduler";
    default: break;
  }
  assert(0 && "Should not reach");
  return std::string("UnknownType");
}

// human readable and milliseconds since epoch
std::tuple<std::string, std::uint64_t> getCurrentTimeString() {
  using namespace std::chrono;
  std::ostringstream ss;
  const auto lNow = system_clock::now();

  // get human readable string
  const auto lTimet = std::chrono::system_clock::to_time_t(lNow);
  ss << std::put_time(gmtime(&lTimet), "%FT%TZ");

  // get milliseconds
  const auto lSinceEpoch = lNow.time_since_epoch();
  const auto lMillis = std::chrono::duration_cast<std::chrono::milliseconds>(lSinceEpoch).count();

  return { ss.str(), lMillis };
}


} /* namespace ConsulImpl */

} /* namespace o2::DataDistribution */
