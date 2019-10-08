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


namespace o2
{
namespace DataDistribution
{

ProcessTypePB to_ProcessTypePB(const ProcessType pType) {
  static const std::map<ProcessType, ProcessTypePB> cProcessTypeMap =
  {
    { ProcessType::StfBuilder, StfBuilder },
    { ProcessType::StfSender, StfSender },
    { ProcessType::TfBuilder, TfBuilder },
    { ProcessType::TfSchedulerService, TfSchedulerService },
    { ProcessType::TfSchedulerInstance, TfSchedulerInstance }
  };

  return cProcessTypeMap.at(pType);
}

namespace ConsulImpl {

std::string to_string(const ProcessTypePB pType) {

  switch (pType) {
    case StfBuilder: return "StfBuilder";
    case StfSender: return "StfSender";
    case TfBuilder: return "TfBuilder";
    case TfSchedulerService: return "TfSchedulerService";
    case TfSchedulerInstance: return "TfSchedulerInstance";
    default: break;
  }
  assert(0 && "Should not reach");
  return std::string("UnknownType");
}

std::tuple<std::string, std::time_t> getCurrentTimeString() {
  using namespace std::chrono;
  std::ostringstream ss;
  const auto lTimet = std::chrono::system_clock::to_time_t(system_clock::now());
  ss << std::put_time(gmtime(&lTimet), "%FT%TZ");
  return { ss.str(), lTimet };
}

template <>
bool ConsulConfig<TfSchedulerServiceConfigStatus>::createKeyPrefix() {
  mConsulKey = "epn/data-dist/TfSchedulerService";
  return true;
}


}

}
}
