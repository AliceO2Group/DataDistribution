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

#ifndef ALICEO2_DATADIST_CONFIG_H_
#define ALICEO2_DATADIST_CONFIG_H_

#include <options/FairMQProgOptions.h>
#include <tools/Network.h>

#include <boost/program_options/options_description.hpp>

#include <DataDistLogger.h>

#include <string>
#include <map>
#include <cassert>

namespace o2
{
namespace DataDistribution
{

struct ProcessType {
  int mType;

  static const ProcessType StfBuilder;
  static const ProcessType StfSender;
  static const ProcessType TfBuilder;
  static const ProcessType TfSchedulerService;
  static const ProcessType TfSchedulerInstance;

  constexpr ProcessType(int pType = 0) : mType(pType) { }

  ProcessType(const std::string pTypeStr) {
    if (pTypeStr == "StfBuilder")  { *this = ProcessType::StfBuilder; return; }
    if (pTypeStr == "StfSender")   { *this = ProcessType::StfSender; return; }
    if (pTypeStr == "TfBuilder")   { *this = ProcessType::TfBuilder; return; }
    if (pTypeStr == "TfSchedulerService") { *this = ProcessType::TfSchedulerService; return; }
    if (pTypeStr == "TfSchedulerInstance") { *this = ProcessType::TfSchedulerInstance; return; }

    assert(0 && "should not reach");
    mType = -1;
  }

  constexpr operator int() const { return mType; }

  constexpr bool operator==(const ProcessType &b) const { return mType == b.mType; }
  constexpr bool operator!=(const ProcessType &b) const { return mType != b.mType; }
  constexpr bool operator<(const ProcessType &b) const { return mType < b.mType; }

  operator std::string() const {

    if (*this == ProcessType::StfBuilder)
      return "StfBuilder";

    if (*this == ProcessType::StfSender)
      return "StfSender";

    if (*this == ProcessType::TfBuilder)
      return "TfBuilder";

    if (*this == ProcessType::TfSchedulerService)
      return "TfSchedulerService";

    if (*this == ProcessType::TfSchedulerInstance)
      return "TfSchedulerInstance";

    assert(0 && "Should not reach");
    return "UnknownType";
  }
};

constexpr const ProcessType ProcessType::StfBuilder{1};
constexpr const ProcessType ProcessType::StfSender{2};
constexpr const ProcessType ProcessType::TfBuilder{3};
constexpr const ProcessType ProcessType::TfSchedulerService{4};
constexpr const ProcessType ProcessType::TfSchedulerInstance{5};


class Config {
public:

  static constexpr const char* OptionKeyDiscoveryNetInterface = "discovery-net-if";
  static constexpr const char* OptionKeyDiscoveryEndpoint = "discovery-endpoint";

  static constexpr const char* OptionKeyDiscoveryId = "discovery-id";

  static constexpr const char* OptionKeyDiscoveryPartition = "discovery-partition";

  static
  boost::program_options::options_description getProgramOptions(const ProcessType pProcType)
  {
    boost::program_options::options_description lDataDistDiscovery("DataDistribution discovery options", 120);

    lDataDistDiscovery.add_options()(
      OptionKeyDiscoveryNetInterface,
      boost::program_options::value<std::string>()->default_value("ib0"),
      "Specifies network interface for DataDistribution TCP/IP control channels.");

    lDataDistDiscovery.add_options()(
      OptionKeyDiscoveryEndpoint,
      boost::program_options::value<std::string>()->default_value(""),
      "Specifies URL of the DataDistribution discovery endpoint.");

    lDataDistDiscovery.add_options()(
      OptionKeyDiscoveryId,
      boost::program_options::value<std::string>()->default_value(""),
      "Specifies ID of the process for DataDistribution discovery. Unique within (partition, process type).");

    if (pProcType != ProcessType::TfSchedulerService) {
      lDataDistDiscovery.add_options()(
        OptionKeyDiscoveryPartition,
        boost::program_options::value<std::string>()->default_value("testing-partition"),
        "Specifies partition ID for the DataDistribution discovery.");
    }

    return lDataDistDiscovery;
  }

  static
  std::string getNetworkIfAddressOption(const FairMQProgOptions& pFMQProgOpt)
  {
    auto lIf = pFMQProgOpt.GetValue<std::string>(OptionKeyDiscoveryNetInterface);
    if (lIf.empty()) {
      DDLOG(fair::Severity::ERROR) << "Network interface for DataDistribution discovery must be provided.";
      throw std::invalid_argument("Network interface not provided.");
    }

    // check if exists
    std::string lAddr;
    try {
      lAddr = fair::mq::tools::getInterfaceIP(lIf);
    } catch (...) {
      DDLOG(fair::Severity::ERROR) << "Could not determine IP address for network interface " << lIf;
      throw std::invalid_argument("Error while looking up address for interface " + lIf);
    }

    if (lAddr.empty()) {
      DDLOG(fair::Severity::ERROR) << "Could not determine IP address for network interface " << lIf;
      throw std::invalid_argument("Could not find address for interface " + lIf);
    }

    return lAddr;
  }

  static
  std::string getEndpointOption(const FairMQProgOptions& pFMQProgOpt)
  {
    auto lOpt = pFMQProgOpt.GetValue<std::string>(OptionKeyDiscoveryEndpoint);
    if (lOpt.empty()) {
      DDLOG(fair::Severity::ERROR) << "Endpoint for DataDistribution discovery must be provided.";
      DDLOG(fair::Severity::ERROR) << "Connection to local endpoint will be attempted... Not suitable for production!";
    }
    return lOpt;
  }

  static
  std::string getIdOption(const FairMQProgOptions& pFMQProgOpt)
  {
    auto lId = pFMQProgOpt.GetValue<std::string>(OptionKeyDiscoveryId);
    if (lId.empty()) {
      DDLOG(fair::Severity::ERROR) << "Process must have unique ID for DataDistribution discovery.";
      throw std::invalid_argument("Process ID for DataDiscovery must be provided.");
    }
    return lId;
  }

  static
  std::string getPartitionOption(const FairMQProgOptions& pFMQProgOpt)
  {
    return pFMQProgOpt.GetValue<std::string>(OptionKeyDiscoveryPartition);
  }

  Config() = delete;
  explicit Config(const ProcessType pProcessType): mProcessType(pProcessType) {
    // setKV("type", std::string(pProcessType));
  }


  const ProcessType getProcessType() const { return mProcessType; }

private:
  ProcessType mProcessType;
};


}
} /* namespace o2::DataDistribution */

#endif /* ALICEO2_DATADIST_CONFIG_H_ */
