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

#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/asio/ip/host_name.hpp>

namespace o2::DataDistribution
{

struct ProcessType {
  int mType;

  static const ProcessType StfBuilder;
  static const ProcessType StfSender;
  static const ProcessType TfBuilder;
  static const ProcessType TfScheduler;

  constexpr ProcessType(int pType = 0) : mType(pType) { }

  ProcessType(const std::string pTypeStr) {
    if (pTypeStr == "StfBuilder")  { *this = ProcessType::StfBuilder; return; }
    if (pTypeStr == "StfSender")   { *this = ProcessType::StfSender; return; }
    if (pTypeStr == "TfBuilder")   { *this = ProcessType::TfBuilder; return; }
    if (pTypeStr == "TfScheduler") { *this = ProcessType::TfScheduler; return; }

    assert(0 && "should not reach");
    mType = -1;
  }

  // constexpr operator int() const { return mType; }

  constexpr bool operator==(const ProcessType &b) const { return mType == b.mType; }
  constexpr bool operator!=(const ProcessType &b) const { return mType != b.mType; }
  constexpr bool operator<(const ProcessType &b) const { return mType < b.mType; }

  std::string to_string() const {

    if (*this == ProcessType::StfBuilder)
      return "StfBuilder";

    if (*this == ProcessType::StfSender)
      return "StfSender";

    if (*this == ProcessType::TfBuilder)
      return "TfBuilder";

    if (*this == ProcessType::TfScheduler)
      return "TfScheduler";

    assert(0 && "Should not reach");
    return "UnknownType";
  }

  constexpr operator int() const { return mType; }
};

constexpr const ProcessType ProcessType::StfBuilder{1};
constexpr const ProcessType ProcessType::StfSender{2};
constexpr const ProcessType ProcessType::TfBuilder{3};
constexpr const ProcessType ProcessType::TfScheduler{4};


class Config {
public:

  static constexpr const char* OptionKeyDiscoveryNetInterface = "discovery-net-if";
  static constexpr const char* OptionKeyDiscoveryEndpoint = "discovery-endpoint";
  static constexpr const char* OptionKeyDiscoveryId = "discovery-id";
  static constexpr const char* OptionKeyDiscoveryPartition = "discovery-partition";

  static
  boost::program_options::options_description getProgramOptions()
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

    lDataDistDiscovery.add_options()(
      OptionKeyDiscoveryPartition,
      boost::program_options::value<std::string>()->default_value(""),
      "Specifies partition ID for the DataDistribution discovery.");

    return lDataDistDiscovery;
  }

  static
  boost::program_options::options_description getProgramOptionsStfBuilder()
  {
    boost::program_options::options_description lDataDistDiscovery("DataDistribution discovery options", 120);

    lDataDistDiscovery.add_options()(
      OptionKeyDiscoveryEndpoint,
      boost::program_options::value<std::string>()->default_value(""),
      "Specifies URL of the DataDistribution discovery endpoint.");

    lDataDistDiscovery.add_options()(
      OptionKeyDiscoveryPartition,
      boost::program_options::value<std::string>()->default_value(""),
      "Specifies partition ID for the DataDistribution discovery.");

    return lDataDistDiscovery;
  }

  static
  std::string getNetworkIfAddressOption(const FairMQProgOptions& pFMQProgOpt)
  {
    const std::string lIf = pFMQProgOpt.GetValue<std::string>(OptionKeyDiscoveryNetInterface);
    if (lIf.empty()) {
      EDDLOG("Network interface for DataDistribution discovery must be provided.");
      throw std::invalid_argument("Network interface not provided.");
    }

    // check if exists
    std::string lAddr;
    try {
      lAddr = fair::mq::tools::getInterfaceIP(lIf);
    } catch (...) {
      EDDLOG("Could not determine IP address for network interface. iface={}", lIf);
      throw std::invalid_argument("Error while looking up address for interface " + lIf);
    }

    if (lAddr.empty()) {
      EDDLOG("Could not determine IP address for network interface. iface={}", lIf);
      throw std::invalid_argument("Could not find address for interface " + lIf);
    }

    return lAddr;
  }

  static
  std::string getEndpointOption(const FairMQProgOptions& pFMQProgOpt)
  {
    auto lOpt = pFMQProgOpt.GetValue<std::string>(OptionKeyDiscoveryEndpoint);
    return lOpt;
  }

  static
  std::string getIdOption(const ProcessType pProcType, const FairMQProgOptions& pFMQProgOpt, const bool pRequired = true)
  {
    // check cmdline first
    {
      std::string lId = pFMQProgOpt.GetValue<std::string>(OptionKeyDiscoveryId);
      if (!lId.empty()) {
        IDDLOG("Parameter <{}> provided on command line. value={}", OptionKeyDiscoveryId, lId);
        return lId;
      }

      if (pRequired && (pProcType == ProcessType::StfSender)) {
        const auto lErrorMsg = fmt::format("Parameter '{}' for StfSender must be provided on command line.",
          OptionKeyDiscoveryId);
        EDDLOG(lErrorMsg);
        throw std::invalid_argument(lErrorMsg);
      }
    }

    {
      // get an unique ID
      const std::string lUniquePart = boost::lexical_cast<std::string>(boost::uuids::random_generator()());
      std::string lHostname = boost::asio::ip::host_name();
      lHostname = lHostname.substr(0, lHostname.find('.'));

      std::string lUniqueId = pProcType.to_string();
      lUniqueId += "-" + lHostname;
      lUniqueId += "-" + lUniquePart.substr(0, 8);

      IDDLOG("Parameter {} not provided, using random value={}", Config::OptionKeyDiscoveryId, lUniqueId);
      return lUniqueId;
    }
  }

  static
  std::optional<std::string> getPartitionOption(const FairMQProgOptions& pFMQProgOpt)
  {
    // check ecs value first
    if (!DataDistLogger::sPartitionIdStr.empty()) {
      DDDLOG("Parameter '{}' provided from ECS. value={}", OptionKeyDiscoveryPartition, DataDistLogger::sPartitionIdStr);
      return DataDistLogger::sPartitionIdStr;
    }

    // check the command line
    const std::string lPartId = pFMQProgOpt.GetValue<std::string>(OptionKeyDiscoveryPartition);
    if (!lPartId.empty()) {
      DDDLOG("Parameter '{}' provided on the command line. value={}", OptionKeyDiscoveryPartition, lPartId);
      DataDistLogger::sPartitionIdStr = lPartId;
      impl::DataDistLoggerCtx::InitInfoLogger();
      return lPartId;
    }

    DDDLOG("Parameter '{}' is not provided", OptionKeyDiscoveryPartition);
    return std::nullopt;
  }

  Config() = delete;
  explicit Config(const ProcessType pProcessType): mProcessType(pProcessType) {
    // setKV("type", std::string(pProcessType));
  }

  const ProcessType getProcessType() const { return mProcessType; }

private:
  ProcessType mProcessType;
};


} /* namespace o2::DataDistribution */

#endif /* ALICEO2_DATADIST_CONFIG_H_ */
