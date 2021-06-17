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

#ifndef ALICEO2_DATADIST_CONFIGCONSUL_H_
#define ALICEO2_DATADIST_CONFIGCONSUL_H_

#include "Config.h"
#include "ConfigParameters.h"

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include "discovery.pb.h"
#pragma GCC diagnostic pop

#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/trim.hpp>

#include <ppconsul/kv.h>
#include <string>
#include <variant>
#include <mutex>

namespace o2
{
namespace DataDistribution
{

namespace ConsulImpl {

std::string to_string(const ProcessTypePB pType);
std::tuple<std::string, std::uint64_t> getCurrentTimeString();

class BasicInfo;
class PartitionInfo;

using ppconsul::kv::Kv;
using ppconsul::kv::KeyValue;

template<typename T>
class ConsulConfig : public Config {

public:

  ConsulConfig() = delete;

  ConsulConfig(const ProcessType pProcessType, const std::string &pEndpoint)
  : Config(pProcessType), mEndpoint(pEndpoint)
  {
    if (mEndpoint.empty()) {
      mEndpoint = "http://127.0.0.1:8500";
    }

    try {
      mConsul = std::make_unique<ppconsul::Consul>(mEndpoint);
      IDDLOG("Connecting to Consul. endpoint={}", mEndpoint);
    } catch (std::exception &err) {
      EDDLOG("Error while connecting to Consul. endpoint={} what={}", mEndpoint, err.what());
    }
  }

  ConsulConfig(const ConsulConfig &pConf)
  : ConsulConfig(pConf.getProcessType(), pConf.mEndpoint)
  { }
  ConsulConfig(ConsulConfig &&) = default;

  virtual ~ConsulConfig() { cleanup(); }

  bool write(bool pIinitial = false)
  {
    if (!createKeyPrefix()) {
      return false;
    }

    auto [lTimeStr, lTimet] = getCurrentTimeString();
    mStatus.mutable_info()->set_last_update(lTimeStr);
    mStatus.mutable_info()->set_last_update_t(lTimet);

    std::string lData;
    mStatus.SerializeToString(&lData);

    // persist global partition info
    if constexpr (std::is_same_v<T, TfSchedulerInstanceConfigStatus>) {
      using namespace std::string_literals;
      static const std::string sPartStateKey = getInfoPrefix(mStatus.partition().partition_id());
      try {
        Kv kv(*mConsul);
        kv.set(sPartStateKey + "/partition-state"s, std::to_string(mStatus.partition_state()) );
        kv.set(sPartStateKey + "/partition-state-str"s, PartitionState_Name(mStatus.partition_state()));
        kv.set(sPartStateKey + "/last-update"s, lTimeStr);
      } catch (std::exception &e) {
        EDDLOG("Consul kv::set error. what={}", e.what());
      }
    }

    return write_string(lData, pIinitial);
  }

  void cleanup()
  {
    if (mConsulKey.empty()) {
      return; // nothing was written
    }

    std::scoped_lock lLock(mConsulLock);

    try {
      Kv kv(*mConsul);
      DDDLOG("Erasing DataDistribution discovery key: {}", mConsulKey);
      kv.eraseAll(mConsulKey);
    } catch (std::exception &e) {
      EDDLOG("Consul kv erase error. what={}", e.what());
      EDDLOG("Unable to cleanup the DataDistribution discovery configuration.");
    }
  }

  T& status() { return mStatus; }

private:

  bool createKeyPrefix();

  bool write_string(const std::string &lData, const bool pInitial = false)
  {
    std::unique_ptr<ppconsul::kv::Kv> kv;

    std::scoped_lock lLock(mConsulLock);

    try {
      kv = std::make_unique<ppconsul::kv::Kv>(*mConsul);
    } catch (std::exception &e) {
      EDDLOG("Consul kv init error. what={}", e.what());
      return false;
    }

    try{
      if (pInitial) {
        // make sure the key does not exist before
        if (kv->count(mConsulKey) > 0) {
          EDDLOG("Consul kv error, the key is already present: {}", mConsulKey);
          return false;
        }
      }

      kv->set(mConsulKey, lData);
    } catch (std::exception &e) {
      EDDLOG("Consul kv set error. what={}", e.what());
      return false;
    }

    return true;
  }


  std::string mEndpoint;

  std::mutex mConsulLock;
  std::unique_ptr<ppconsul::Consul> mConsul;
  std::string mConsulKey;

  T mStatus;

private:

  static
  const std::string getInfoPrefix(const std::string pPartId)
  {
    using namespace std::string_literals;
    return "epn/data-dist/partition/"s + pPartId + "/info"s;
  }

public:
  bool getNewPartitionRequest(PartitionRequest &pNewPartitionRequest)
  {
    using namespace std::string_literals;

    static const std::string sReqKeyPrefix = "epn/data-dist/request"s;
    static const std::string sInvalidKeyPrefix = "epn/data-dist/invalid_requests/"s; // + "time_t"

    static const std::string sTimeSubKey = "/request-time"s;

    static const std::string sPartitionIdSubKey = "/partition-id"s;
    static const std::string sStfSenderListSubKey = "/stf-sender-id-list"s;


    static const std::string sReqPartitionIdKey   = sReqKeyPrefix + sPartitionIdSubKey;
    static const std::string sReqStfSenderListKey = sReqKeyPrefix + sStfSenderListSubKey;

    if constexpr (! std::is_same_v<T, TfSchedulerServiceConfigStatus>) {
      static_assert("Only TfSchedulerService process can call this method.");
    }

    bool lReqValid = false;

    // check for 'epn/data-dist/request/partition-name' key
    try {
      std::scoped_lock lLock(mConsulLock);

      Kv kv(*mConsul);

      std::vector<KeyValue> lReqItems;

      do {
        lReqItems = kv.items(sReqKeyPrefix);

        if (lReqItems.size() == 0) {
          return false;
        }

        if (lReqItems.size() < 2) {
          DDDLOG("Incomplete partition request, retrying...");
          return false;
        }

        if (lReqItems.size() == 2) {
          // get the request fields
          auto lPartitionIdIt = std::find_if(std::begin(lReqItems), std::end(lReqItems),
            [&] (KeyValue const& p) { return p.key == sReqPartitionIdKey; });
          if (lPartitionIdIt == std::end(lReqItems)) {
            EDDLOG("Invalid new partition request. Missing key: {}", sReqPartitionIdKey);
            break;
          }

          auto lFlpIdList = std::find_if(std::begin(lReqItems), std::end(lReqItems),
            [&] (KeyValue const& p) { return p.key == sReqStfSenderListKey; });
          if (lFlpIdList == std::end(lReqItems)) {
            EDDLOG("Invalid new partition request. Missing key: {}", sReqStfSenderListKey);
            break;
          }

          // validate the request fields
          // partition name, check if already exist
          const std::string lPartitionId = boost::trim_copy(lPartitionIdIt->value);
          if (lPartitionId.empty()) {
            EDDLOG("Invalid new partition request. Partition (ID) cannot be empty.");
            break;
          }

          // validate the flp list
          std::vector<std::string> lStfSenderIds;
          const std::string lStfSenderIdsReq = lFlpIdList->value;

          // split, trim, remove empty
          boost::split(lStfSenderIds, lStfSenderIdsReq, boost::is_any_of(";,\n\t\r "), boost::token_compress_on);
          const auto lNumStfSendersReq = lStfSenderIds.size();

          // sort and unique
          std::sort(std::begin(lStfSenderIds), std::end(lStfSenderIds));
          lStfSenderIds.erase( std::unique(std::begin(lStfSenderIds), std::end(lStfSenderIds)), std::end(lStfSenderIds));

          if (lStfSenderIds.empty()) {
            EDDLOG("Invalid new partition request. List of StfSender IDs is empty.");
            break;
          }

          if (lNumStfSendersReq != lStfSenderIds.size()) {
            EDDLOG("Invalid new partition request. Requested FLP IDs are not unique. "
              "provided_num={} unique_num={}", lNumStfSendersReq, lStfSenderIds.size());
            break;
          }

          pNewPartitionRequest.mPartitionId = lPartitionId;
          pNewPartitionRequest.mStfSenderIdList = std::move(lStfSenderIds);

          lReqValid = true;
        }
      } while(false);

      // move the partition request if exists
      if (!lReqItems.empty()) {
        std::string lInfoPrefix;
        auto [lTimeStr, lTimet] = getCurrentTimeString();
        // build info key for valid or invalid partition request
        if (lReqValid) {
          lInfoPrefix = getInfoPrefix(pNewPartitionRequest.mPartitionId);
        } else {
          lInfoPrefix = sInvalidKeyPrefix + std::to_string(lTimet);
        }

        kv.set(lInfoPrefix + sTimeSubKey, lTimeStr);

        // move values and erase original request
        for (const auto &lKeyVal : lReqItems) {
          auto lNewKey = lInfoPrefix + lKeyVal.key.substr(sReqKeyPrefix.length());
          kv.set(lNewKey, lKeyVal.value);
          kv.erase(lKeyVal.key);
        }
      }
    } catch (std::exception &e) {
      EDDLOG("Consul kv partition retrieve error. what={}", e.what());
      EDDLOG("Unable to check for new partition requests.");
    }

    return lReqValid;
  }

  bool getStfSenderConfig(const std::string &pPartId, const std::string &pStfSenderId, StfSenderConfigStatus &pStfSenderStat /*out*/)
  {
    static const std::string sKeyPrefix = "epn/data-dist/partition/";

    const std::string lConsulKey = sKeyPrefix + pPartId + "/StfSender/" + pStfSenderId;

    try {
      std::scoped_lock lLock(mConsulLock);

      Kv kv(*mConsul);

      const auto lReqItem = kv.item(lConsulKey);

      if (!lReqItem.valid()) {
        // does not exist!
        return false;
      }

      if (lReqItem.value.empty()) {
        EDDLOG("Consul: no data returned for key: {}", lConsulKey);
        return false;
      }

      if (!pStfSenderStat.ParseFromString(lReqItem.value)) {
        EDDLOG("Cannot parse protobuf message from consul! (type StfSenderConfigStatus)");
      }

      return true;

    } catch (std::exception &e) {
      EDDLOG("Consul kv StfSender retrieve error. what={}", e.what());
    }
    return false;
  }

  bool getTfBuilderConfig(const std::string &pPartId, const std::string &pTfBuilderId, TfBuilderConfigStatus &pTfBuilderStat /*out*/)
  {
    static const std::string sKeyPrefix = "epn/data-dist/partition/";

    const std::string lConsulKey = sKeyPrefix + pPartId + "/TfBuilder/" + pTfBuilderId;

    try {
      std::scoped_lock lLock(mConsulLock);

      Kv kv(*mConsul);

      const auto lReqItem = kv.item(lConsulKey);

      if (!lReqItem.valid()) {
        EDDLOG("Consul: key does not exist: {}", lConsulKey);
        return false;
      }

      if (lReqItem.value.empty()) {
        EDDLOG("Consul: no data returned for key: {}", lConsulKey);
        return false;
      }

      if (!pTfBuilderStat.ParseFromString(lReqItem.value)) {
        EDDLOG("Cannot parse protobuf message from consul! (type StfSenderConfigStatus)");
      }

      return true;

    } catch (std::exception &e) {
      EDDLOG("Consul kv StfSender retrieve error. what={}", e.what());
    }

    return false;
  }

  bool getTfSchedulerConfig(const std::string &pPartId, TfSchedulerInstanceConfigStatus &pTfSchedulerStat /*out*/)
  {
    static constexpr const char* sKeyPrefix = "epn/data-dist/partition/";

    const std::string lConsulKey = sKeyPrefix + pPartId + "/TfSchedulerInstance";

    // get the scheduler instance with the "smallest" ID
    try {
      std::scoped_lock lLock(mConsulLock);

      Kv kv(*mConsul);

      // get partition state
      using namespace std::string_literals;
      static const std::string sPartStateKey = getInfoPrefix(mStatus.partition().partition_id());
      auto lPartState = kv.item(sPartStateKey + "/partition-state"s);
      if (lPartState.valid() && !lPartState.value.empty()) {
        pTfSchedulerStat.set_partition_state(o2::DataDistribution::PartitionState(stoul(lPartState.value)));
      }

      // get all schedulers in the partition
      auto lReqItems = kv.items(lConsulKey);
      if (lReqItems.empty()) {
        return false;
      }

      // sort lReqItems by SchedulerInstanceId
      std::sort(lReqItems.begin(), lReqItems.end(),  [](auto const& a, auto const& b){
        return a.key < b.key;
      });

      // get the scheduler with lowest ID
      if (!pTfSchedulerStat.ParseFromString(lReqItems.begin()->value)) {
        EDDLOG("Cannot parse protobuf message from consul! (type StfSenderConfigStatus)");
      }

      return true;

    } catch (std::exception &e) {
      EDDLOG("Consul kv StfSender retrieve error. what={}", e.what());
    }
    return false;
  }


};


template <class T>
bool ConsulConfig<T>::createKeyPrefix() {
  // make sure all fields are available
  if (mStatus.partition().partition_id().empty()) {
    return false;
  }

  auto &lBasic = mStatus.info();

  mConsulKey =  "epn/data-dist/partition/" +
                mStatus.partition().partition_id() + "/" +
                to_string(lBasic.type()) + "/" +
                lBasic.process_id();

  return true;
}


template <>
bool ConsulConfig<TfSchedulerServiceConfigStatus>::createKeyPrefix();


} /* namespace ConsulImpl */

///
///  ConsulConfig specializations for o2::DataDistribution
///

// TfSchedulerInstance
using ConsulTfSchedulerInstance = ConsulImpl::ConsulConfig<TfSchedulerInstanceConfigStatus>;

// TfSchedulerService
using ConsulTfSchedulerService = ConsulImpl::ConsulConfig<TfSchedulerServiceConfigStatus>;

// StfSender
using ConsulStfSender = ConsulImpl::ConsulConfig<StfSenderConfigStatus>;

// TfBuilder
using ConsulTfBuilder = ConsulImpl::ConsulConfig<TfBuilderConfigStatus>;


}
} /* namespace o2::DataDistribution */

#endif /* ALICEO2_DATADIST_CONFIGCONSUL_H_ */
