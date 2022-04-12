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

#include "DataDistLogger.h"
#include "TfBuilderDevice.h"
#include "TfBuilderRpc.h"

#include <SubTimeFrameFileSink.h>
#include <Config.h>
#include <FmqUtilities.h>

#include <options/FairMQProgOptions.h>
#include <fairmq/DeviceRunner.h>
#include <grpc/grpc.h>

using namespace o2::DataDistribution;

int main(int argc, char* argv[])
{
    using namespace fair::mq;
    using namespace fair::mq::hooks;
    namespace bpo = boost::program_options;

    grpc_init();

    // set InfoLogger Facility
    DataDistLogger::sInfoLoggerFacility = "datadist/tfbuilder";

    fair::mq::DeviceRunner runner{argc, argv};

    // Populate options from the command line
    runner.AddHook<fair::mq::hooks::SetCustomCmdLineOptions>([](fair::mq::DeviceRunner& r) {

      // Add InfoLogger Options
      r.fConfig.AddToCmdLineOptions(impl::DataDistLoggerCtx::getProgramOptions());
      // Add Monitoring Options
      r.fConfig.AddToCmdLineOptions(DataDistMonitor::getProgramOptions());

      // TfBuilder options
      bpo::options_description lTfBuilderOptions("TfBuilder options", 120);
      lTfBuilderOptions.add_options()(
        o2::DataDistribution::TfBuilderDevice::OptionKeyStandalone,
        bpo::bool_switch()->default_value(false),
        "Standalone operation. TimeFrames will not be forwarded to other processes.")(
        o2::DataDistribution::TfBuilderDevice::OptionKeyTfDataRegionSize,
        bpo::value<std::uint64_t>()->default_value(1024),
        "Memory buffer (shm region) reserved for building and buffering TimeFrames (in MiB).")(
        o2::DataDistribution::TfBuilderDevice::OptionKeyTfDataRegionId,
        bpo::value<std::uint16_t>()->default_value(std::uint16_t(~0)),
        "Optional shm id for reusing existing TimeFrame regions. (default will create a new region)")(
        o2::DataDistribution::TfBuilderDevice::OptionKeyTfHdrRegionSize,
        bpo::value<std::uint64_t>()->default_value(512),
        "Memory buffer (shm region) reserved for TimeFrame O2 headers (in MiB).")(
        o2::DataDistribution::TfBuilderDevice::OptionKeyTfHdrRegionId,
        bpo::value<std::uint16_t>()->default_value(std::uint16_t(~0)),
        "Optional shm id for reusing existing TimeFrame header regions. (default will create a new region)");

      bpo::options_description lTfBuilderDplOptions("TfBuilder DPL options", 120);
      lTfBuilderDplOptions.add_options()(
        o2::DataDistribution::TfBuilderDevice::OptionKeyDplChannelName,
        bpo::value<std::string>()->default_value(""),
        "Name of the DPL output channel.");

      r.fConfig.AddToCmdLineOptions(lTfBuilderOptions);
      r.fConfig.AddToCmdLineOptions(lTfBuilderDplOptions);

      // Add options for TF file sink
      r.fConfig.AddToCmdLineOptions(o2::DataDistribution::SubTimeFrameFileSink::getProgramOptions());
      // Add options for Data Distribution discovery
      r.fConfig.AddToCmdLineOptions(o2::DataDistribution::Config::getProgramOptions());

    });


    runner.AddHook<InstantiateDevice>([](DeviceRunner& r){

      // Install listener for Logging options
      fmqtools::HandleFMQOptions(r);

      // reset unsupported options
      r.fConfig.SetProperty<int>("io-threads", (int) std::min(std::thread::hardware_concurrency(), 4u));
      r.fConfig.SetProperty<float>("rate", 0.f);
      r.fConfig.SetProperty<bool>("shm-throw-bad-alloc", false);
      r.fConfig.SetProperty<std::string>("transport", std::string("shmem"));
      r.fConfig.Notify();

      // Instantiate the device
      r.fDevice = std::make_unique<o2::DataDistribution::TfBuilderDevice>();
    });

    auto lRet = runner.RunWithExceptionHandlers();

    grpc_shutdown();

    return lRet;
}
