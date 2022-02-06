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

#ifndef DATADIST_OPTIONS_H_
#define DATADIST_OPTIONS_H_

#include <string_view>

namespace o2::DataDistribution
{
////////////////////////////////////////////////////////////////////////////////
/// TfScheduler
////////////////////////////////////////////////////////////////////////////////

// Define maximum number of concurent TFs in building per TfBuilder
static constexpr const std::string_view MaxNumTfsInBuildingKey = "MaxNumTfsInBuilding";
static constexpr std::uint64_t MaxNumTfsInBuildingDevault = 25;

// Decision wether to build or drop incomplete (stale) TFs
static const constexpr std::string_view BuildStaleTfsKey = "BuildStaleTfs";
static const constexpr bool BuildStaleTfsValue = true;

// An incomplete TF is considered stale when the following timeout expires after the last
// STF is reported.
static const constexpr std::string_view StaleStfTimeoutMsKey = "StaleTfTimeoutMs";
static const constexpr std::uint64_t StaleStfTimeoutMsValue = 1500;

// Max number of incomplete TFs to keep before considering them stale
static const constexpr std::string_view IncompleteTfsMaxCntKey = "IncompleteTfsMaxCnt";
static const constexpr std::uint64_t IncompleteTfsMaxCntValue = 100;



////////////////////////////////////////////////////////////////////////////////
/// TfBuilder
////////////////////////////////////////////////////////////////////////////////

// Define maximum number of concurent STF transfers
static constexpr const std::string_view MaxNumStfTransfersKey = "MaxNumStfTransfers";
static constexpr std::uint64_t MaxNumStfTransferDefault = 100;


} /* o2::DataDistribution */

#endif /* DATADIST_OPTIONS_H_ */
