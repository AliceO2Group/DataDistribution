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

#include "RootGui.h"

namespace o2
{
namespace DataDistribution
{

RootGui::RootGui(const std::string& pName, const std::string& pWindowName,
                 const unsigned pW, const unsigned pH)
{
  mApp = std::make_unique<TApplication>(pName.c_str(), nullptr, nullptr);

  mCanvas = std::make_unique<TCanvas>("cnv", pWindowName.c_str(), pW, pH);
  mCanvas->GetCanvas()->SetBit(kNoContextMenu);
}
}
} /* o2::DataDistribution */
