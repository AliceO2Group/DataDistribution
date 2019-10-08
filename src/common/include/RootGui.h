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

#ifndef DATADIST_ROOTGUI_H_
#define DATADIST_ROOTGUI_H_

#include <TApplication.h>
#include <TCanvas.h>
#include <TH1.h>

#include <memory>
#include <cmath>

namespace o2
{
namespace DataDistribution
{

class RootGui
{
 private:
  std::unique_ptr<TApplication> mApp;
  std::unique_ptr<TCanvas> mCanvas;

 public:
  RootGui(const std::string& pName,
          const std::string& pWindowName,
          const unsigned pW, const unsigned pH);

  auto& App() { return *mApp; }
  TCanvas& Canvas() { return *mCanvas->GetCanvas(); }

  template <typename container>
  void DrawHist(TH1* h, const container& c)
  {
    auto [ min, max ] = c.MinMax();
    h->Reset();

    for (const auto v : c)
      h->Fill(v);

    max = max == min ? nextafter(max, max + 1) : max;
    max += (max - min) / 10;
    min -= (max - min) / 10;

    h->GetXaxis()->SetRangeUser(min, max);
    h->Draw();
  }
};
}
} /* namespace o2::DataDistribution */

#endif /* DATADIST_ROOTGUI_H_ */
