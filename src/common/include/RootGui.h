// Copyright CERN and copyright holders of ALICE O2. This software is
// distributed under the terms of the GNU General Public License v3 (GPL
// Version 3), copied verbatim in the file "COPYING".
//
// See http://alice-o2.web.cern.ch/license for full licensing information.
//
// In applying this license CERN does not waive the privileges and immunities
// granted to it by virtue of its status as an Intergovernmental Organization
// or submit itself to any jurisdiction.

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
