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

#ifndef STF_DATAMODEL_UTILS_H_
#define STF_DATAMODEL_UTILS_H_

namespace o2
{
namespace DataDistribution
{

////////////////////////////////////////////////////////////////////////////////
/// IDataModelObject interface
////////////////////////////////////////////////////////////////////////////////
class EquipmentHBFrames;
class SubTimeFrame;

class ISubTimeFrameVisitor
{
 public:
  virtual void visit(SubTimeFrame&) = 0;
};

class ISubTimeFrameConstVisitor
{
 public:
  virtual void visit(const SubTimeFrame&) = 0;
};

class IDataModelObject
{
 public:
  virtual void accept(ISubTimeFrameVisitor& v) = 0;
  virtual void accept(ISubTimeFrameConstVisitor& v) const = 0;
};
}
} /* o2::DataDistribution */

#endif /* STF_DATAMODEL_UTILS_H_ */
