# @author Gvozden Neskovic
# @brief  DataDistribution common stuff: DataModel and Utilities

#-------------------------------------------------------------------------------
set (LIB_COMMONG_SOURCES
  ReadoutDataModel
  FilePathUtils
  RootGui
  SubTimeFrameBuilder
  SubTimeFrameDataModel
  SubTimeFrameVisitors
  SubTimeFrameUtils
  SubTimeFrameFile
  SubTimeFrameFileWriter
  SubTimeFrameFileSink
  SubTimeFrameFileReader
  SubTimeFrameFileSource
  SubTimeFrameDPL
)

add_library(common OBJECT ${LIB_COMMONG_SOURCES})

include(CheckIPOSupported)
check_ipo_supported(RESULT result)
if(result)
  set_target_properties(common PROPERTIES INTERPROCEDURAL_OPTIMIZATION TRUE)
endif()

target_include_directories(common
  PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${MS_GSL_INCLUDE_DIR}
)

target_link_libraries(common
  PUBLIC
  PRIVATE
    Boost::filesystem
    FairMQ::FairMQ
    AliceO2::AliceO2
    ${Monitoring_LIBRARIES}
    ROOT::Gui
)
