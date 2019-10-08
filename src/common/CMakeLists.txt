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
  PUBLIC
  PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)


target_link_libraries(common
  PUBLIC
  PRIVATE
    Boost::filesystem
    FairMQ::FairMQ
    AliceO2::Headers
    ROOT::Gui
)

add_subdirectory(discovery)
