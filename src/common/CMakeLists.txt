# @author Gvozden Neskovic
# @brief  DataDistribution common DataModel related stuff

#-------------------------------------------------------------------------------

add_subdirectory(base)
add_subdirectory(discovery)

set (LIB_COMMON_SOURCES
  ReadoutDataModel
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

add_library(common OBJECT ${LIB_COMMON_SOURCES})

include(CheckIPOSupported)
check_ipo_supported(RESULT result)
if(result)
  set_target_properties(common PROPERTIES INTERPROCEDURAL_OPTIMIZATION TRUE)
endif()

target_include_directories(common
  PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)

target_link_libraries(common
  PUBLIC
    base
    FairMQ::FairMQ
    AliceO2::Headers
    AliceO2::Framework
    ROOT::Gui
)
