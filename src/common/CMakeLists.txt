# @author Gvozden Neskovic
# @brief  DataDistribution common DataModel related stuff

#-------------------------------------------------------------------------------

add_subdirectory(base)
add_subdirectory(discovery)

set (LIB_COMMON_SOURCES
  ReadoutDataModel
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
)


set (LIB_COMMON_ROOT_SOURCES
  RootGui
)

add_library(common_root OBJECT ${LIB_COMMON_ROOT_SOURCES})

include(CheckIPOSupported)
check_ipo_supported(RESULT result)
if(result)
  set_target_properties(common_root PROPERTIES INTERPROCEDURAL_OPTIMIZATION TRUE)
endif()

target_include_directories(common_root
  PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)

target_link_libraries(common_root
  PUBLIC
    ROOT::Gui
)

