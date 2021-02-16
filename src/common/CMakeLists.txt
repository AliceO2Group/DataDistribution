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

target_include_directories(common
  PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)

target_link_libraries(common
  PUBLIC
    base
    Boost::iostreams
    FairMQ::FairMQ
    AliceO2::Headers
)