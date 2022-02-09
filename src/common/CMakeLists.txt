# @author Gvozden Neskovic
# @brief  DataDistribution common DataModel related stuff

#-------------------------------------------------------------------------------

add_subdirectory(base)
add_subdirectory(discovery)
add_subdirectory(fmqtools)

add_subdirectory(monitoring)
if(UCX_FOUND)
  add_subdirectory(ucxtools)
endif()

set (LIB_COMMON_SOURCES
  ReadoutDataModel
  SubTimeFrameBuilder
  SubTimeFrameDataModel
  SubTimeFrameVisitors
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
    ${CMAKE_CURRENT_SOURCE_DIR}
)

target_link_libraries(common
  PUBLIC
    base
    fmqtools
    discovery
    protobuf::libprotobuf
    Boost::iostreams
    FairMQ::FairMQ
    AliceO2::Headers
)
