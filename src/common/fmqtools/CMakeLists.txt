# @author Gvozden Neskovic
# @brief  DataDistribution base stuff: Logging, etc...

#-------------------------------------------------------------------------------
set (LIB_FMQTOOLS_SOURCES
  FmqUtilities
)

add_library(fmqtools OBJECT ${LIB_FMQTOOLS_SOURCES})


target_include_directories(fmqtools
  PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}
)

target_link_libraries(fmqtools
  PUBLIC
    FairLogger::FairLogger
    FairMQ::FairMQ
    discovery
    monitoring
    base
)
