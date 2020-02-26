# @author Gvozden Neskovic
# @brief  DataDistribution base stuff: Logging, etc...

#-------------------------------------------------------------------------------
set (LIB_BASE_SOURCES
  DataDistLogger
  FilePathUtils
)

add_library(base OBJECT ${LIB_BASE_SOURCES})

include(CheckIPOSupported)
check_ipo_supported(RESULT result)
if(result)
  set_target_properties(base PROPERTIES INTERPROCEDURAL_OPTIMIZATION TRUE)
endif()

target_include_directories(base
  PUBLIC
    ${CMAKE_BINARY_DIR}/spdlog/include
    ${CMAKE_CURRENT_SOURCE_DIR}
)

target_link_libraries(base
  PUBLIC
    spdlog::spdlog
    Boost::filesystem
    FairLogger::FairLogger
    FairMQ::FairMQ
)
