# @author Gvozden Neskovic
# @brief  DataDistribution base stuff: Logging, etc...

#-------------------------------------------------------------------------------
set (LIB_BASE_SOURCES
  DataDistLogger
  FilePathUtils
)

add_library(base OBJECT ${LIB_BASE_SOURCES})

target_include_directories(base
  PUBLIC
    ${CMAKE_BINARY_DIR}/spdlog/include
    ${CMAKE_CURRENT_SOURCE_DIR}
)

target_link_libraries(base
  PUBLIC
    jemalloc_STATIC
    spdlog::spdlog
    Boost::filesystem
    AliceO2::InfoLogger
    FairLogger::FairLogger
)
