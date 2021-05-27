# @author Gvozden Neskovic
# @brief  DataDistribution monitoring lib

#-------------------------------------------------------------------------------
set (LIB_MON_SOURCES
  DataDistMonitoring
)

add_library(monitoring OBJECT ${LIB_MON_SOURCES})

target_include_directories(monitoring
  PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}
)

target_link_libraries(monitoring
  PRIVATE
    base
    discovery
  PUBLIC
    AliceO2::Monitoring
)