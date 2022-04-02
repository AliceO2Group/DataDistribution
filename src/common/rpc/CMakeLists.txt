# @author Gvozden Neskovic
# @brief  DataDistribution grpc client stuff

#-------------------------------------------------------------------------------
set (LIB_BASE_SOURCES
  StfSenderRpcClient
  TfSchedulerRpcClient
)

add_library(rpc OBJECT ${LIB_BASE_SOURCES})

target_include_directories(rpc
  PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}
)

target_link_libraries(rpc
  PUBLIC
    base
    discovery
    monitoring
)
