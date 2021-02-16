# @author Gvozden Nešković
# @brief  cmake for TfBuilder

set(EXE_TFB_SOURCES
  TfBuilderDevice
  TfBuilderInput
  TfBuilderRpc
  runTfBuilderDevice
)

add_library(TfBuilder_lib OBJECT ${EXE_TFB_SOURCES})
target_link_libraries(TfBuilder_lib base common discovery)

add_executable(TfBuilder)

include(CheckIPOSupported)
check_ipo_supported(RESULT result)
if(result)
  set_target_properties(TfBuilder PROPERTIES INTERPROCEDURAL_OPTIMIZATION TRUE)
endif()

target_link_libraries(TfBuilder
  PRIVATE
    TfBuilder_lib
    base common discovery
)

install(TARGETS TfBuilder RUNTIME DESTINATION bin)
