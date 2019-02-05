# @author Gvozden Neskovic
# @brief  cmake for TimeFrameBuilder

set(EXE_TFB_SOURCES
  TimeFrameBuilderDevice
  TimeFrameBuilderInput
  runTimeFrameBuilderDevice
)

add_executable(TfBuilder ${EXE_TFB_SOURCES} $<TARGET_OBJECTS:common>)

include(CheckIPOSupported)
check_ipo_supported(RESULT result)
if(result)
  set_target_properties(TfBuilder PROPERTIES INTERPROCEDURAL_OPTIMIZATION TRUE)
endif()

target_include_directories(TfBuilder
  PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/../common/include
    ${MS_GSL_INCLUDE_DIR}
)

target_link_libraries(TfBuilder
  PUBLIC
  PRIVATE
    FairMQ::FairMQ
    AliceO2::Headers
    ${Monitoring_LIBRARIES}
    ROOT::Gui
)

install(TARGETS TfBuilder RUNTIME DESTINATION bin)
