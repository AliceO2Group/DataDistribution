# @author Gvozden Neskovic
# @brief  cmake for SubTimeFrameBuilder

# Experimental TimeFrameFiltering
# add_definitions(-DSTF_FILTER_EXAMPLE)

set(EXE_STFB_SOURCES
  SubTimeFrameBuilderInput
  SubTimeFrameBuilderDevice
  runSubTimeFrameBuilderDevice
)

add_executable(StfBuilder ${EXE_STFB_SOURCES} $<TARGET_OBJECTS:common>)

include(CheckIPOSupported)
check_ipo_supported(RESULT result)
if(result)
  set_target_properties(StfBuilder PROPERTIES INTERPROCEDURAL_OPTIMIZATION TRUE)
endif()

target_include_directories(StfBuilder
  PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/../common/include
)

target_link_libraries(StfBuilder
  PUBLIC
  PRIVATE
    FairMQ::FairMQ
    AliceO2::Headers
    AliceO2::Framework
    ROOT::Gui
)

install(TARGETS StfBuilder RUNTIME DESTINATION bin)
