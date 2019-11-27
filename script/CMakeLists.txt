# @author Gvozden Neskovic
# @brief  Demonstration and test chain scripts

set(DEMO_SCRIPTS
  start_Emulator-3FLP-3EPN.sh
  start_Discovery-3FLP-3EPN.sh
)

set(DEMO_CONFIGS
  readout-emu-flp-epn-chain.json
  discovery-flp-epn-chain.json
  flp-epn-dds-topology.xml
  readout_cfg/readout_emu.cfg
)

foreach(SCRIPT ${DEMO_SCRIPTS})
  configure_file(${SCRIPT}.in ${CMAKE_BINARY_DIR}/bin/${SCRIPT} @ONLY)
  install(PROGRAMS ${CMAKE_BINARY_DIR}/bin/${SCRIPT} DESTINATION bin)
endforeach()

foreach(CONFIG ${DEMO_CONFIGS})
  configure_file(${CONFIG} ${CMAKE_BINARY_DIR}/config/${CONFIG} @ONLY)
  install(FILES ${CMAKE_BINARY_DIR}/config/${CONFIG} DESTINATION config)
endforeach()
