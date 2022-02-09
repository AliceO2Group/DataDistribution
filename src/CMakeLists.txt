add_subdirectory(common)
add_subdirectory(StfBuilder)

if (UCX_FOUND)
    add_subdirectory(ReadoutEmulator)
    add_subdirectory(StfSender)
    add_subdirectory(TfBuilder)
    add_subdirectory(TfScheduler)
    add_subdirectory(DataDistControl)
endif()

add_subdirectory(tests)
