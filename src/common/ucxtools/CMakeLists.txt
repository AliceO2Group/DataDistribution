# @author Gvozden Neskovic
# @brief  DataDistribution UCX helpers

#-------------------------------------------------------------------------------
set (LIB_UCXTOOLS_SOURCES
  UCXUtilities
)

add_library(ucxtools OBJECT ${LIB_UCXTOOLS_SOURCES})


target_include_directories(ucxtools
  PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}
)

target_link_libraries(ucxtools
  PUBLIC
    base
    UCX::ucp
    UCX::uct
    UCX::ucm
    UCX::ucs
)
