#--- EXTERNAL PROJECTS  --------------------------------------------------------------
include(ExternalProject)

ExternalProject_Add(jemalloc
  GIT_REPOSITORY "https://github.com/jemalloc/jemalloc.git"
  GIT_TAG "5.2.1"

  GIT_SHALLOW TRUE
  GIT_PROGRESS TRUE

  UPDATE_COMMAND ""
  PATCH_COMMAND ""

  SOURCE_DIR "${CMAKE_BINARY_DIR}/3rdparty/jemalloc"

  BUILD_IN_SOURCE 1
  BUILD_BYPRODUCTS ${jemalloc_STATIC} ${jemalloc_STATIC_PIC}

  CONFIGURE_COMMAND ./autogen.sh && ./configure --disable-shared --disable-doc --prefix=${CMAKE_BINARY_DIR}/jemalloc --with-malloc-conf=abort_conf:true,background_thread:true,metadata_thp:auto,dirty_decay_ms:300000,muzzy_decay_ms:300000
  BUILD_COMMAND ${MAKE}
  INSTALL_COMMAND make install

  LOG_DOWNLOAD True
  LOG_UPDATE True
  LOG_INSTALL True
  LOG_OUTPUT_ON_FAILURE True
)

set(jemalloc_INCLUDE_DIRS ${CMAKE_CURRENT_BINARY_DIR}/jemalloc/include)

add_library(jemalloc_STATIC STATIC IMPORTED)
set_property(TARGET jemalloc_STATIC PROPERTY IMPORTED_LOCATION ${CMAKE_BINARY_DIR}/jemalloc/lib/libjemalloc.a)
add_dependencies(jemalloc_STATIC jemalloc)

add_library(jemalloc_STATIC_PIC STATIC IMPORTED)
set_property(TARGET jemalloc_STATIC_PIC PROPERTY IMPORTED_LOCATION ${CMAKE_BINARY_DIR}/jemalloc/lib/libjemalloc_pic.a)
add_dependencies(jemalloc_STATIC_PIC jemalloc)

if (!APPLE)
  link_libraries(-Wl,--no-as-needed)
endif(!APPLE)

link_libraries(dl ${jemalloc_STATIC_PIC})
