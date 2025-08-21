# =============================================
# Build Targets
# =============================================

file(GLOB_RECURSE EXTENSION_SOURCES CONFIGURE_DEPENDS "${EXTENSION_ROOT_DIR}/src/*.cpp")

log_stage("Building static extension")
build_static_extension(duckdb_graphar ${EXTENSION_SOURCES})
log_done()

log_stage("Building loadable extension")
build_loadable_extension(duckdb_graphar ${EXTENSION_SOURCES})
log_done()

set(DEPS_INSTALL_DIR ${CMAKE_BINARY_DIR}/_deps)
set(ARROW_INSTALL_DIR ${DEPS_INSTALL_DIR}/arrow-install)
set(GRAPHAR_INSTALL_DIR ${DEPS_INSTALL_DIR}/graphar-install)

set(GRAPHAR_SOURCE_DIR ${DEPS_INSTALL_DIR}/graphar-src)

set(EXTENSION_INCLUDES
    $<BUILD_INTERFACE:${EXTENSION_ROOT_DIR}/include>
    $<BUILD_INTERFACE:${GRAPHAR_INSTALL_DIR}/include>
    $<BUILD_INTERFACE:${GRAPHAR_SOURCE_DIR}/cpp/thirdparty>
    $<BUILD_INTERFACE:${duckdb_SOURCE_DIR}/src/include>
    $<BUILD_INTERFACE:${arrow_SOURCE_DIR}/cpp/src>
)

target_include_directories(duckdb_graphar_extension PUBLIC ${EXTENSION_INCLUDES} $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>)
target_include_directories(duckdb_graphar_loadable_extension PUBLIC ${EXTENSION_INCLUDES} $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>)

target_link_libraries(duckdb_graphar_extension
    duckdb_static
    arrow::arrow_shared
    graphar::graphar_shared
)

target_link_libraries(duckdb_graphar_loadable_extension
    arrow::arrow_shared
    graphar::graphar_shared
)

# =============================================
# Installation
# =============================================
log_stage("Installing...")
set(INSTALL_LIB_DIR "${CMAKE_INSTALL_LIBDIR}" CACHE PATH "Library installation directory")

install(
  TARGETS duckdb_graphar_extension
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")

install(TARGETS duckdb_graphar_loadable_extension
    LIBRARY DESTINATION ${INSTALL_LIB_DIR}
)

install(DIRECTORY include/ DESTINATION include
    FILES_MATCHING PATTERN "*.h" PATTERN "*.hpp"
)

log_done()


if(BUILD_TESTS)
    message(STATUS "Building unit tests for extension")
 
    # Fetch the catch2 testing framework using CMake's FetchContent
    include(FetchContent)
    FetchContent_Declare(
        Catch2
        GIT_REPOSITORY https://github.com/catchorg/Catch2.git
        GIT_TAG        v3.5.2 # The stable version of catch2
    )
    FetchContent_MakeAvailable(Catch2)
    get_property(catch_targets GLOBAL PROPERTY Catch2_IMPORTED_TARGETS)
    message(STATUS "Imported Catch2 targets: ${catch_targets}")
    # Add the test runner executable, which includes your test files.
    # unittest_main.cpp will contain your test cases.
    add_executable(unittest "${EXTENSION_ROOT_DIR}/test/unittests/bfs_tests.cpp")
    
    # Link the test runner against your extension's code.
    # This makes your Storage Manager's classes and functions available to the tests.
    target_link_libraries(unittest PRIVATE ${DUCKDB_EXTENSION_NAME})
 
    # Link against DuckDB and catch2.
    # Catch2::Catch2WithMain provides the main() function for the test runner.
    target_link_libraries(unittest PRIVATE duckdb graphar::graphar_shared Catch2::Catch2WithMain)
 
    # Ensure the test runner can find DuckDB's header files.
    target_include_directories(unittest PRIVATE ${DUCKDB_INCLUDES})
    target_include_directories(unittest PRIVATE ${EXTENSION_INCLUDES})

    # Integrate with ctest for easy test execution.
    add_test(NAME ${DUCKDB_EXTENSION_NAME}_unittest COMMAND unittest)
    message(STATUS "Unittests done")
endif()

