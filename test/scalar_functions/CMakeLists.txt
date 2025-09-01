log_stage("scalar_functions")
add_library_unity(test_graphar_scalar_funcs OBJECT test_bfs.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:test_graphar_scalar_funcs>
    PARENT_SCOPE)
