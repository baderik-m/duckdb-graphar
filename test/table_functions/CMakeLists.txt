log_stage("tablefunctions")
add_library_unity(test_table_functions OBJECT table_functions_fixture.cpp test_read_vertices.cpp test_read_edges.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:test_table_functions>
    PARENT_SCOPE)
