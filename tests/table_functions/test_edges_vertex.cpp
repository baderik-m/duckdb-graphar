#define CATCH_CONFIG_MAIN
#include <catch2/catch_test_macros.hpp>
#include <catch2/catch_template_test_macros.hpp>
#include <catch2/benchmark/catch_benchmark.hpp>

#include <filesystem>
#include <iostream>

#include "table_functions_fixture.hpp"
#include "functions/table/edges_vertex.hpp"

using namespace duckdb;
using namespace graphar;

#define TestFixture TableFunctionsFixture<TestType>


TEST_CASE("EdgesVertex GetFunction basic test", "[edges_vertex]") {
    TableFunction edges_vertex = EdgesVertex::GetFunction();

    REQUIRE(edges_vertex.name == "edges_vertex");
    REQUIRE(edges_vertex.arguments.size() == 1);
    REQUIRE(edges_vertex.named_parameters.size() == 0);
    REQUIRE(edges_vertex.filter_pushdown == true);
    REQUIRE(edges_vertex.projection_pushdown == false);
}

TEMPLATE_TEST_CASE_METHOD(TableFunctionsFixture, "EdgesVertex Bind and Execute functions vertex without properties", "[edges_vertex]", FILE_TYPES_FOR_TEST) {
    INFO("Start mocking data for bind");
    vector<Value> inputs({Value(TestFixture::path_trial_graph)});

    INFO("Path: " + TestFixture::path_trial_graph);

    named_parameter_map_t named_parameters({});
    vector<LogicalType> input_table_types({});
    auto bind_input = TestFixture::CreateMockBindInput(inputs, named_parameters, input_table_types);

    vector<LogicalType> return_types;
    vector<std::string> names;
    INFO("Finish mocking");

    TableFunction edges_vertex = EdgesVertex::GetFunction();
    
    INFO("Bind test");
    unique_ptr<FunctionData> bind_data;
    REQUIRE_NOTHROW(bind_data = edges_vertex.bind(*TestFixture::conn.context, bind_input, return_types, names));

    REQUIRE(bind_data != nullptr);
    REQUIRE(names == vector<std::string>({SRC_GID_COLUMN, "degree"}));
    REQUIRE(return_types == vector<LogicalType>({LogicalType::BIGINT, LogicalType::BIGINT}));
    INFO("Finish bind test");

    TableFunctionInitInput func_init_input(bind_data.get(), vector<column_t>(), {}, nullptr);

    unique_ptr<GlobalTableFunctionState> gstate;
    REQUIRE_NOTHROW(gstate = edges_vertex.init_global(*TestFixture::conn.context, func_init_input));

    TableFunctionInput func_input(bind_data.get(), nullptr, gstate);
    DataChunk res;
    res.Initialize(*TestFixture::conn.context, return_types);

    INFO("Execute test");
    REQUIRE_NOTHROW(edges_vertex.function(*TestFixture::conn.context, func_input, res));
    REQUIRE(res.size() == 3); // 1 2; 1 3; 2 3;
    REQUIRE(res.ColumnCount() == 2);
    INFO("Finish execute test");
}

TEMPLATE_TEST_CASE_METHOD(TableFunctionsFixture,"EdgesVertex Bind and Execute functions vertex with properties", "[edges_vertex]", FILE_TYPES_FOR_TEST) {
    INFO("Start mocking");
    vector<Value> inputs({Value(TestFixture::path_trial_feature_graph)});

    INFO("Path: " + TestFixture::path_trial_feature_graph);

    named_parameter_map_t named_parameters({{"vid", Value("2")}});
    vector<LogicalType> input_table_types({});
    auto input = TestFixture::CreateMockBindInput(inputs, named_parameters, input_table_types);

    vector<LogicalType> return_types;
    vector<std::string> names;
    INFO("Finish mocking");
    
    TableFunction edges_vertex = EdgesVertex::GetFunction();
    
    INFO("Bind test");
    unique_ptr<FunctionData> bind_data;
    REQUIRE_NOTHROW(bind_data = edges_vertex.bind(*TestFixture::conn.context, input, return_types, names));

    REQUIRE(bind_data != nullptr);
    REQUIRE(names == vector<std::string> ({SRC_GID_COLUMN, "degree"}));
    REQUIRE(return_types == vector<LogicalType>({LogicalType::BIGINT, LogicalType::BIGINT}));
    INFO("Finish bind test");

    TableFunctionInitInput func_init_input(bind_data.get(), vector<column_t>(), {}, nullptr);

    unique_ptr<GlobalTableFunctionState> gstate;
    REQUIRE_NOTHROW(gstate = edges_vertex.init_global(*TestFixture::conn.context, func_init_input));
    
    TableFunctionInput func_input(bind_data.get(), nullptr, gstate);
    DataChunk res;
    res.Initialize(*TestFixture::conn.context, return_types);

    INFO("Execute test");
    REQUIRE_NOTHROW(edges_vertex.function(*TestFixture::conn.context, func_input, res));
    REQUIRE(res.size() == 5); 
    REQUIRE(res.ColumnCount() == 2);
    INFO("Finish execute test");
}

TEMPLATE_TEST_CASE_METHOD(TableFunctionsFixture, "EdgesVertex Bind function with invalid vertex id", "[edges_vertex]", FILE_TYPES_FOR_TEST) {
    INFO("Start mocking");
    vector<Value> inputs({Value(TestFixture::path_trial_graph)});

    INFO("Path: " + TestFixture::path_trial_graph);

    named_parameter_map_t named_parameters({});
    vector<LogicalType> input_table_types({});
    auto input = TestFixture::CreateMockBindInput(inputs, named_parameters, input_table_types);

    vector<LogicalType> return_types;
    vector<std::string> names;
    INFO("Finish mocking");

    TableFunction edges_vertex = EdgesVertex::GetFunction();

    REQUIRE_THROWS_AS(edges_vertex.bind(*TestFixture::conn.context, input, return_types, names), BinderException);
}