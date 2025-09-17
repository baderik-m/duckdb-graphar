#define CATCH_CONFIG_MAIN
#include <catch2/catch_test_macros.hpp>
#include <catch2/benchmark/catch_benchmark.hpp>

#include <filesystem>  
#include <iostream>  

#include "table_functions_fixture.hpp"
#include "functions/table/read_edges.hpp"

using namespace duckdb;
using namespace graphar;

#define TestFixture TableFunctionsFixture<TestType>

TEST_CASE("ReadEdges GetFunction basic test", "[read_edges]") {
    TableFunction read_edges = ReadEdges::GetFunction();
    
    REQUIRE(read_edges.name == "read_edges");
    REQUIRE(read_edges.arguments.size() == 1);
    REQUIRE(read_edges.named_parameters.size() == 3);
    REQUIRE(read_edges.filter_pushdown == true);
    REQUIRE(read_edges.projection_pushdown == true);
    
    REQUIRE(read_edges.named_parameters.find("src") != read_edges.named_parameters.end());
    REQUIRE(read_edges.named_parameters.find("dst") != read_edges.named_parameters.end());
    REQUIRE(read_edges.named_parameters.find("type") != read_edges.named_parameters.end());
}

TEMPLATE_TEST_CASE_METHOD(TableFunctionsFixture, "ReadEdges Bind function basic test", "[read_edges]", FileTypeParquet, FileTypeCsv) {

    INFO("Start mocking");    
    vector<Value> inputs({Value(TestFixture::path_trial_graph)});
    named_parameter_map_t named_parameters({{"src", Value("Person")}, {"dst", Value("Person")}, {"type", Value("knows")}});
    vector<LogicalType> input_table_types({});
    auto input = TestFixture::CreateMockBindInput(inputs, named_parameters, input_table_types);    
    
    vector<LogicalType> return_types;
    vector<std::string> names;
    INFO("Finish mocking");

    TableFunction read_edges = ReadEdges::GetFunction();

    INFO("Start bind");
    auto bind_data = read_edges.bind(*TestFixture::conn.context, input, return_types, names);
    INFO("Finish bind");

    REQUIRE(bind_data != nullptr);
    REQUIRE(return_types == vector<LogicalType> ({LogicalType::BIGINT, LogicalType::BIGINT}));
    REQUIRE(names == vector<std::string> ({SRC_GID_COLUMN, DST_GID_COLUMN}));
}

TEMPLATE_TEST_CASE_METHOD(TableFunctionsFixture, "ReadEdges Bind function invalid_edge", "[read_edges]", FileTypeParquet, FileTypeCsv) {
    INFO("Start mocking");
    vector<Value> inputs({Value(TestFixture::path_trial_graph)});
    named_parameter_map_t named_parameters({{"src", Value("Person")}, {"dst", Value("Person")}, {"type", Value("invalid_edge")}});
    vector<LogicalType> input_table_types({});
    auto input = TestFixture::CreateMockBindInput(inputs, named_parameters, input_table_types);    
        
    vector<LogicalType> return_types;
    vector<std::string> names;
    INFO("Finish mocking");
    
    TableFunction read_edges = ReadEdges::GetFunction();

    REQUIRE_THROWS_AS(read_edges.bind(*TestFixture::conn.context, input, return_types, names), BinderException);
}

TEMPLATE_TEST_CASE_METHOD(TableFunctionsFixture, "ReadEdges Bind function edge with property", "[read_edges]", FileTypeParquet, FileTypeCsv) {
    INFO("Start mocking");
    DuckDB db(nullptr); 
    Connection conn(db);
    INFO("Path: " + TestFixture::path_trial_graph);
    vector<Value> inputs({Value(TestFixture::path_trial_feature_graph)});
    named_parameter_map_t named_parameters({{"src", Value("Person")}, {"dst", Value("Person")}, {"type", Value("knows")}});
    vector<LogicalType> input_table_types({});
    auto input = TestFixture::CreateMockBindInput(inputs, named_parameters, input_table_types);    
    
    vector<LogicalType> return_types;
    vector<std::string> names;
    INFO("Finish mocking");

    TableFunction read_edges = ReadEdges::GetFunction();

    INFO("Start bind");    
    auto bind_data = read_edges.bind(*TestFixture::conn.context, input, return_types, names);
    INFO("Finish bind");

    REQUIRE(bind_data != nullptr);
    REQUIRE(return_types == vector<LogicalType> ({LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::INTEGER, LogicalType::VARCHAR, LogicalType::FLOAT}));
    REQUIRE(names == vector<std::string> ({SRC_GID_COLUMN, DST_GID_COLUMN, "friend_score", "created_at", "tmp_"}));
}

/*
TEST_CASE("ReadEdges GetScanFunction basic test", "[read_edges]") {
    TableFunction scan_edges = ReadEdges::GetScanFunction();
    
    REQUIRE(scan_edges.name == "");
    REQUIRE(scan_edges.arguments.empty());
    REQUIRE(scan_edges.filter_pushdown == true);
    REQUIRE(scan_edges.projection_pushdown == true);
}


TEST_CASE("ReadEdges GetReader test", "[read_edges]") {
    INFO("Creating db");
    DuckDB db(nullptr);
    Connection conn(db);

    INFO("Start mocking");

    vector<Value> inputs({Value(GRAPH_YML_PATH)});
    named_parameter_map_t named_parameters({{"src", Value("Person")}, {"dst", Value("Person")}, {"type", Value("knows")}});
    vector<LogicalType> input_table_types;
    auto input = CreateMockBindInput(inputs, named_parameters, input_table_types);

    vector<LogicalType> return_types;
    vector<std::string> names;
    INFO("Finish mocking");

    auto bind_data_uniq = ReadEdges::GetFunction().bind(*conn.context, input, return_types, names);
    auto& bind_data = bind_data_uniq->Cast<ReadBindData>();

    ReadBaseGlobalTableFunctionState gstate;

    SECTION("Тест без фильтра"){
        auto reader = ReadEdges::GetReader(gstate, bind_data, 0, "", "", "");
        REQUIRE(reader != nullptr);

        auto result = GetChunk(*reader);
        REQUIRE(!result.has_error());
        auto table = result.value();
        REQUIRE(table->num_rows() == 578006);
        REQUIRE(table->num_columns() == 2);
    }

    SECTION("Тест с фильтром по SRC"){
        auto reader = ReadEdges::GetReader(gstate, bind_data, 0, "0", SRC_GID_COLUMN, "int64");
        REQUIRE(reader != nullptr);

        auto result = GetChunk(*reader);
        REQUIRE(!result.has_error());
        auto table = result.value();
        REQUIRE(table->num_rows() == 1); // только одно ребро выходящее из вершины 0
    }
}
*/