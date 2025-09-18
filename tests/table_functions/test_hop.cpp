#define CATCH_CONFIG_MAIN
#include <catch2/catch_test_macros.hpp>
#include <catch2/catch_template_test_macros.hpp>
#include <catch2/benchmark/catch_benchmark.hpp>

#include <filesystem>
#include <iostream>

#include "table_functions_fixture.hpp"
#include "functions/table/hop.hpp"

using namespace duckdb;
using namespace graphar;

#define TestFixture TableFunctionsFixture<TestType>

TEST_CASE("OneMoreHop GetFunction basic test", "[one_more_hop]") {
    TableFunction one_more_hop = OneMoreHop::GetFunction();

    REQUIRE(one_more_hop.name == "one_more_hop");
    REQUIRE(one_more_hop.arguments.size() == 1);
    REQUIRE(one_more_hop.named_parameters.size() == 1);
    REQUIRE(one_more_hop.filter_pushdown == false);
    REQUIRE(one_more_hop.projection_pushdown == true);

    REQUIRE(one_more_hop.named_parameters.find("vid") != one_more_hop.named_parameters.end());
}


TEMPLATE_TEST_CASE_METHOD(TableFunctionsFixture, "OneMoreHop Bind and Execute functions vertex without properties", "[one_more_hop]", FileTypeParquet, FileTypeCsv) {
    INFO("Start mocking data for bind");
    vector<Value> inputs({Value(TestFixture::path_trial_graph)});
    named_parameter_map_t named_parameters({{"vid", Value("1")}});
    vector<LogicalType> input_table_types({});
    auto bind_input = TestFixture::CreateMockBindInput(inputs, named_parameters, input_table_types);

    vector<LogicalType> return_types;
    vector<std::string> names;
    INFO("Finish mocking");

    TableFunction one_more_hop = OneMoreHop::GetFunction();
    
    INFO("Bind test");
    auto bind_data = one_more_hop.bind(*TestFixture::conn.context, bind_input, return_types, names);

    REQUIRE(bind_data != nullptr);
    REQUIRE(names == vector<std::string>({SRC_GID_COLUMN, DST_GID_COLUMN}));
    REQUIRE(return_types == vector<LogicalType>({LogicalType::BIGINT, LogicalType::BIGINT}));
    INFO("Finish bind test");

    OneMoreHopGlobalTableFunctionState gstate(*TestFixture::conn.context, bind_data->template Cast<TwoHopBindData>());
    TableFunctionInput func_input(bind_data.get(), nullptr, gstate);

    DataChunk res;

    INFO("Execute test");
    one_more_hop.function(*TestFixture::conn.context, func_input, res);
    REQUIRE(res.size() == 3); // 1 2; 1 3; 2 3;
    REQUIRE(res.ColumnCount() == 2);
    INFO("Finish execute test");
}

TEMPLATE_TEST_CASE_METHOD(TableFunctionsFixture, "OneMoreHop Bind function with invalid vertex id", "[one_more_hop]", FileTypeParquet, FileTypeCsv) {
    INFO("Start mocking");
    vector<Value> inputs({Value(TestFixture::path_trial_graph)});
    named_parameter_map_t named_parameters({{"vid", Value("3141")}});
    vector<LogicalType> input_table_types({});
    auto input = TestFixture::CreateMockBindInput(inputs, named_parameters, input_table_types);

    vector<LogicalType> return_types;
    vector<std::string> names;
    INFO("Finish mocking");

    TableFunction one_more_hop = OneMoreHop::GetFunction();

    REQUIRE_THROWS_AS(one_more_hop.bind(*TestFixture::conn.context, input, return_types, names), BinderException);
}

TEMPLATE_TEST_CASE_METHOD(TableFunctionsFixture,"OneMoreHop Bind and Execute functions vertex with properties", "[one_more_hop]", FileTypeParquet, FileTypeCsv) {
    INFO("Start mocking");
    vector<Value> inputs({Value(TestFixture::path_trial_feature_graph)});

    named_parameter_map_t named_parameters({{"vid", Value("2")}});
    vector<LogicalType> input_table_types({});
    auto input = TestFixture::CreateMockBindInput(inputs, named_parameters, input_table_types);

    vector<LogicalType> return_types;
    vector<std::string> names;
    INFO("Finish mocking");
    
    TableFunction one_more_hop = OneMoreHop::GetFunction();
    
    INFO("Bind test");
    auto bind_data = one_more_hop.bind(*TestFixture::conn.context, input, return_types, names);

    REQUIRE(bind_data != nullptr);
    REQUIRE(names == vector<std::string> ({SRC_GID_COLUMN, DST_GID_COLUMN}));
    REQUIRE(return_types == vector<LogicalType>({LogicalType::BIGINT, LogicalType::BIGINT}));
    INFO("Finish bind test");

    OneMoreHopGlobalTableFunctionState gstate(*TestFixture::conn.context, bind_data->template Cast<TwoHopBindData>());
    TableFunctionInput func_input(bind_data.get(), nullptr, gstate);

    DataChunk res;

    INFO("Execute test");
    one_more_hop.function(*TestFixture::conn.context, func_input, res);
    REQUIRE(res.size() == 3); // 2 3; 2 4; 3 4;
    REQUIRE(res.ColumnCount() == 2);
    INFO("Finish execute test");
}





TEST_CASE("TwoHop GetFunction basic test", "[two_hop]") {
    TableFunction two_hop = TwoHop::GetFunction();

    REQUIRE(two_hop.name == "two_hop");
    REQUIRE(two_hop.arguments.size() == 1);
    REQUIRE(two_hop.named_parameters.size() == 1);
    REQUIRE(two_hop.filter_pushdown == false);
    REQUIRE(two_hop.projection_pushdown == true);

    REQUIRE(two_hop.named_parameters.find("vid") != two_hop.named_parameters.end());
}


TEMPLATE_TEST_CASE_METHOD(TableFunctionsFixture, "TwoHop Bind and Execute functions vertex without properties", "[two_hop]", FileTypeParquet, FileTypeCsv) {
    INFO("Start mocking data for bind");
    vector<Value> inputs({Value(TestFixture::path_trial_graph)});
    named_parameter_map_t named_parameters({{"vid", Value("1")}});
    vector<LogicalType> input_table_types({});
    auto bind_input = TestFixture::CreateMockBindInput(inputs, named_parameters, input_table_types);

    vector<LogicalType> return_types;
    vector<std::string> names;
    INFO("Finish mocking");

    TableFunction two_hop = TwoHop::GetFunction();
    
    INFO("Bind test");
    auto bind_data = two_hop.bind(*TestFixture::conn.context, bind_input, return_types, names);

    REQUIRE(bind_data != nullptr);
    REQUIRE(names == vector<std::string>({SRC_GID_COLUMN, DST_GID_COLUMN}));
    REQUIRE(return_types == vector<LogicalType>({LogicalType::BIGINT, LogicalType::BIGINT}));
    INFO("Finish bind test");

    OneMoreHopGlobalTableFunctionState gstate(*TestFixture::conn.context, bind_data->template Cast<TwoHopBindData>());
    TableFunctionInput func_input(bind_data.get(), nullptr, gstate);

    DataChunk res;

    INFO("Execute test");
    two_hop.function(*TestFixture::conn.context, func_input, res);
    REQUIRE(res.size() == 6); // 1 2; 1 3; 2 3; 2 4; 3 4; 3 5;
    REQUIRE(res.ColumnCount() == 2);
    INFO("Finish execute test");
}

TEMPLATE_TEST_CASE_METHOD(TableFunctionsFixture, "TwoHop Bind function with invalid vertex id", "[two_hop]", FileTypeParquet, FileTypeCsv) {
    INFO("Start mocking");
    vector<Value> inputs({Value(TestFixture::path_trial_graph)});
    named_parameter_map_t named_parameters({{"vid", Value("3141")}});
    vector<LogicalType> input_table_types({});
    auto input = TestFixture::CreateMockBindInput(inputs, named_parameters, input_table_types);

    vector<LogicalType> return_types;
    vector<std::string> names;
    INFO("Finish mocking");

    TableFunction two_hop = TwoHop::GetFunction();

    REQUIRE_THROWS_AS(two_hop.bind(*TestFixture::conn.context, input, return_types, names), BinderException);
}

TEMPLATE_TEST_CASE_METHOD(TableFunctionsFixture,"TwoHop Bind and Execute functions vertex with properties", "[two_hop]", FileTypeParquet, FileTypeCsv) {
    INFO("Start mocking");
    vector<Value> inputs({Value(TestFixture::path_trial_feature_graph)});

    named_parameter_map_t named_parameters({{"vid", Value("2")}});
    vector<LogicalType> input_table_types({});
    auto input = TestFixture::CreateMockBindInput(inputs, named_parameters, input_table_types);

    vector<LogicalType> return_types;
    vector<std::string> names;
    INFO("Finish mocking");
    
    TableFunction two_hop = TwoHop::GetFunction();
    
    INFO("Bind test");
    auto bind_data = two_hop.bind(*TestFixture::conn.context, input, return_types, names);

    REQUIRE(bind_data != nullptr);
    REQUIRE(names == vector<std::string> ({SRC_GID_COLUMN, DST_GID_COLUMN}));
    REQUIRE(return_types == vector<LogicalType>({LogicalType::BIGINT, LogicalType::BIGINT}));
    INFO("Finish bind test");

    OneMoreHopGlobalTableFunctionState gstate(*TestFixture::conn.context, bind_data->template Cast<TwoHopBindData>());
    TableFunctionInput func_input(bind_data.get(), nullptr, gstate);

    DataChunk res;

    INFO("Execute test");
    two_hop.function(*TestFixture::conn.context, func_input, res);
    REQUIRE(res.size() == 5); // 2 3; 2 4; 3 4; 3 5; 4 5;
    REQUIRE(res.ColumnCount() == 2);
    INFO("Finish execute test");
}