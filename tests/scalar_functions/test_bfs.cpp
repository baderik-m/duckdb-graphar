#define CATCH_CONFIG_MAIN
#include <catch2/catch_test_macros.hpp>
#include <catch2/catch_template_test_macros.hpp>
#include <catch2/benchmark/catch_benchmark.hpp>

#include <filesystem>
#include <iostream>
// #include "duckdb/planner/expression.hpp"
#include "duckdb/execution/expression_executor_state.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"

#include "scalar_functions_fixture.hpp"
#include "functions/scalar/bfs.hpp"

using namespace duckdb;
using namespace graphar;

#define TestFixture ScalarFunctionsFixture<TestType>
/*
std::string GetTrialGraph() {
    std::string path = "test/data/";
    std::string graph_name = "trial_graph";
    std::string graph_path = path+graph_name+"/"+graph_name+".graph.yml";

    auto graph_info = graphar::GraphInfo::Load(graph_path).value_or(nullptr);
    if (graph_info != nullptr){ return graph_path; }
    return createAndSaveGraphAr(graph_name, {0, 1, 2, 3, 4}, {{0,1}, {1,3}, {2, 0}, {2, 1}}, path);

}

std::string GetLongGraph() {
    std::string path = "test/data/";
    std::string graph_name = "long_graph";
    std::string graph_path = path+graph_name+"/"+graph_name+".graph.yml";
    
    auto graph_info = graphar::GraphInfo::Load(graph_path).value_or(nullptr);
    if (graph_info != nullptr){ return path; }
    
    std::vector<int64_t> vertices(530);
    std::vector<Edge> edges(556);
    vertices[0] = 0;
    vertices[539]= 539;
    for (int i = 1; i < 30; ++i) {
        edges[2*(i-1)] = {0, i };
        edges[2*(i-1)+1] = {30, i};
        vertices[i] = i;
    }
    for (int i = 31; i < 528; ++i) {
        edges[27 + i] = {i-1, i };
        vertices[i] = i;
    }
    return createAndSaveGraphAr(graph_name, vertices, edges, path);
}
*/


TEST_CASE("BFS GetFunction basic test", "[bfs]") {
    SECTION("bfs_exist") {
        ScalarFunction bfs_exist_path = Bfs::GetFunctionExists();
        
        REQUIRE(bfs_exist_path.name == "bfs_exist");
        REQUIRE(bfs_exist_path.arguments.size() == 3);
        REQUIRE(bfs_exist_path.arguments == vector<LogicalType>({LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::VARCHAR}));
        REQUIRE(bfs_exist_path.return_type == LogicalType::BOOLEAN);
    }

    SECTION("bfs_length") {
        ScalarFunction bfs_length_path = Bfs::GetFunctionLength();

        REQUIRE(bfs_length_path.name == "bfs_length");
        REQUIRE(bfs_length_path.arguments.size() == 3);
        REQUIRE(bfs_length_path.arguments == vector<LogicalType>({LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::VARCHAR}));
        REQUIRE(bfs_length_path.return_type == LogicalType::BIGINT);
    }
}



TEMPLATE_TEST_CASE_METHOD(ScalarFunctionsFixture, "BFS Execute function vertex", "[bfs]", FILE_TYPES_FOR_TEST) {
    DataChunk args;
    args.Initialize(*TestFixture::conn.context, {LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::VARCHAR}, 1);
    args.SetCardinality(1);

    SECTION("bfs_exist") {
        ScalarFunction bfs_exist_path = Bfs::GetFunctionExists();        

        INFO("Mocking state");
        duckdb::BoundFunctionExpression bound_expr(
            LogicalType::BOOLEAN,
            bfs_exist_path,
            {},
            nullptr,
            false
        );
        duckdb::ExpressionExecutorState executor_state{};
        duckdb::ExpressionExecutor executor(*TestFixture::conn.context);

        executor_state.executor = &executor;
        duckdb::ExpressionState state(bound_expr, executor_state);

        duckdb::Vector result(duckdb::LogicalType::BOOLEAN);

        SECTION("1 hop"){
            INFO("Set args");

            args.SetValue(0, 0, Value::BIGINT(1));
            args.SetValue(1, 0, Value::BIGINT(2));
            args.SetValue(2, 0, Value(TestFixture::path_trial_graph));

            INFO("Execute test");
            bfs_exist_path.function(args, state, result);
            auto result_data = FlatVector::GetData<bool>(result);
            REQUIRE(result_data[0] == true);  
            INFO("Finish execute test");
        }
        SECTION(""){
            INFO("Set args");

            args.SetValue(0, 0, Value::BIGINT(1));
            args.SetValue(1, 0, Value::BIGINT(4));
            args.SetValue(2, 0, Value(TestFixture::path_trial_graph));

            INFO("Execute test");
            bfs_exist_path.function(args, state, result);
            auto result_data = FlatVector::GetData<bool>(result);
            REQUIRE(result_data[0] == true);  
            INFO("Finish execute test");
        }

        SECTION("2 hop"){
            INFO("Set args");

            args.SetValue(0, 0, Value::BIGINT(1));
            args.SetValue(1, 0, Value::BIGINT(4));
            args.SetValue(2, 0, Value(TestFixture::path_trial_graph));

            INFO("Execute test");
            bfs_exist_path.function(args, state, result);
            auto result_data = FlatVector::GetData<bool>(result);
            REQUIRE(result_data[0] == true);  
            INFO("Finish execute test");
        }

        SECTION("Fake vertex"){
            INFO("Set args");

            args.SetValue(0, 0, Value::BIGINT(0));
            args.SetValue(1, 0, Value::BIGINT(2));
            args.SetValue(2, 0, Value(TestFixture::path_trial_graph));

            INFO("Execute test");
            
            bfs_exist_path.function(args, state, result);
            auto result_data = FlatVector::GetData<bool>(result);
            REQUIRE(result_data[0] == false);  
            INFO("Finish execute test");
        }

    }
    SECTION("bfs_length"){
        ScalarFunction bfs_length_path = Bfs::GetFunctionLength(); 
        
        INFO("Mocking state");
        duckdb::BoundFunctionExpression bound_expr(
            LogicalType::BIGINT,
            bfs_length_path,
            {},
            nullptr,
            false
        );
        duckdb::ExpressionExecutorState executor_state{};
        duckdb::ExpressionExecutor executor(*TestFixture::conn.context);

        executor_state.executor = &executor;
        duckdb::ExpressionState state(bound_expr, executor_state);

        duckdb::Vector result(duckdb::LogicalType::BIGINT);

        SECTION("1 hop"){
            INFO("Set args");

            args.SetValue(0, 0, Value::BIGINT(1));
            args.SetValue(1, 0, Value::BIGINT(2));
            args.SetValue(2, 0, Value(TestFixture::path_trial_graph));

            INFO("Execute test");
            bfs_length_path.function(args, state, result);
            auto result_data = FlatVector::GetData<long long>(result);
            REQUIRE(result_data[0] == 1);  
            INFO("Finish execute test");
        }

        SECTION("2 hop"){
            INFO("Set args");

            args.SetValue(0, 0, Value::BIGINT(1));
            args.SetValue(1, 0, Value::BIGINT(4));
            args.SetValue(2, 0, Value(TestFixture::path_trial_graph));

            INFO("Execute test");
            bfs_length_path.function(args, state, result);
            auto result_data = FlatVector::GetData<long long>(result);
            REQUIRE(result_data[0] == 2);  
            INFO("Finish execute test");
        }
    }
}
/*
TEST_CASE("BFS Basic Functionality", "[bfs]") {
    DuckDB db(nullptr);
    Connection con(db);
    
    Bfs::Register(*db.instance);
    
    std::string graph_path = GetTrialGraph();
    
    SECTION("BFS Length - Direct Connection") {
        auto result = con.Query("SELECT bfs_length(0, 1, '" + graph_path + "')");
        
        REQUIRE(result->GetValue(0, 0).GetValue<int64_t>() == 1);
    }
    
    SECTION("BFS Length - Two Hops") {
        auto result = con.Query("SELECT bfs_length(0, 3, '" + graph_path + "')");
        
        REQUIRE(result->GetValue(0, 0).GetValue<int64_t>() == 2);
    }
    
    SECTION("BFS Length - No Path") {
        auto result = con.Query("SELECT bfs_length(4, 0, '" + graph_path + "')");
        
        REQUIRE(result->GetValue(0, 0).GetValue<int64_t>() == -1);
    }
    
    SECTION("BFS Exists - Direct Connection") {
        auto result = con.Query("SELECT bfs_exist(0, 1, '" + graph_path + "')");
        
        REQUIRE(result->GetValue(0, 0).GetValue<bool>());
    }
    
    SECTION("BFS Exists - No Path") {
        auto result = con.Query("SELECT bfs_exist(4, 0, '" + graph_path + "')");
        
        REQUIRE(!result->GetValue(0, 0).GetValue<bool>());
    }
}

TEST_CASE("BFS Performance", "[bfs][stress]") {
    DuckDB db(nullptr);
    Connection con(db);
    
    Bfs::Register(*db.instance);
    
    std::string graph_path = GetLongGraph();
    
    SECTION("BFS Long Path Stress Test (500 hops)") {
        std::string long_graph_path = "test_long_graph";
        
        BENCHMARK("BFS Long Path") {
            auto result = con.Query("SELECT bfs_length(0, 527, '" + long_graph_path + "')");
            return (result->GetValue(0, 0).GetValue<int64_t>() == 500);
        };

        BENCHMARK("BFS No Path") {
            auto result = con.Query("SELECT bfs_length(0, 528, '" + long_graph_path + "')");
            return (result->GetValue(0, 0).GetValue<int64_t>()==-1);
        };
    }
}
*/