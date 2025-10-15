#define CATCH_CONFIG_MAIN
#include <catch2/catch_test_macros.hpp>
#include <catch2/catch_template_test_macros.hpp>
#include <catch2/benchmark/catch_benchmark.hpp>

#include <filesystem>
#include <iostream>

#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/execution/expression_executor.hpp"

#include "scalar_functions_fixture.hpp"
#include "functions/scalar/bfs.hpp"

using namespace duckdb;
using namespace graphar;

#define TestFixture ScalarFunctionsFixture<TestType>


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


TEMPLATE_TEST_CASE_METHOD(ScalarFunctionsFixture, "BFS Execute function for trial graph", "[bfs]", FILE_TYPES_FOR_TEST) {
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
            REQUIRE_NOTHROW(bfs_exist_path.function(args, state, result));
            auto result_data = FlatVector::GetData<bool>(result);
            REQUIRE(result_data[0] == true);  
            INFO("Finish execute test");
        }
        SECTION("Non-existent path"){
            INFO("Set args");

            args.SetValue(0, 0, Value::BIGINT(1));
            args.SetValue(1, 0, Value::BIGINT(7));
            args.SetValue(2, 0, Value(TestFixture::path_trial_graph));

            INFO("Execute test");
            REQUIRE_NOTHROW(bfs_exist_path.function(args, state, result));
            auto result_data = FlatVector::GetData<bool>(result);
            REQUIRE(result_data[0] == false);  
            INFO("Finish execute test");
        }
        SECTION("Reverse path 1-hop"){
            INFO("Set args");

            args.SetValue(0, 0, Value::BIGINT(2));
            args.SetValue(1, 0, Value::BIGINT(1));
            args.SetValue(2, 0, Value(TestFixture::path_trial_graph));

            INFO("Execute test");
            REQUIRE_NOTHROW(bfs_exist_path.function(args, state, result));
            auto result_data = FlatVector::GetData<bool>(result);
            REQUIRE(result_data[0] == false);  
            INFO("Finish execute test");
        }
        SECTION("2 hop"){
            INFO("Set args");

            args.SetValue(0, 0, Value::BIGINT(1));
            args.SetValue(1, 0, Value::BIGINT(4));
            args.SetValue(2, 0, Value(TestFixture::path_trial_graph));

            INFO("Execute test");
            REQUIRE_NOTHROW(bfs_exist_path.function(args, state, result));
            auto result_data = FlatVector::GetData<bool>(result);
            REQUIRE(result_data[0] == true);  
            INFO("Finish execute test");
        }
        SECTION("Non-existent vertex"){
            INFO("Set args");

            args.SetValue(0, 0, Value::BIGINT(0));
            args.SetValue(1, 0, Value::BIGINT(2));
            args.SetValue(2, 0, Value(TestFixture::path_trial_graph));

            INFO("Execute test");
            
            REQUIRE_NOTHROW(bfs_exist_path.function(args, state, result));
            auto result_data = FlatVector::GetData<bool>(result);
            REQUIRE(result_data[0] == false);  
            INFO("Finish execute test");
        }
        SECTION("Reverse path 2-hop"){
            INFO("Set args");

            args.SetValue(0, 0, Value::BIGINT(4));
            args.SetValue(1, 0, Value::BIGINT(1));
            args.SetValue(2, 0, Value(TestFixture::path_trial_graph));

            INFO("Execute test");
            REQUIRE_NOTHROW(bfs_exist_path.function(args, state, result));
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
            REQUIRE_NOTHROW(bfs_length_path.function(args, state, result));
            auto result_data = FlatVector::GetData<long long>(result);
            REQUIRE(result_data[0] == 1);  
            INFO("Finish execute test");
        }
        SECTION("Non-existent path"){
            INFO("Set args");

            args.SetValue(0, 0, Value::BIGINT(1));
            args.SetValue(1, 0, Value::BIGINT(7));
            args.SetValue(2, 0, Value(TestFixture::path_trial_graph));

            INFO("Execute test");
            REQUIRE_NOTHROW(bfs_length_path.function(args, state, result));
            auto result_data = FlatVector::GetData<long long>(result);
            REQUIRE(result_data[0] == -1);  
            INFO("Finish execute test");
        }
        SECTION("Reverse path 1-hop"){
            INFO("Set args");

            args.SetValue(0, 0, Value::BIGINT(2));
            args.SetValue(1, 0, Value::BIGINT(1));
            args.SetValue(2, 0, Value(TestFixture::path_trial_graph));

            INFO("Execute test");
            REQUIRE_NOTHROW(bfs_length_path.function(args, state, result));
            auto result_data = FlatVector::GetData<long long>(result);
            REQUIRE(result_data[0] == -1);  
            INFO("Finish execute test");
        }
        SECTION("2 hop"){
            INFO("Set args");

            args.SetValue(0, 0, Value::BIGINT(1));
            args.SetValue(1, 0, Value::BIGINT(4));
            args.SetValue(2, 0, Value(TestFixture::path_trial_graph));

            INFO("Execute test");
            REQUIRE_NOTHROW(bfs_length_path.function(args, state, result));
            auto result_data = FlatVector::GetData<long long>(result);
            REQUIRE(result_data[0] == 2);  
            INFO("Finish execute test");
        }
        SECTION("Non-existent vertex"){
            INFO("Set args");

            args.SetValue(0, 0, Value::BIGINT(0));
            args.SetValue(1, 0, Value::BIGINT(2));
            args.SetValue(2, 0, Value(TestFixture::path_trial_graph));

            INFO("Execute test");
            
            REQUIRE_NOTHROW(bfs_length_path.function(args, state, result));
            auto result_data = FlatVector::GetData<long long>(result);
            REQUIRE(result_data[0] == -1);  
            INFO("Finish execute test");
        }
        SECTION("Reverse path 2-hop"){
            INFO("Set args");

            args.SetValue(0, 0, Value::BIGINT(4));
            args.SetValue(1, 0, Value::BIGINT(1));
            args.SetValue(2, 0, Value(TestFixture::path_trial_graph));

            INFO("Execute test");
            REQUIRE_NOTHROW(bfs_length_path.function(args, state, result));
            auto result_data = FlatVector::GetData<long long>(result);
            REQUIRE(result_data[0] == -1);  
            INFO("Finish execute test");
        }
    }
}

TEMPLATE_TEST_CASE_METHOD(ScalarFunctionsFixture, "BFS Execute function for 500 hop", "[bfs]", FILE_TYPES_FOR_TEST) {
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
        
        SECTION("Long Path 500 hop"){
            args.SetValue(0, 0, Value::BIGINT(0));
            args.SetValue(1, 0, Value::BIGINT(528));
            args.SetValue(2, 0, Value(TestFixture::path_large_graph));
            REQUIRE_NOTHROW(bfs_exist_path.function(args, state, result));
            REQUIRE(FlatVector::GetData<bool>(result)[0] == true);
            BENCHMARK("Exists path") {
                bfs_exist_path.function(args, state, result);
                return FlatVector::GetData<bool>(result)[0];  
            };

            args.SetValue(0, 0, Value::BIGINT(0));
            args.SetValue(1, 0, Value::BIGINT(529));
            args.SetValue(2, 0, Value(TestFixture::path_large_graph));
            REQUIRE_NOTHROW(bfs_exist_path.function(args, state, result));
            REQUIRE(FlatVector::GetData<bool>(result)[0] == false);
            BENCHMARK("No Path") {
                bfs_exist_path.function(args, state, result);
                return FlatVector::GetData<bool>(result)[0];  
            };
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

        SECTION("Long Path 500 hop"){
            args.SetValue(0, 0, Value::BIGINT(0));
            args.SetValue(1, 0, Value::BIGINT(528));
            args.SetValue(2, 0, Value(TestFixture::path_large_graph));
            REQUIRE_NOTHROW(bfs_length_path.function(args, state, result));
            REQUIRE(FlatVector::GetData<long long>(result)[0] == 500);
            BENCHMARK("Exists path") {
                bfs_length_path.function(args, state, result);
                return FlatVector::GetData<long long>(result)[0];  
            };

            args.SetValue(0, 0, Value::BIGINT(0));
            args.SetValue(1, 0, Value::BIGINT(529));
            args.SetValue(2, 0, Value(TestFixture::path_large_graph));
            REQUIRE_NOTHROW(bfs_length_path.function(args, state, result));
            REQUIRE(FlatVector::GetData<long long>(result)[0] == -1);
            BENCHMARK("No Path") {
                bfs_length_path.function(args, state, result);
                return FlatVector::GetData<long long>(result)[0];  
            };
        }
    }
}