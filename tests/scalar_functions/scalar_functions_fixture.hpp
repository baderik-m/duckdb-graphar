#pragma once
#include "basic_graphar_fixture.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
// class TestExpressionState : public ExpressionState {
//     public:
//         TestExpressionState(ExecutionContext& context) : ExpressionState(nullptr, context) {
//             expr = make_uniq<ParsedExpression>(ExpressionType::VALUE_CONSTANT, LogicalType::BOOLEAN);
//         }
//     } test_state(state);

template <typename FileTypeTag> 
class ScalarFunctionsFixture: public BasicGrapharFixture<FileTypeTag> {
protected:
    std::string path_trial_graph;
    std::string path_trial_feature_graph;
    struct TestExpressionState : public duckdb::ExpressionState {
    std::shared_ptr<duckdb::ExpressionExecutor> executor_owner;

    TestExpressionState(
        const duckdb::BoundFunctionExpression &expr,
        duckdb::ExpressionExecutorState &root,
        std::shared_ptr<duckdb::ExpressionExecutor> executor)
        : duckdb::ExpressionState(expr, root), executor_owner(std::move(executor)) {}
    };
public:
    ~ScalarFunctionsFixture() = default;
    ScalarFunctionsFixture(): BasicGrapharFixture<FileTypeTag>() {
        path_trial_graph = BasicGrapharFixture<FileTypeTag>::CreateTestGraph(
            "trial", 
            {
                VerticesSchema(
                    "Person", 1024, 
                    {PropertySchema("hash_phone_no", "int32", false, true)}, 
                    {
                        {1, {{"hash_phone_no", int32_t{10}}}}, 
                        {2, {{"hash_phone_no", int32_t{20}}}}, 
                        {3, {{"hash_phone_no", int32_t{30}}}}, 
                        {4, {{"hash_phone_no", int32_t{40}}}}, 
                        {5, {{"hash_phone_no", int32_t{50}}}}
                    }
                )
            }, 
            {
                EdgesSchema(
                    "Person", "knows", "Person", 0, false, 
                    {}, 
                    {
                        {1, 2}, 
                        {1, 3}, 
                        {2, 3}, 
                        {2, 4}, 
                        {3, 4}, 
                        {3, 5}, 
                        {4, 5}
                    },
                    5
                )
            }
        );
        path_trial_feature_graph = BasicGrapharFixture<FileTypeTag>::CreateTestGraph(
            "trial_f", 
            {
                VerticesSchema(
                    "Person", 1024, 
                    {
                        PropertySchema("hash_phone_no", "int32", false, true), 
                        PropertySchema("first_name", "string", false, false),
                        PropertySchema("last_name", "string", false, false)
                    }, 
                    {
                        {1, {{"hash_phone_no", int32_t{10}}, {"first_name", std::string{"Emily"}}, {"last_name", std::string{"Johnson"}}}}, 
                        {2, {{"hash_phone_no", int32_t{20}}, {"first_name", std::string{"James"}}, {"last_name", std::string{"Wilson"}}}}, 
                        {3, {{"hash_phone_no", int32_t{30}}, {"first_name", std::string{"Olivia"}}, {"last_name", std::string{"Brown"}}}}, 
                        {4, {{"hash_phone_no", int32_t{40}}, {"first_name", std::string{"Benjamin"}}, {"last_name", std::string{"Taylor"}}}}, 
                        {5, {{"hash_phone_no", int32_t{50}}, {"first_name", std::string{"Sophia"}}, {"last_name", std::string{"Martinez"}}}}
                    }
                )
            }, 
            {
                EdgesSchema(
                    "Person", "knows", "Person", 0, false, 
                    {PropertySchema("friend_score", "int32", false, false), PropertySchema("created_at", "string", false, false), PropertySchema("tmp_", "float", false, false)}, 
                    {
                        {1, 2, {{"friend_score", int32_t{1}}, {"created_at", std::string{"2021-01-01"}}, {"tmp_", float{0.1}}}}, 
                        {1, 3, {{"friend_score", int32_t{2}}, {"created_at", std::string{"2022-01-01"}}, {"tmp_", float{0.1}}}}, 
                        {2, 3, {{"friend_score", int32_t{3}}, {"created_at", std::string{"2021-11-01"}}, {"tmp_", float{0.1}}}}, 
                        {2, 4, {{"friend_score", int32_t{4}}, {"created_at", std::string{"2021-01-01"}}, {"tmp_", float{0.1}}}}, 
                        {3, 4, {{"friend_score", int32_t{1}}, {"created_at", std::string{"2021-01-01"}}, {"tmp_", float{0.1}}}}, 
                        {3, 5, {{"friend_score", int32_t{1}}, {"created_at", std::string{"2021-01-01"}}, {"tmp_", float{0.1}}}}, 
                        {4, 5, {{"friend_score", int32_t{1}}, {"created_at", std::string{"2021-01-01"}}, {"tmp_", float{0.1}}}}
                    },
                    5
                )
            }
        );
        
        
    };

    std::shared_ptr<duckdb::ExpressionState> MockingState(
        duckdb::LogicalType return_type,
        duckdb::ScalarFunction func,
        std::shared_ptr<duckdb::ExpressionExecutor> executor
    ) {
        auto bound_expr = std::make_shared<duckdb::BoundFunctionExpression>(
            return_type, func, std::vector<duckdb::unique_ptr<duckdb::Expression>>{}, nullptr, false
        );

        auto executor_state = std::make_shared<duckdb::ExpressionExecutorState>();
        executor_state->executor = executor.get();

        auto expression_state = std::make_shared<duckdb::ExpressionState>(*bound_expr, *executor_state);

        return expression_state;
    }
};
