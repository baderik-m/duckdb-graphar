#pragma once

#include "duckdb.hpp"
#include <string>

#include <graphar/api/high_level_writer.h>
#include <graphar/graph_info.h>
#include <graphar/status.h>


class TestGraphGenFixture {
public:
    duckdb::DuckDB db;
    duckdb::Connection con;

    TestGraphGenFixture(): db(nullptr), con(db);
    ~TestGraphGenFixture();    
}