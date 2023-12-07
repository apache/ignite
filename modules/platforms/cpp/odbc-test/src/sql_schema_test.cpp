/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <stdint.h>

#include <iterator>

#include <boost/test/unit_test.hpp>

#include <ignite/common/utils.h>

#include "ignite/cache/cache.h"
#include "ignite/cache/query/query_cursor.h"
#include "ignite/cache/query/query_sql_fields.h"
#include "ignite/ignite.h"
#include "ignite/ignition.h"

#include "odbc_test_suite.h"
#include "test_utils.h"
#include "ignite/odbc/odbc_error.h"

using namespace boost::unit_test;

using namespace ignite;
using namespace ignite::cache;
using namespace ignite::cache::query;

using namespace ignite_test;

/**
 * Ensure that cursor is empy fails.
 *
 * @param stmt Statement.
 */
void ChechEmptyCursorGetNextThrowsException(SQLHSTMT stmt)
{
    SQLRETURN ret = SQLFetch(stmt);

    BOOST_REQUIRE_EQUAL(ret, SQL_NO_DATA);
}

/**
 * Check single row through iteration.
 *
 * @param stmt Statement.
 * @param c1 First column.
 */
void CheckSingleLongRow1(SQLHSTMT stmt, const int64_t c1)
{
    int64_t val1 = 0;

    SQLRETURN ret = SQLFetch(stmt);
    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    ret = SQLGetData(stmt, 1, SQL_C_SBIGINT, &val1, 0, 0);
    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    BOOST_REQUIRE_EQUAL(val1, c1);

    ChechEmptyCursorGetNextThrowsException(stmt);
}

/**
 * Check row through iteration.
 *
 * @param stmt Statement.
 * @param c1 First column.
 * @param c2 Second column.
 */
void CheckStringRow2(SQLHSTMT stmt, const std::string& c1, const std::string& c2)
{
    char val1[1024] = { 0 };
    char val2[1024] = { 0 };
    SQLLEN val1Len = 0;
    SQLLEN val2Len = 0;

    SQLRETURN ret = SQLFetch(stmt);
    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    ret = SQLGetData(stmt, 1, SQL_C_CHAR, val1, sizeof(val1), &val1Len);
    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    ret = SQLGetData(stmt, 2, SQL_C_CHAR, val2, sizeof(val2), &val2Len);
    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    BOOST_REQUIRE_EQUAL(std::string(val1, static_cast<size_t>(val1Len)), c1);
    BOOST_REQUIRE_EQUAL(std::string(val2, static_cast<size_t>(val2Len)), c2);
}

/**
 * Check single row through iteration.
 *
 * @param stmt Statement.
 * @param c1 First column.
 * @param c2 Second column.
 */
void CheckSingleIntRow2(SQLHSTMT stmt, int32_t c1, int32_t c2)
{
    int32_t val1 = 0;
    int32_t val2 = 0;

    SQLRETURN ret = SQLFetch(stmt);
    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    ret = SQLGetData(stmt, 1, SQL_C_SLONG, &val1, 0, 0);
    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    ret = SQLGetData(stmt, 2, SQL_C_SLONG, &val2, 0, 0);
    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    BOOST_REQUIRE_EQUAL(val1, c1);
    BOOST_REQUIRE_EQUAL(val2, c2);

    ChechEmptyCursorGetNextThrowsException(stmt);
}

static const std::string TABLE_NAME = "T1";
static const std::string SCHEMA_NAME_1 = "SCHEMA_1";
static const std::string SCHEMA_NAME_2 = "SCHEMA_2";
static const std::string SCHEMA_NAME_3 = "ScHeMa3";
static const std::string Q_SCHEMA_NAME_3 = '"' + SCHEMA_NAME_3 + '"';
static const std::string SCHEMA_NAME_4 = "SCHEMA_4";

/**
 * Test setup fixture.
 */
struct SchemaTestSuiteFixture : odbc::OdbcTestSuite
{
    /**
     * Constructor.
     */
    SchemaTestSuiteFixture() :
        grid(StartNode("queries-schema.xml", "Node1"))
    {
        Connect("DRIVER={Apache Ignite};address=127.0.0.1:11110;schema=PUBLIC");
    }

    /**
     * Destructor.
     */
    ~SchemaTestSuiteFixture()
    {
        Ignition::StopAll(true);
    }

    /** Perform SQL in cluster. */
    void Sql(const std::string& sql)
    {
        SQLFreeStmt(stmt, SQL_CLOSE);

        SQLRETURN ret = ExecQuery(sql);

        ODBC_THROW_ON_ERROR(ret, SQL_HANDLE_STMT, stmt);
    }

    std::string TableName(bool withSchema)
    {
        return withSchema ? "PUBLIC." + TABLE_NAME : TABLE_NAME;
    }

    template<typename Predicate>
    void ExecuteStatementsAndVerify(Predicate& pred)
    {
        Sql("CREATE TABLE " + TableName(pred()) + " (id INT PRIMARY KEY, val INT)");

        Sql("CREATE INDEX t1_idx_1 ON " + TableName(pred()) + "(val)");

        Sql("INSERT INTO " + TableName(pred()) + " (id, val) VALUES(1, 2)");

        Sql("SELECT * FROM " + TableName(pred()));
        CheckSingleIntRow2(stmt, 1, 2);

        Sql("UPDATE " + TableName(pred()) + " SET val = 5");
        Sql("SELECT * FROM " + TableName(pred()));
        CheckSingleIntRow2(stmt, 1, 5);

        Sql("DELETE FROM " + TableName(pred()) + " WHERE id = 1");
        Sql("SELECT COUNT(*) FROM " + TableName(pred()));
        CheckSingleLongRow1(stmt, 0);

        Sql("SELECT COUNT(*) FROM SYS.TABLES WHERE schema_name = 'PUBLIC' "
            "AND table_name = \'" + TABLE_NAME + "\'");
        CheckSingleLongRow1(stmt, 1);

        Sql("DROP TABLE " + TableName(pred()));
    }

    /** Node started during the test. */
    Ignite grid;
};

BOOST_FIXTURE_TEST_SUITE(SchemaTestSuite, SchemaTestSuiteFixture)

bool TruePred()
{
    return true;
}

BOOST_AUTO_TEST_CASE(TestBasicOpsExplicitPublicSchema)
{
    ExecuteStatementsAndVerify(TruePred);
}

bool FalsePred()
{
    return false;
}

BOOST_AUTO_TEST_CASE(TestBasicOpsImplicitPublicSchema)
{
    ExecuteStatementsAndVerify(FalsePred);
}

struct MixedPred
{
    int i;

    MixedPred() : i(0)
    {
        // No-op.
    }

    bool operator()()
    {
        return (++i & 1) == 0;
    }
};

BOOST_AUTO_TEST_CASE(TestBasicOpsMixedPublicSchema)
{
    MixedPred pred;

    ExecuteStatementsAndVerify(pred);
}

BOOST_AUTO_TEST_CASE(TestCreateDropNonExistingSchema)
{
    BOOST_CHECK_THROW(
        Sql("CREATE TABLE UNKNOWN_SCHEMA." + TABLE_NAME + "(id INT PRIMARY KEY, val INT)"),
        OdbcClientError
    );

    BOOST_CHECK_THROW(
        Sql("DROP TABLE UNKNOWN_SCHEMA." + TABLE_NAME),
        OdbcClientError
    );
}

BOOST_AUTO_TEST_CASE(TestBasicOpsDiffSchemas)
{
    Sql("CREATE TABLE " + SCHEMA_NAME_1 + '.' + TABLE_NAME + " (s1_key INT PRIMARY KEY, s1_val INT)");
    Sql("CREATE TABLE " + SCHEMA_NAME_2 + '.' + TABLE_NAME + " (s2_key INT PRIMARY KEY, s2_val INT)");
    Sql("CREATE TABLE " + Q_SCHEMA_NAME_3 + '.' + TABLE_NAME + " (s3_key INT PRIMARY KEY, s3_val INT)");
    Sql("CREATE TABLE " + SCHEMA_NAME_4 + '.' + TABLE_NAME + " (s4_key INT PRIMARY KEY, s4_val INT)");

    Sql("INSERT INTO " + SCHEMA_NAME_1 + '.' + TABLE_NAME + " (s1_key, s1_val) VALUES (1, 2)");
    Sql("INSERT INTO " + SCHEMA_NAME_2 + '.' + TABLE_NAME + " (s2_key, s2_val) VALUES (1, 2)");
    Sql("INSERT INTO " + Q_SCHEMA_NAME_3 + '.' + TABLE_NAME + " (s3_key, s3_val) VALUES (1, 2)");
    Sql("INSERT INTO " + SCHEMA_NAME_4 + '.' + TABLE_NAME + " (s4_key, s4_val) VALUES (1, 2)");

    Sql("UPDATE " + SCHEMA_NAME_1 + '.' + TABLE_NAME + " SET s1_val = 5");
    Sql("UPDATE " + SCHEMA_NAME_2 + '.' + TABLE_NAME + " SET s2_val = 5");
    Sql("UPDATE " + Q_SCHEMA_NAME_3 + '.' + TABLE_NAME + " SET s3_val = 5");
    Sql("UPDATE " + SCHEMA_NAME_4 + '.' + TABLE_NAME + " SET s4_val = 5");

    Sql("DELETE FROM " + SCHEMA_NAME_1 + '.' + TABLE_NAME);
    Sql("DELETE FROM " + SCHEMA_NAME_2 + '.' + TABLE_NAME);
    Sql("DELETE FROM " + Q_SCHEMA_NAME_3 + '.' + TABLE_NAME);
    Sql("DELETE FROM " + SCHEMA_NAME_4 + '.' + TABLE_NAME);

    Sql("CREATE INDEX t1_idx_1 ON " + SCHEMA_NAME_1 + '.' + TABLE_NAME + "(s1_val)");
    Sql("CREATE INDEX t1_idx_1 ON " + SCHEMA_NAME_2 + '.' + TABLE_NAME + "(s2_val)");
    Sql("CREATE INDEX t1_idx_1 ON " + Q_SCHEMA_NAME_3 + '.' + TABLE_NAME + "(s3_val)");
    Sql("CREATE INDEX t1_idx_1 ON " + SCHEMA_NAME_4 + '.' + TABLE_NAME + "(s4_val)");

    Sql("SELECT * FROM " + SCHEMA_NAME_1 + '.' + TABLE_NAME);
    Sql("SELECT * FROM " + SCHEMA_NAME_2 + '.' + TABLE_NAME);
    Sql("SELECT * FROM " + Q_SCHEMA_NAME_3 + '.' + TABLE_NAME);
    Sql("SELECT * FROM " + SCHEMA_NAME_4 + '.' + TABLE_NAME);

    Sql("SELECT * FROM " + SCHEMA_NAME_1 + '.' + TABLE_NAME
      + " JOIN " + SCHEMA_NAME_2 + '.' + TABLE_NAME
      + " JOIN " + Q_SCHEMA_NAME_3 + '.' + TABLE_NAME
      + " JOIN " + SCHEMA_NAME_4 + '.' + TABLE_NAME);

    Sql("SELECT SCHEMA_NAME, KEY_ALIAS FROM SYS.TABLES ORDER BY SCHEMA_NAME");

    CheckStringRow2(stmt, SCHEMA_NAME_1, "S1_KEY");
    CheckStringRow2(stmt, SCHEMA_NAME_2, "S2_KEY");
    CheckStringRow2(stmt, SCHEMA_NAME_4, "S4_KEY");
    CheckStringRow2(stmt, SCHEMA_NAME_3, "S3_KEY");

    ChechEmptyCursorGetNextThrowsException(stmt);

    Sql("DROP TABLE " + SCHEMA_NAME_1 + '.' + TABLE_NAME);
    Sql("DROP TABLE " + SCHEMA_NAME_2 + '.' + TABLE_NAME);
    Sql("DROP TABLE " + Q_SCHEMA_NAME_3 + '.' + TABLE_NAME);
    Sql("DROP TABLE " + SCHEMA_NAME_4 + '.' + TABLE_NAME);
}

BOOST_AUTO_TEST_CASE(TestCreateTblsInDiffSchemasForSameCache)
{
    std::string testCache = "cache1";

    Sql("CREATE TABLE " + SCHEMA_NAME_1 + '.' + TABLE_NAME +
        " (s1_key INT PRIMARY KEY, s1_val INT) WITH \"cache_name=" + testCache + '"');

    BOOST_CHECK_THROW(
        Sql("CREATE TABLE " + SCHEMA_NAME_2 + '.' + TABLE_NAME +
            " (s1_key INT PRIMARY KEY, s2_val INT) WITH \"cache_name=" + testCache + '"'),
        OdbcClientError
    );
}

BOOST_AUTO_TEST_SUITE_END()
