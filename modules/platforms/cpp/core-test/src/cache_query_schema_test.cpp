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
#include "ignite/test_utils.h"

using namespace boost::unit_test;

using namespace ignite;
using namespace ignite::cache;
using namespace ignite::cache::query;
using namespace ignite::common;

using ignite::impl::binary::BinaryUtils;

/**
 * Ensure that cursor is empy fails.
 *
 * @param cur Cursor.
 */
template<typename Cursor>
void ChechEmptyCursorGetNextThrowsException(Cursor& cur)
{
    BOOST_REQUIRE(!cur.HasNext());
    BOOST_CHECK_EXCEPTION(cur.GetNext(), IgniteError, ignite_test::IsGenericError);
}

/**
 * Check single row through iteration.
 *
 * @param cur Cursor.
 * @param c1 First column.
 */
template<typename T1>
void CheckSingleRow(QueryFieldsCursor& cur, const T1& c1)
{
    BOOST_REQUIRE(cur.HasNext());

    QueryFieldsRow row = cur.GetNext();

    BOOST_REQUIRE_EQUAL(row.GetNext<T1>(), c1);

    BOOST_REQUIRE(!row.HasNext());

    ChechEmptyCursorGetNextThrowsException(cur);
}

/**
 * Check row through iteration.
 *
 * @param cur Cursor.
 * @param c1 First column.
 */
template<typename T1, typename T2>
void CheckRow(QueryFieldsCursor& cur, const T1& c1, const T2& c2)
{
    BOOST_REQUIRE(cur.HasNext());

    QueryFieldsRow row = cur.GetNext();

    BOOST_REQUIRE_EQUAL(row.GetNext<T1>(), c1);
    BOOST_REQUIRE_EQUAL(row.GetNext<T2>(), c2);

    BOOST_REQUIRE(!row.HasNext());
}

/**
 * Check single row through iteration.
 *
 * @param cur Cursor.
 * @param c1 First column.
 */
template<typename T1, typename T2>
void CheckSingleRow(QueryFieldsCursor& cur, const T1& c1, const T2& c2)
{
    CheckRow<T1, T2>(cur, c1, c2);

    ChechEmptyCursorGetNextThrowsException(cur);
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
struct CacheQuerySchemaTestSuiteFixture
{
    Ignite StartNode(const char* name)
    {
#ifdef IGNITE_TESTS_32
        return ignite_test::StartNode("cache-query-schema-32.xml", name);
#else
        return ignite_test::StartNode("cache-query-schema.xml", name);
#endif
    }

    /**
     * Constructor.
     */
    CacheQuerySchemaTestSuiteFixture() :
        grid(StartNode("Node1"))
    {
        // No-op.
    }

    /**
     * Destructor.
     */
    ~CacheQuerySchemaTestSuiteFixture()
    {
        Ignition::StopAll(true);
    }

    /** Perform SQL in cluster. */
    QueryFieldsCursor Sql(const std::string& sql)
    {
        return grid
            .GetOrCreateCache<int, int>("SchemaTestCache")
            .Query(SqlFieldsQuery(sql));
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

        QueryFieldsCursor cursor = Sql("SELECT * FROM " + TableName(pred()));
        CheckSingleRow<int32_t, int32_t>(cursor, 1, 2);

        Sql("UPDATE " + TableName(pred()) + " SET val = 5");
        cursor = Sql("SELECT * FROM " + TableName(pred()));
        CheckSingleRow<int32_t, int32_t>(cursor, 1, 5);

        Sql("DELETE FROM " + TableName(pred()) + " WHERE id = 1");
        cursor = Sql("SELECT COUNT(*) FROM " + TableName(pred()));
        CheckSingleRow<int64_t>(cursor, 0);

        cursor = Sql("SELECT COUNT(*) FROM SYS.TABLES WHERE schema_name = 'PUBLIC' "
            "AND table_name = \'" + TABLE_NAME + "\'");
        CheckSingleRow<int64_t>(cursor, 1);

        Sql("DROP TABLE " + TableName(pred()));
    }

    /** Node started during the test. */
    Ignite grid;
};

BOOST_FIXTURE_TEST_SUITE(CacheQuerySchemaTestSuite, CacheQuerySchemaTestSuiteFixture)

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
        IgniteError
    );

    BOOST_CHECK_THROW(
        Sql("DROP TABLE UNKNOWN_SCHEMA." + TABLE_NAME),
        IgniteError
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

    QueryFieldsCursor cursor = Sql("SELECT SCHEMA_NAME, KEY_ALIAS FROM SYS.TABLES ORDER BY SCHEMA_NAME");

    CheckRow<std::string, std::string>(cursor, SCHEMA_NAME_1, "S1_KEY");
    CheckRow<std::string, std::string>(cursor, SCHEMA_NAME_2, "S2_KEY");
    CheckRow<std::string, std::string>(cursor, SCHEMA_NAME_4, "S4_KEY");
    CheckRow<std::string, std::string>(cursor, SCHEMA_NAME_3, "S3_KEY");

    ChechEmptyCursorGetNextThrowsException(cursor);

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
        IgniteError
    );
}

BOOST_AUTO_TEST_SUITE_END()
