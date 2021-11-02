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

#include <boost/test/unit_test.hpp>

#include <ignite/ignite_error.h>
#include <ignite/ignition.h>

#include <ignite/thin/ignite_client_configuration.h>
#include <ignite/thin/ignite_client.h>

#include <ignite/test_type.h>
#include <test_utils.h>

using namespace ignite::thin;
using namespace ignite::thin::cache::query;
using namespace boost::unit_test;

class SqlFieldsQueryTestSuiteFixture
{
public:
    static ignite::Ignite StartNode(const char* name)
    {
        return ignite_test::StartCrossPlatformServerNode("sql-query-fields.xml", name);
    }

    SqlFieldsQueryTestSuiteFixture()
    {
        serverNode = StartNode("ServerNode");

        IgniteClientConfiguration cfg;

        cfg.SetEndPoints("127.0.0.1:11110");

        client = IgniteClient::Start(cfg);

        cacheAllFields = client.GetCache<int64_t, ignite::TestType>("cacheAllFields");
    }

    ~SqlFieldsQueryTestSuiteFixture()
    {
        ignite::Ignition::StopAll(false);
    }

    /**
     * Perform an invalid SQL query and check whether returned SQLSTATE is expected.
     */
    void CheckSqlStateForQuery(const std::string& sql, const std::string& expectedSqlState)
    {
        SqlFieldsQuery testQry(sql);
        testQry.SetSchema("PUBLIC");

        try
        {
            cacheAllFields.Query(testQry);

            BOOST_FAIL("Expected to get SQL error here");
        }
        catch (ignite::IgniteError& err)
        {
            std::string msg(err.GetText());

            std::string sqlState(msg.substr(0, msg.find(':')));

            BOOST_CHECK_EQUAL(expectedSqlState, sqlState);
        }
    }

protected:
    /** Server node. */
    ignite::Ignite serverNode;

    /** Client. */
    IgniteClient client;

    /** Cache with TestType. */
    cache::CacheClient<int64_t, ignite::TestType> cacheAllFields;
};

BOOST_AUTO_TEST_SUITE(SqlFieldsQueryBasicTestSuite)

BOOST_AUTO_TEST_CASE(SqlFieldsQueryDefaults)
{
    std::string sql("select * from TestType");

    SqlFieldsQuery qry(sql);

    BOOST_CHECK_EQUAL(qry.GetSql(), sql);
    BOOST_CHECK_EQUAL(qry.GetTimeout(), 0);
    BOOST_CHECK_EQUAL(qry.GetMaxRows(), 0);
    BOOST_CHECK_EQUAL(qry.GetPageSize(), 1024);
    BOOST_CHECK_EQUAL(qry.GetSchema(), std::string());

    BOOST_CHECK(!qry.IsLocal());
    BOOST_CHECK(!qry.IsCollocated());
    BOOST_CHECK(!qry.IsDistributedJoins());
    BOOST_CHECK(!qry.IsEnforceJoinOrder());
    BOOST_CHECK(!qry.IsLazy());
}

BOOST_AUTO_TEST_CASE(SqlFieldsQuerySetGet)
{
    std::string sql("select * from TestType");

    SqlFieldsQuery qry(sql);

    qry.SetTimeout(1000);
    qry.SetMaxRows(100);
    qry.SetPageSize(4096);
    qry.SetSchema("PUBLIC");

    qry.SetLocal(true);
    qry.SetCollocated(true);
    qry.SetDistributedJoins(true);
    qry.SetEnforceJoinOrder(true);
    qry.SetLazy(true);

    BOOST_CHECK_EQUAL(qry.GetSql(), sql);
    BOOST_CHECK_EQUAL(qry.GetTimeout(), 1000);
    BOOST_CHECK_EQUAL(qry.GetMaxRows(), 100);
    BOOST_CHECK_EQUAL(qry.GetPageSize(), 4096);
    BOOST_CHECK_EQUAL(qry.GetSchema(), std::string("PUBLIC"));

    BOOST_CHECK(qry.IsLocal());
    BOOST_CHECK(qry.IsCollocated());
    BOOST_CHECK(qry.IsDistributedJoins());
    BOOST_CHECK(qry.IsEnforceJoinOrder());
    BOOST_CHECK(qry.IsLazy());
}

BOOST_AUTO_TEST_SUITE_END()

/**
 * Check that error empty cursor error.
 *
 * @param err Error.
 */
bool IsCursorEmptyError(const ignite::IgniteError& err)
{
    return err.GetCode() == ignite::IgniteError::IGNITE_ERR_GENERIC &&
        std::string(err.GetText()) == "The cursor is empty";
}

/**
 * Check that cursor is empty.
 *
 * @param cursor Cursor.
 */
void CheckCursorEmpty(QueryFieldsCursor& cursor)
{
    BOOST_CHECK(!cursor.HasNext());
    BOOST_CHECK_EXCEPTION(cursor.GetNext(), ignite::IgniteError, IsCursorEmptyError);
}

/**
 * Check that row is empty.
 *
 * @param row Row.
 */
void CheckRowCursorEmpty(QueryFieldsRow& row)
{
    BOOST_CHECK(!row.HasNext());
    BOOST_CHECK_EXCEPTION(row.GetNext<int8_t>(), ignite::IgniteError, IsCursorEmptyError);
}

/**
 * Check that row columns equal value fields.
 *
 * @param row Row.
 * @param val Value.
 */
void CheckRowEqualsValue(QueryFieldsRow& row, const ignite::TestType& val)
{
    BOOST_CHECK_EQUAL(row.GetNext<int8_t>(), val.i8Field);
    BOOST_CHECK_EQUAL(row.GetNext<int16_t>(), val.i16Field);
    BOOST_CHECK_EQUAL(row.GetNext<int32_t>(), val.i32Field);
    BOOST_CHECK_EQUAL(row.GetNext<int64_t>(), val.i64Field);
    BOOST_CHECK_EQUAL(row.GetNext<std::string>(), val.strField);
    BOOST_CHECK_CLOSE(row.GetNext<float>(), val.floatField, 0.0001f);
    BOOST_CHECK_CLOSE(row.GetNext<double>(), val.doubleField, 0.0001);
    BOOST_CHECK_EQUAL(row.GetNext<bool>(), val.boolField);
    BOOST_CHECK_EQUAL(row.GetNext<ignite::Guid>(), val.guidField);
    BOOST_CHECK(row.GetNext<ignite::Date>() == val.dateField);
    BOOST_CHECK_EQUAL(row.GetNext<ignite::Time>().GetMilliseconds(), val.timeField.GetMilliseconds());
    BOOST_CHECK(row.GetNext<ignite::Timestamp>() == val.timestampField);

    std::vector<int8_t> resArray = row.GetNext< std::vector<int8_t> >();

    BOOST_CHECK_EQUAL_COLLECTIONS(
            resArray.begin(), resArray.end(),
            val.i8ArrayField.begin(), val.i8ArrayField.end());
}

/**
 * Make custom test value.
 *
 * @param seed Seed to generate value.
 */
ignite::TestType MakeCustomTestValue(int32_t seed)
{
    ignite::TestType val;

    val.i8Field = seed;
    val.i16Field = 2 * seed;
    val.i32Field = 4 * seed;
    val.i64Field = 8 * seed;
    val.strField = "Lorem ipsum";
    val.floatField = 16.0f * seed;
    val.doubleField = 32.0 * seed;
    val.boolField = ((seed % 2) == 0);
    val.guidField = ignite::Guid(0x1020304050607080 * seed, 0x9000A0B0C0D0E0F0 * seed);
    val.dateField = ignite::Date(235682736 * seed);
    val.timeField = ignite::Time((124523 * seed) % (24 * 60 * 60 * 1000));
    val.timestampField = ignite::Timestamp(128341594123 * seed);

    val.i8ArrayField.push_back(9 * seed);
    val.i8ArrayField.push_back(6 * seed);
    val.i8ArrayField.push_back(3 * seed);
    val.i8ArrayField.push_back(42 * seed);

    return val;
}


BOOST_FIXTURE_TEST_SUITE(SqlFieldsQueryTestSuite, SqlFieldsQueryTestSuiteFixture)

BOOST_AUTO_TEST_CASE(SelectEmpty)
{
    SqlFieldsQuery qry("select * from TestType");

    QueryFieldsCursor cursor = cacheAllFields.Query(qry);

    CheckCursorEmpty(cursor);

    const std::vector<std::string>& columns = cursor.GetColumnNames();

    BOOST_CHECK_EQUAL(columns.size(), 13);

    BOOST_CHECK_EQUAL(columns.at(0), "I8FIELD");
    BOOST_CHECK_EQUAL(columns.at(1), "I16FIELD");
    BOOST_CHECK_EQUAL(columns.at(2), "I32FIELD");
    BOOST_CHECK_EQUAL(columns.at(3), "I64FIELD");
    BOOST_CHECK_EQUAL(columns.at(4), "STRFIELD");
    BOOST_CHECK_EQUAL(columns.at(5), "FLOATFIELD");
    BOOST_CHECK_EQUAL(columns.at(6), "DOUBLEFIELD");
    BOOST_CHECK_EQUAL(columns.at(7), "BOOLFIELD");
    BOOST_CHECK_EQUAL(columns.at(8), "GUIDFIELD");
    BOOST_CHECK_EQUAL(columns.at(9), "DATEFIELD");
    BOOST_CHECK_EQUAL(columns.at(10), "TIMEFIELD");
    BOOST_CHECK_EQUAL(columns.at(11), "TIMESTAMPFIELD");
    BOOST_CHECK_EQUAL(columns.at(12), "I8ARRAYFIELD");
}

BOOST_AUTO_TEST_CASE(SelectSingleValue)
{
    ignite::TestType val = MakeCustomTestValue(1);

    cacheAllFields.Put(42, val);

    SqlFieldsQuery qry("select i8Field, i16Field, i32Field, i64Field, strField, floatField, "
        "doubleField, boolField, guidField, dateField, timeField, timestampField, i8ArrayField FROM TestType");

    QueryFieldsCursor cursor = cacheAllFields.Query(qry);

    BOOST_CHECK(cursor.HasNext());

    QueryFieldsRow row = cursor.GetNext();

    BOOST_CHECK(row.HasNext());

    CheckRowEqualsValue(row, val);

    CheckRowCursorEmpty(row);
    CheckCursorEmpty(cursor);
}

BOOST_AUTO_TEST_CASE(SelectTwoValues)
{
    ignite::TestType val1 = MakeCustomTestValue(1);
    ignite::TestType val2 = MakeCustomTestValue(2);

    cacheAllFields.Put(1, val1);
    cacheAllFields.Put(2, val2);

    SqlFieldsQuery qry("select i8Field, i16Field, i32Field, i64Field, strField, floatField, "
        "doubleField, boolField, guidField, dateField, timeField, timestampField, i8ArrayField FROM TestType "
        "ORDER BY _key");

    QueryFieldsCursor cursor = cacheAllFields.Query(qry);

    BOOST_CHECK(cursor.HasNext());
    QueryFieldsRow row = cursor.GetNext();
    BOOST_CHECK(row.HasNext());

    CheckRowEqualsValue(row, val1);
    CheckRowCursorEmpty(row);

    BOOST_CHECK(cursor.HasNext());
    row = cursor.GetNext();
    BOOST_CHECK(row.HasNext());

    CheckRowEqualsValue(row, val2);

    CheckRowCursorEmpty(row);
    CheckCursorEmpty(cursor);
}

BOOST_AUTO_TEST_CASE(Select10000Values)
{
    const int32_t num = 10000;

    std::map<int64_t, ignite::TestType> values;

    for (int32_t i = 0; i < num; ++i)
        values[i] = MakeCustomTestValue(i);

    BOOST_CHECK_EQUAL(values.size(), static_cast<size_t>(num));

    cacheAllFields.PutAll(values);

    SqlFieldsQuery qry("select i8Field, i16Field, i32Field, i64Field, strField, floatField, "
        "doubleField, boolField, guidField, dateField, timeField, timestampField, i8ArrayField FROM TestType "
        "ORDER BY _key");

    QueryFieldsCursor cursor = cacheAllFields.Query(qry);

    for (int64_t i = 0; i < num; ++i)
    {
        BOOST_CHECK(cursor.HasNext());
        QueryFieldsRow row = cursor.GetNext();
        BOOST_CHECK(row.HasNext());

        CheckRowEqualsValue(row, values[i]);
        CheckRowCursorEmpty(row);
    }

    CheckCursorEmpty(cursor);
}

BOOST_AUTO_TEST_CASE(Select10ValuesPageSize1)
{
    const int32_t num = 10;

    std::map<int64_t, ignite::TestType> values;

    for (int32_t i = 0; i < num; ++i)
        values[i] = MakeCustomTestValue(i);

    BOOST_CHECK_EQUAL(values.size(), static_cast<size_t>(num));

    cacheAllFields.PutAll(values);

    SqlFieldsQuery qry("select i8Field, i16Field, i32Field, i64Field, strField, floatField, "
                       "doubleField, boolField, guidField, dateField, timeField, timestampField, i8ArrayField FROM TestType "
                       "ORDER BY _key");

    qry.SetPageSize(1);

    QueryFieldsCursor cursor = cacheAllFields.Query(qry);

    for (int64_t i = 0; i < num; ++i)
    {
        BOOST_CHECK(cursor.HasNext());
        QueryFieldsRow row = cursor.GetNext();
        BOOST_CHECK(row.HasNext());

        CheckRowEqualsValue(row, values[i]);
        CheckRowCursorEmpty(row);
    }

    CheckCursorEmpty(cursor);
}


BOOST_AUTO_TEST_CASE(SelectKeyValue)
{
    const int64_t key = 123;
    const ignite::TestType val = MakeCustomTestValue(1);

    cacheAllFields.Put(key, val);

    SqlFieldsQuery qry("select _key, _val FROM TestType");

    QueryFieldsCursor cursor = cacheAllFields.Query(qry);

    BOOST_CHECK(cursor.HasNext());

    QueryFieldsRow row = cursor.GetNext();

    BOOST_CHECK(row.HasNext());

    const int64_t keyActual = row.GetNext<int64_t>();
    const ignite::TestType valActual = row.GetNext<ignite::TestType>();

    BOOST_CHECK_EQUAL(key, keyActual);
    BOOST_CHECK(val == valActual);

    CheckRowCursorEmpty(row);
    CheckCursorEmpty(cursor);
}

BOOST_AUTO_TEST_CASE(SelectTwoValuesInDifferentOrder)
{
    typedef ignite::common::concurrent::SharedPointer<void> SP_Void;

    ignite::TestType val1 = MakeCustomTestValue(1);
    ignite::TestType val2 = MakeCustomTestValue(2);

    cacheAllFields.Put(1, val1);
    cacheAllFields.Put(2, val2);

    SqlFieldsQuery qry("select i8Field, i16Field, i32Field, i64Field, strField, floatField, "
        "doubleField, boolField, guidField, dateField, timeField, timestampField, i8ArrayField FROM TestType "
        "ORDER BY _key");

    // Checking if everything going to be OK if we destroy cursor before fetching rows.
    QueryFieldsRow row1 = QueryFieldsRow(SP_Void());
    QueryFieldsRow row2 = QueryFieldsRow(SP_Void());

    {
        QueryFieldsCursor cursor = cacheAllFields.Query(qry);

        BOOST_CHECK(cursor.HasNext());
        row1 = cursor.GetNext();

        BOOST_CHECK(cursor.HasNext());
        row2 = cursor.GetNext();

        CheckCursorEmpty(cursor);
    }

    BOOST_CHECK(row2.HasNext());
    CheckRowEqualsValue(row2, val2);
    CheckRowCursorEmpty(row2);

    BOOST_CHECK(row1.HasNext());
    CheckRowEqualsValue(row1, val1);
    CheckRowCursorEmpty(row1);
}

BOOST_AUTO_TEST_CASE(CreateTableInsertSelect)
{
    SqlFieldsQuery qry("create table TestTable(id int primary key, val int)");
    qry.SetSchema("PUBLIC");

    QueryFieldsCursor cursor = cacheAllFields.Query(qry);

    QueryFieldsRow row = cursor.GetNext();
    BOOST_CHECK(row.HasNext());
    BOOST_CHECK_EQUAL(row.GetNext<int64_t>(), 0);
    CheckRowCursorEmpty(row);

    qry.SetSql("insert into TestTable(id, val) values(1, 2)");

    cursor = cacheAllFields.Query(qry);

    row = cursor.GetNext();
    BOOST_CHECK(row.HasNext());
    BOOST_CHECK_EQUAL(row.GetNext<int64_t>(), 1);
    CheckRowCursorEmpty(row);

    qry.SetSql("select id, val from TestTable");

    cursor = cacheAllFields.Query(qry);

    BOOST_CHECK(cursor.HasNext());

    row = cursor.GetNext();

    BOOST_CHECK(row.HasNext());
    BOOST_CHECK_EQUAL(row.GetNext<int32_t>(), 1);
    BOOST_CHECK_EQUAL(row.GetNext<int32_t>(), 2);
    CheckRowCursorEmpty(row);

    CheckCursorEmpty(cursor);
}


/**
 * Test that null value can be inserted using SQL query.
 */
BOOST_AUTO_TEST_CASE(TestInsertNull)
{
    const int64_t testKey(111);
    SqlFieldsQuery insertPersonQry("INSERT INTO TestType(_key, strField, i32Field) VALUES(?, ?, ?)");

    insertPersonQry.AddArgument<int64_t>(testKey);
    insertPersonQry.AddArgument<std::string*>(0);
    insertPersonQry.AddArgument<int32_t*>(0);

    cacheAllFields.Query(insertPersonQry);

    SqlFieldsQuery select("Select _key from TestType WHERE strField is NULL AND i32Field is NULL");

    QueryFieldsCursor cursor = cacheAllFields.Query(select);

    BOOST_CHECK(cursor.HasNext());

    QueryFieldsRow row = cursor.GetNext();

    BOOST_CHECK(row.HasNext());
    BOOST_CHECK_EQUAL(row.GetNext<int64_t>(), testKey);
    CheckRowCursorEmpty(row);

    CheckCursorEmpty(cursor);
}


/**
 * Test that SQL errors contain SQLSTATE with cause.
 */
BOOST_AUTO_TEST_CASE(TestSqlStateOnErrors)
{
    CheckSqlStateForQuery("select * from \"UnknownCache\".UNKNOWN_TABLE", "42000");
    CheckSqlStateForQuery("select * from UNKNOWN_TABLE", "42000");
    CheckSqlStateForQuery("insert into \"cacheAllFields\".TestType(_key) values(null)", "22004");
    CheckSqlStateForQuery("insert into \"cacheAllFields\".TestType(_key) values('abc')", "0700B");
    CheckSqlStateForQuery("insert into \"cacheAllFields\".TestType(_key, _val) values(1, null)", "0A000");

    SqlFieldsQuery qry("CREATE TABLE PUBLIC.varchar_table(id INT PRIMARY KEY, str VARCHAR(5))");
    cacheAllFields.Query(qry);

    CheckSqlStateForQuery("insert into varchar_table(id, str) values(1, 'too_long')", "23000");
}

BOOST_AUTO_TEST_SUITE_END()
