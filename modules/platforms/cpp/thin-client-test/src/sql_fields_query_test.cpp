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
#include <boost/thread/thread.hpp>

#include <ignite/ignition.h>

#include <ignite/thin/ignite_client_configuration.h>
#include <ignite/thin/ignite_client.h>
#include <ignite/thin/cache/cache_client.h>
#include <ignite/thin/cache/query/query_sql_fields.h>

#include <ignite/complex_type.h>
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
        cacheComplex = client.GetCache<int64_t, ignite::ComplexType>("cacheComplex");
    }

    ~SqlFieldsQueryTestSuiteFixture()
    {
        ignite::Ignition::StopAll(false);
    }

//private:
    /** Server node. */
    ignite::Ignite serverNode;

    /** Client. */
    IgniteClient client;

    /** Cache with TestType. */
    cache::CacheClient<int64_t, ignite::TestType> cacheAllFields;

    /** Cache with ComplexType. */
    cache::CacheClient<int64_t, ignite::ComplexType> cacheComplex;
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


bool IsCursorEmptyError(const ignite::IgniteError& err)
{
    return err.GetCode() == ignite::IgniteError::IGNITE_ERR_GENERIC &&
        std::string(err.GetText()) == "The cursor is empty";
}

void CheckCursorEmpty(QueryFieldsCursor& cursor)
{
    BOOST_CHECK(!cursor.HasNext());
    BOOST_CHECK_EXCEPTION(cursor.GetNext(), ignite::IgniteError, IsCursorEmptyError);
}

void CheckRowCursorEmpty(QueryFieldsRow& row)
{
    BOOST_CHECK(!row.HasNext());
    BOOST_CHECK_EXCEPTION(row.GetNext<int8_t>(), ignite::IgniteError, IsCursorEmptyError);
}


BOOST_FIXTURE_TEST_SUITE(SqlFieldsQueryTestSuite, SqlFieldsQueryTestSuiteFixture)

BOOST_AUTO_TEST_CASE(SqlFieldsQuerySelectEmpty)
{
    SqlFieldsQuery qry("select * from TestType");

    QueryFieldsCursor cursor = cacheAllFields.Query(qry);

    CheckCursorEmpty(cursor);
}

BOOST_AUTO_TEST_CASE(SqlFieldsQuerySelect)
{
    ignite::TestType val;

    val.i8Field = 1;
    val.i16Field = 2;
    val.i32Field = 4;
    val.i64Field = 8;
    val.strField = "Lorem ipsum";
    val.floatField = 16.0;
    val.doubleField = 32.0;
    val.boolField = false;
    val.guidField = ignite::Guid(0x1020304050607080, 0x9000A0B0C0D0E0F0);
    val.dateField = ignite::common::MakeDateGmt(2020, 12, 25);
    val.timeField = ignite::common::MakeTimeGmt(16, 45, 2);
    val.timestampField = ignite::common::MakeTimestampGmt(2021, 1, 12, 22, 3, 10, 42424242);

    val.i8ArrayField.push_back(9);
    val.i8ArrayField.push_back(6);
    val.i8ArrayField.push_back(3);
    val.i8ArrayField.push_back(42);

    cacheAllFields.Put(42, val);

    SqlFieldsQuery qry("SELECT i8Field, i16Field, i32Field, i64Field, strField, floatField, "
         "doubleField, boolField, guidField, dateField, timeField, timestampField, i8ArrayField FROM TestType");

    QueryFieldsCursor cursor = cacheAllFields.Query(qry);

    BOOST_CHECK(cursor.HasNext());

    QueryFieldsRow row = cursor.GetNext();

    BOOST_CHECK(row.HasNext());

    BOOST_CHECK_EQUAL(row.GetNext<int8_t>(), val.i8Field);
    BOOST_CHECK_EQUAL(row.GetNext<int16_t>(), val.i16Field);
    BOOST_CHECK_EQUAL(row.GetNext<int32_t>(), val.i32Field);
    BOOST_CHECK_EQUAL(row.GetNext<int64_t>(), val.i64Field);
    BOOST_CHECK_EQUAL(row.GetNext<std::string>(), val.strField);
    BOOST_CHECK_CLOSE(row.GetNext<float>(), val.floatField, 0.0001);
    BOOST_CHECK_CLOSE(row.GetNext<double>(), val.doubleField, 0.0001);
    BOOST_CHECK_EQUAL(row.GetNext<bool>(), val.boolField);
    BOOST_CHECK_EQUAL(row.GetNext<ignite::Guid>(), val.guidField);
    BOOST_CHECK(row.GetNext<ignite::Date>() == val.dateField);
    BOOST_CHECK(row.GetNext<ignite::Time>() == val.timeField);
    BOOST_CHECK(row.GetNext<ignite::Timestamp>() == val.timestampField);

    std::vector<int8_t> resArray(val.i8ArrayField.size());

    row.GetNextInt8Array(&resArray[0], static_cast<int32_t>(resArray.size()));

    BOOST_CHECK_EQUAL_COLLECTIONS(resArray.begin(), resArray.end(), val.i8ArrayField.begin(), val.i8ArrayField.end());

    CheckRowCursorEmpty(row);
    CheckCursorEmpty(cursor);
}

BOOST_AUTO_TEST_SUITE_END()
