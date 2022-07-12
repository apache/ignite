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

class ScanQueryTestSuiteFixture
{
public:
    static ignite::Ignite StartNode(const char* name)
    {
        return ignite_test::StartCrossPlatformServerNode("sql-query-fields.xml", name);
    }

    ScanQueryTestSuiteFixture()
    {
        serverNode = StartNode("ServerNode");

        IgniteClientConfiguration cfg;

        cfg.SetEndPoints("127.0.0.1:11110");

        client = IgniteClient::Start(cfg);

        cacheAllFields = client.GetCache<int64_t, ignite::TestType>("cacheAllFields");
    }

    ~ScanQueryTestSuiteFixture()
    {
        ignite::Ignition::StopAll(false);
    }

protected:
    /** Server node. */
    ignite::Ignite serverNode;

    /** Client. */
    IgniteClient client;

    /** Cache with TestType. */
    cache::CacheClient<int64_t, ignite::TestType> cacheAllFields;
};

BOOST_AUTO_TEST_SUITE(ScanQueryBasicTestSuite)

BOOST_AUTO_TEST_CASE(ScanQueryDefaults)
{
    ScanQuery qry;

    BOOST_CHECK_EQUAL(qry.GetPartition(), -1);
    BOOST_CHECK_EQUAL(qry.GetPageSize(), 1024);

    BOOST_CHECK(!qry.IsLocal());
}

BOOST_AUTO_TEST_CASE(ScanQuerySetGet)
{
    ScanQuery qry;

    qry.SetPageSize(4096);
    qry.SetPartition(200);

    qry.SetLocal(true);

    BOOST_CHECK_EQUAL(qry.GetPageSize(), 4096);
    BOOST_CHECK_EQUAL(qry.GetPartition(), 200);

    BOOST_CHECK(qry.IsLocal());
}

BOOST_AUTO_TEST_SUITE_END()

namespace
{
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
    template<typename K, typename V>
    void CheckCursorEmpty(QueryCursor<K, V>& cursor)
    {
        BOOST_CHECK(!cursor.HasNext());
        BOOST_CHECK_EXCEPTION(cursor.GetNext(), ignite::IgniteError, IsCursorEmptyError);
    }
    /**
     * Check empty result through GetAll().
     *
     * @param cur Cursor.
     */
    template<typename K, typename V>
    void CheckEmptyGetAll(QueryCursor<K, V>& cur)
    {
        std::vector<cache::CacheEntry<K, V> > res;

        cur.GetAll(res);

        BOOST_REQUIRE(res.size() == 0);

        CheckCursorEmpty(cur);
    }

    /**
     * Check empty result through iter version of GetAll().
     *
     * @param cur Cursor.
     */
    template<typename K, typename V>
    void CheckEmptyGetAllIter(QueryCursor<K, V>& cur)
    {
        std::vector<cache::CacheEntry<K, V> > res;

        cur.GetAll(std::back_inserter(res));

        BOOST_REQUIRE(res.size() == 0);

        CheckCursorEmpty(cur);
    }

    /**
     * Check single result through iteration.
     *
     * @param cur Cursor.
     * @param key Key.
     * @param value Value.
     */
    template<typename K, typename V>
    void CheckSingle(QueryCursor<K, V>& cur, const K& key, const V& value)
    {
        BOOST_REQUIRE(cur.HasNext());

        cache::CacheEntry<K, V> entry = cur.GetNext();

        BOOST_REQUIRE(entry.GetKey() == key);
        BOOST_REQUIRE(entry.GetValue() == value);

        CheckCursorEmpty(cur);
    }

    /**
     * Check single result through GetAll().
     *
     * @param cur Cursor.
     * @param key Key.
     * @param value Value.
     */
    template<typename K, typename V>
    void CheckSingleGetAll(QueryCursor<K, V>& cur, const K& key, const V& value)
    {
        std::vector< cache::CacheEntry<K, V> > res;

        cur.GetAll(res);

        CheckCursorEmpty(cur);

        BOOST_CHECK_EQUAL(res.size(), 1);

        BOOST_CHECK_EQUAL(res[0].GetKey(), key);
        BOOST_CHECK(res[0].GetValue() == value);
    }

    /**
     * Check single result through iter version of GetAll().
     *
     * @param cur Cursor.
     * @param key Key.
     * @param value Value.
     */
    template<typename K, typename V>
    void CheckSingleGetAllIter(QueryCursor<K, V>& cur, const K& key, const V& value)
    {
        std::vector< cache::CacheEntry<K, V> > res;

        cur.GetAll(std::back_inserter(res));

        CheckCursorEmpty(cur);

        BOOST_CHECK_EQUAL(res.size(), 1);

        BOOST_CHECK_EQUAL(res[0].GetKey(), key);
        BOOST_CHECK(res[0].GetValue() == value);
    }

    /**
     * Check multiple results through iteration.
     *
     * @param cur Cursor.
     * @param key1 Key 1.
     * @param value1 Value 1.
     * @param key2 Key 2.
     * @param value2 Value 2.
     */
    template<typename K, typename V>
    void CheckMultiple(QueryCursor<K, V>& cur, const K& key1, const V& value1, const K& key2, const V& value2)
    {
        for (int i = 0; i < 2; i++)
        {
            BOOST_REQUIRE(cur.HasNext());

            cache::CacheEntry<K, V> entry = cur.GetNext();

            if (entry.GetKey() == key1)
                BOOST_CHECK(entry.GetValue() == value1);
            else if (entry.GetKey() == key2)
                BOOST_CHECK(entry.GetValue() == value2);
            else
                BOOST_FAIL("Unexpected entry.");
        }

        CheckCursorEmpty(cur);
    }

    /**
     * Check multiple results through GetAll().
     *
     * @param cur Cursor.
     * @param key1 Key 1.
     * @param value1 Value 1.
     * @param key2 Key 2.
     * @param value2 Value 2.
     */
    template<typename K, typename V>
    void CheckMultipleGetAll(QueryCursor<K, V>& cur, const K& key1, const V& value1, const K& key2, const V& value2)
    {
        std::vector< cache::CacheEntry<K, V> > res;

        cur.GetAll(res);

        CheckCursorEmpty(cur);

        BOOST_REQUIRE_EQUAL(res.size(), 2);

        for (int i = 0; i < 2; i++)
        {
            cache::CacheEntry<K, V> entry = res[i];

            if (entry.GetKey() == key1)
                BOOST_CHECK(entry.GetValue() == value1);
            else if (entry.GetKey() == key2)
                BOOST_CHECK(entry.GetValue() == value2);
            else
                BOOST_FAIL("Unexpected entry.");
        }
    }

    /**
     * Check multiple results through GetAll().
     *
     * @param cur Cursor.
     * @param key1 Key 1.
     * @param value1 Value 1.
     * @param key2 Key 2.
     * @param value2 Value 2.
     */
    template<typename K, typename V>
    void CheckMultipleGetAllIter(QueryCursor<K, V>& cur, const K& key1, const V& value1, const K& key2, const V& value2)
    {
        std::vector< cache::CacheEntry<K, V> > res;

        cur.GetAll(std::back_inserter(res));

        CheckCursorEmpty(cur);

        BOOST_REQUIRE_EQUAL(res.size(), 2);

        for (int i = 0; i < 2; i++)
        {
            cache::CacheEntry<K, V> entry = res[i];

            if (entry.GetKey() == key1)
                BOOST_CHECK(entry.GetValue() == value1);
            else if (entry.GetKey() == key2)
                BOOST_CHECK(entry.GetValue() == value2);
            else
                BOOST_FAIL("Unexpected entry.");
        }
    }

    /**
     * Make custom test value.
     *
     * @param seed Seed to generate value.
     */
    IGNORE_SIGNED_OVERFLOW
    ignite::TestType MakeCustomTestValue(int64_t seed)
    {
        ignite::TestType val;

        val.i8Field = static_cast<int8_t>(seed);
        val.i16Field = static_cast<int16_t>(2 * seed);
        val.i32Field = static_cast<int32_t>(4 * seed);
        val.i64Field = 8 * seed;
        val.strField = "Lorem ipsum";
        val.floatField = 16.0f * seed;
        val.doubleField = 32.0 * seed;
        val.boolField = ((seed % 2) == 0);
        val.guidField = ignite::Guid(0x1020304050607080 * seed, 0x9000A0B0C0D0E0F0 * seed);
        val.dateField = ignite::Date(235682736 * seed);
        val.timeField = ignite::Time((124523 * seed) % (24 * 60 * 60 * 1000));
        val.timestampField = ignite::Timestamp(128341594123 * seed);

        val.i8ArrayField.push_back(static_cast<int8_t>(9 * seed));
        val.i8ArrayField.push_back(static_cast<int8_t>(6 * seed));
        val.i8ArrayField.push_back(static_cast<int8_t>(3 * seed));
        val.i8ArrayField.push_back(static_cast<int8_t>(42 * seed));

        return val;
    }

} // anonymous namespace

BOOST_FIXTURE_TEST_SUITE(ScanQueryTestSuite, ScanQueryTestSuiteFixture)

/**
 * Test scan query.
 */
BOOST_AUTO_TEST_CASE(TestScanQuery)
{
    // Test query with no results.
    ScanQuery qry;

    QueryCursor<int64_t, ignite::TestType> cursor = cacheAllFields.Query(qry);
    CheckCursorEmpty(cursor);

    cursor = cacheAllFields.Query(qry);
    CheckEmptyGetAll(cursor);

    cursor = cacheAllFields.Query(qry);
    CheckEmptyGetAllIter(cursor);

    int64_t key1 = 1;
    ignite::TestType val1 = MakeCustomTestValue(1);

    // Test simple query.
    cacheAllFields.Put(key1, val1);

    cursor = cacheAllFields.Query(qry);
    CheckSingle(cursor, key1, val1);

    cursor = cacheAllFields.Query(qry);
    CheckSingleGetAll(cursor, key1, val1);

    cursor = cacheAllFields.Query(qry);
    CheckSingleGetAllIter(cursor, key1, val1);

    int64_t key2 = 2;
    ignite::TestType val2 = MakeCustomTestValue(2);

    // Test query returning multiple entries.
    cacheAllFields.Put(key2, val2);

    cursor = cacheAllFields.Query(qry);
    CheckMultiple(cursor, key1, val1, key2, val2);

    cursor = cacheAllFields.Query(qry);
    CheckMultipleGetAll(cursor, key1, val1, key2, val2);

    cursor = cacheAllFields.Query(qry);
    CheckMultipleGetAllIter(cursor, key1, val1, key2, val2);
}

/**
 * Test scan query over partitions.
 */
BOOST_AUTO_TEST_CASE(TestScanQueryPartitioned)
{
    // Populate cache with data.
    int32_t partCnt = 256;   // Defined in configuration explicitly.
    int64_t entryCnt = 1000; // Should be greater than partCnt.

    for (int64_t i = 0; i < entryCnt; i++)
        cacheAllFields.Put(i, MakeCustomTestValue(i));

    // Iterate over all partitions and collect data.
    std::set<int64_t> keys;

    for (int32_t i = 0; i < partCnt; i++)
    {
        ScanQuery qry;
        qry.SetPartition(i);

        QueryCursor<int64_t, ignite::TestType> cur = cacheAllFields.Query(qry);

        while (cur.HasNext())
        {
            cache::CacheEntry<int64_t, ignite::TestType> entry = cur.GetNext();

            int64_t key = entry.GetKey();
            keys.insert(key);

            BOOST_REQUIRE(entry.GetValue() == MakeCustomTestValue(key));
        }
    }

    // Ensure that all keys were read.
    BOOST_CHECK_EQUAL(keys.size(), entryCnt);
}

BOOST_AUTO_TEST_SUITE_END()
