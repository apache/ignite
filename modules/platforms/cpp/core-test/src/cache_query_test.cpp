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

#ifndef _MSC_VER
    #define BOOST_TEST_DYN_LINK
#endif

#include <sstream>

#include <boost/test/unit_test.hpp>

#include "ignite/impl/utils.h"
#include "ignite/cache/cache.h"
#include "ignite/cache/query/query_cursor.h"
#include "ignite/cache/query/query_sql.h"
#include "ignite/cache/query/query_text.h"
#include "ignite/cache/query/query_sql_fields.h"
#include "ignite/ignite.h"
#include "ignite/ignition.h"

using namespace boost::unit_test;

using namespace ignite;
using namespace ignite::cache;
using namespace ignite::cache::query;
using namespace ignite::impl::utils;

/**
 * Person class for query tests.
 */
class IGNITE_IMPORT_EXPORT QueryPerson
{
public:
    /**
     * Constructor.
     */
    QueryPerson() : name(NULL), age(0)
    {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param name Name.
     * @param age Age.
     */
    QueryPerson(const std::string& name, int age) : name(CopyChars(name.c_str())), age(age)
    {
        // No-op.
    }

    /**
     * Copy constructor.
     *
     * @param other Other instance.
     */
    QueryPerson(const QueryPerson& other)
    {
        name = CopyChars(other.name);
        age = other.age;
    }

    /**
     * Assignment operator.
     *
     * @param other Other instance.
     * @return This instance.
     */
    QueryPerson& operator=(const QueryPerson& other)
    {
        if (&other != this)
        {
            QueryPerson tmp(other);

            char* name0 = name;
            int age0 = age;

            name = tmp.name;
            age = tmp.age;

            tmp.name = name0;
            tmp.age = age0;
        }

        return *this;
    }

    /**
     * Destructor.
     */
    ~QueryPerson()
    {
        ReleaseChars(name);
    }

    /**
     * Get name.
     * 
     * @return Name.
     */
    std::string GetName()
    {
        return name ? std::string(name) : std::string();
    }

    /**
     * Get age.
     * 
     * @return Age.
     */
    int32_t GetAge()
    {
        return age;
    }

private:
    /** Name. */
    char* name;

    /** Age. */
    int age;
};

namespace ignite
{
    namespace binary
    {
        /**
         * Binary type definition.
         */
        IGNITE_BINARY_TYPE_START(QueryPerson)
            IGNITE_BINARY_GET_TYPE_ID_AS_HASH(QueryPerson)
            IGNITE_BINARY_GET_TYPE_NAME_AS_IS(QueryPerson)
            IGNITE_BINARY_GET_FIELD_ID_AS_HASH
            IGNITE_BINARY_GET_HASH_CODE_ZERO(QueryPerson)
            IGNITE_BINARY_IS_NULL_FALSE(QueryPerson)
            IGNITE_BINARY_GET_NULL_DEFAULT_CTOR(QueryPerson)

            void Write(BinaryWriter& writer, QueryPerson obj)
            {
                writer.WriteString("name", obj.GetName());
                writer.WriteInt32("age", obj.GetAge());
            }

            QueryPerson Read(BinaryReader& reader)
            {
                std::string name = reader.ReadString("name");
                int age = reader.ReadInt32("age");
            
                return QueryPerson(name, age);
            }

        IGNITE_BINARY_TYPE_END
    }
}

/** Node started during the test. */
Ignite grid = Ignite();

/** Cache accessor. */
Cache<int, QueryPerson> GetCache()
{
    return grid.GetCache<int, QueryPerson>("cache");
}

/**
 * Test setup fixture.
 */
struct CacheQueryTestSuiteFixture {
    /**
     * Constructor.
     */
    CacheQueryTestSuiteFixture()
    {
        IgniteConfiguration cfg;
        
        cfg.jvmOpts.push_back("-Xdebug");
        cfg.jvmOpts.push_back("-Xnoagent");
        cfg.jvmOpts.push_back("-Djava.compiler=NONE");
        cfg.jvmOpts.push_back("-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005");
        cfg.jvmOpts.push_back("-XX:+HeapDumpOnOutOfMemoryError");

#ifdef IGNITE_TESTS_32
        cfg.jvmInitMem = 256;
        cfg.jvmMaxMem = 768;
#else
        cfg.jvmInitMem = 1024;
        cfg.jvmMaxMem = 4096;
#endif

        char* cfgPath = getenv("IGNITE_NATIVE_TEST_CPP_CONFIG_PATH");

        cfg.springCfgPath = std::string(cfgPath).append("/").append("cache-query.xml");

        IgniteError err;

        Ignite grid0 = Ignition::Start(cfg, &err);

        if (err.GetCode() != IgniteError::IGNITE_SUCCESS)
            BOOST_ERROR(err.GetText());

        grid = grid0;
    }

    /**
     * Destructor.
     */
    ~CacheQueryTestSuiteFixture()
    {
        Ignition::Stop(grid.GetName(), true);
    }
};

/**
 * Ensure that HasNext() fails.
 *
 * @param cur Cursor.
 */
template<typename Cursor>
void CheckHasNextFail(Cursor& cur)
{
    try
    {
        cur.HasNext();

        BOOST_FAIL("Must fail.");
    }
    catch (IgniteError& err)
    {
        BOOST_REQUIRE(err.GetCode() == IgniteError::IGNITE_ERR_GENERIC);
    }
}

/**
 * Ensure that GetNext() fails.
 *
 * @param cur Cursor.
 */
template<typename Cursor>
void CheckGetNextFail(Cursor& cur)
{
    try
    {
        cur.GetNext();

        BOOST_FAIL("Must fail.");
    }
    catch (IgniteError& err)
    {
        BOOST_REQUIRE(err.GetCode() == IgniteError::IGNITE_ERR_GENERIC);
    }
}

/**
 * Ensure that GetAll() fails.
 *
 * @param cur Cursor.
 */
template<typename Cursor>
void CheckGetAllFail(Cursor& cur)
{
    try 
    {
        std::vector<CacheEntry<int, QueryPerson>> res;

        cur.GetAll(res);

        BOOST_FAIL("Must fail.");
    }
    catch (IgniteError& err)
    {
        BOOST_REQUIRE(err.GetCode() == IgniteError::IGNITE_ERR_GENERIC);
    }
}

/**
 * Check empty result through iteration.
 *
 * @param cur Cursor.
 */
void CheckEmpty(QueryCursor<int, QueryPerson>& cur)
{
    BOOST_REQUIRE(!cur.HasNext());

    CheckGetNextFail(cur);
    CheckGetAllFail(cur);
}

/**
* Check empty result through iteration.
*
* @param cur Cursor.
*/
void CheckEmpty(QueryFieldsCursor& cur)
{
    BOOST_REQUIRE(!cur.HasNext());

    CheckGetNextFail(cur);
}

/**
 * Check empty result through GetAll().
 *
 * @param cur Cursor.
 */
template<typename Cursor>
void CheckEmptyGetAll(Cursor& cur)
{
    std::vector<CacheEntry<int, QueryPerson>> res;

    cur.GetAll(res);

    BOOST_REQUIRE(res.size() == 0);

    CheckHasNextFail(cur);
    CheckGetNextFail(cur);
}

/**
 * Check single result through iteration.
 *
 * @param cur Cursor.
 * @param key1 Key.
 * @param name1 Name.
 * @param age1 Age.
 */
template<typename Cursor>
void CheckSingle(Cursor& cur, int key, const std::string& name, int age)
{
    BOOST_REQUIRE(cur.HasNext());

    CheckGetAllFail(cur);

    CacheEntry<int, QueryPerson> entry = cur.GetNext();

    CheckGetAllFail(cur);

    BOOST_REQUIRE(entry.GetKey() == key);
    BOOST_REQUIRE(entry.GetValue().GetName().compare(name) == 0);
    BOOST_REQUIRE(entry.GetValue().GetAge() == age);

    BOOST_REQUIRE(!cur.HasNext());

    CheckGetNextFail(cur);
    CheckGetAllFail(cur);
}

/**
 * Check single result through GetAll().
 *
 * @param cur Cursor.
 * @param key1 Key.
 * @param name1 Name.
 * @param age1 Age.
 */
template<typename Cursor>
void CheckSingleGetAll(Cursor& cur, int key, const std::string& name, int age)
{
    std::vector<CacheEntry<int, QueryPerson>> res;

    cur.GetAll(res);

    CheckHasNextFail(cur);
    CheckGetNextFail(cur);
    CheckGetAllFail(cur);

    BOOST_REQUIRE(res.size() == 1);

    BOOST_REQUIRE(res[0].GetKey() == 1);    
    BOOST_REQUIRE(res[0].GetValue().GetName().compare(name) == 0);
    BOOST_REQUIRE(res[0].GetValue().GetAge() == age);

    CheckHasNextFail(cur);
    CheckGetNextFail(cur);
    CheckGetAllFail(cur);
}

/**
 * Check multiple results through iteration.
 *
 * @param cur Cursor.
 * @param key1 Key 1.
 * @param name1 Name 1.
 * @param age1 Age 1.
 * @param key2 Key 2.
 * @param name2 Name 2.
 * @param age2 Age 2.
 */
template<typename Cursor>
void CheckMultiple(Cursor& cur, int key1, const std::string& name1, 
    int age1, int key2, const std::string& name2, int age2)
{
    for (int i = 0; i < 2; i++)
    {
        BOOST_REQUIRE(cur.HasNext());

        CheckGetAllFail(cur);

        CacheEntry<int, QueryPerson> entry = cur.GetNext();

        CheckGetAllFail(cur);

        if (entry.GetKey() == key1)
        {
            BOOST_REQUIRE(entry.GetValue().GetName().compare(name1) == 0);
            BOOST_REQUIRE(entry.GetValue().GetAge() == age1);            
        }
        else if (entry.GetKey() == key2)
        {
            BOOST_REQUIRE(entry.GetValue().GetName().compare(name2) == 0);
            BOOST_REQUIRE(entry.GetValue().GetAge() == age2);            
        }
        else
            BOOST_FAIL("Unexpected entry.");
    }
    
    BOOST_REQUIRE(!cur.HasNext());

    CheckGetNextFail(cur);
    CheckGetAllFail(cur);
}

/**
 * Check multiple results through GetAll().
 *
 * @param cur Cursor.
 * @param key1 Key 1.
 * @param name1 Name 1.
 * @param age1 Age 1.
 * @param key2 Key 2.
 * @param name2 Name 2.
 * @param age2 Age 2.
 */
template<typename Cursor>
void CheckMultipleGetAll(Cursor& cur, int key1, const std::string& name1,
    int age1, int key2, const std::string& name2, int age2)
{
    std::vector<CacheEntry<int, QueryPerson>> res;

    cur.GetAll(res);

    CheckHasNextFail(cur);
    CheckGetNextFail(cur);
    CheckGetAllFail(cur);

    BOOST_REQUIRE(res.size() == 2);

    for (int i = 0; i < 2; i++)
    {
        CacheEntry<int, QueryPerson> entry = res[i];

        if (entry.GetKey() == key1)
        {
            BOOST_REQUIRE(entry.GetValue().GetName().compare(name1) == 0);
            BOOST_REQUIRE(entry.GetValue().GetAge() == age1);            
        }
        else if (entry.GetKey() == key2)
        {
            BOOST_REQUIRE(entry.GetValue().GetName().compare(name2) == 0);
            BOOST_REQUIRE(entry.GetValue().GetAge() == age2);
        }
        else
            BOOST_FAIL("Unexpected entry.");
    }
}

BOOST_FIXTURE_TEST_SUITE(CacheQueryTestSuite, CacheQueryTestSuiteFixture)

/**
 * Test SQL query.
 */
BOOST_AUTO_TEST_CASE(TestSqlQuery)
{    
    Cache<int, QueryPerson> cache = GetCache();

    // Test query with no results.
    SqlQuery qry("QueryPerson", "age < 20");

    QueryCursor<int, QueryPerson> cursor = cache.Query(qry);
    CheckEmpty(cursor);

    cursor = cache.Query(qry);
    CheckEmptyGetAll(cursor);

    // Test simple query.
    cache.Put(1, QueryPerson("A1", 10));
    cache.Put(2, QueryPerson("A2", 20));
    
    cursor = cache.Query(qry);
    CheckSingle(cursor, 1, "A1", 10);
    
    cursor = cache.Query(qry);
    CheckSingleGetAll(cursor, 1, "A1", 10);

    // Test simple local query.
    qry.SetLocal(true);

    cursor = cache.Query(qry);
    CheckSingle(cursor, 1, "A1", 10);

    cursor = cache.Query(qry);
    CheckSingleGetAll(cursor, 1, "A1", 10);

    // Test query with arguments.
    qry.SetSql("age < ? AND name = ?");
    qry.AddArgument<int>(20);
    qry.AddArgument<std::string>("A1");

    cursor = cache.Query(qry);
    CheckSingle(cursor, 1, "A1", 10);

    cursor = cache.Query(qry);
    CheckSingleGetAll(cursor, 1, "A1", 10);

    // Test query returning multiple entries.
    qry = SqlQuery("QueryPerson", "age < 30");

    cursor = cache.Query(qry);
    CheckMultiple(cursor, 1, "A1", 10, 2, "A2", 20);

    cursor = cache.Query(qry);
    CheckMultipleGetAll(cursor, 1, "A1", 10, 2, "A2", 20);
}

/**
 * Test text query.
 */
BOOST_AUTO_TEST_CASE(TestTextQuery)
{
    Cache<int, QueryPerson> cache = GetCache();

    // Test query with no results.
    TextQuery qry("QueryPerson", "A1");

    QueryCursor<int, QueryPerson> cursor = cache.Query(qry);
    CheckEmpty(cursor);

    cursor = cache.Query(qry);
    CheckEmptyGetAll(cursor);

    // Test simple query.
    cache.Put(1, QueryPerson("A1", 10));
    cache.Put(2, QueryPerson("A2", 20));

    cursor = cache.Query(qry);
    CheckSingle(cursor, 1, "A1", 10);

    cursor = cache.Query(qry);
    CheckSingleGetAll(cursor, 1, "A1", 10);

    // Test simple local query.
    qry.SetLocal(true);

    cursor = cache.Query(qry);
    CheckSingle(cursor, 1, "A1", 10);

    cursor = cache.Query(qry);
    CheckSingleGetAll(cursor, 1, "A1", 10);

    // Test query returning multiple entries.
    qry = TextQuery("QueryPerson", "A*");

    cursor = cache.Query(qry);
    CheckMultiple(cursor, 1, "A1", 10, 2, "A2", 20);

    cursor = cache.Query(qry);
    CheckMultipleGetAll(cursor, 1, "A1", 10, 2, "A2", 20);
}

/**
 * Test scan query.
 */
BOOST_AUTO_TEST_CASE(TestScanQuery)
{
    // Test simple query.
    Cache<int, QueryPerson> cache = GetCache();

    // Test query with no results.
    ScanQuery qry;

    QueryCursor<int, QueryPerson> cursor = cache.Query(qry);
    CheckEmpty(cursor);

    cursor = cache.Query(qry);
    CheckEmptyGetAll(cursor);

    // Test simple query.
    cache.Put(1, QueryPerson("A1", 10));

    cursor = cache.Query(qry);
    CheckSingle(cursor, 1, "A1", 10);

    cursor = cache.Query(qry);
    CheckSingleGetAll(cursor, 1, "A1", 10);

    // Test query returning multiple entries.
    cache.Put(2, QueryPerson("A2", 20));

    cursor = cache.Query(qry);
    CheckMultiple(cursor, 1, "A1", 10, 2, "A2", 20);

    cursor = cache.Query(qry);
    CheckMultipleGetAll(cursor, 1, "A1", 10, 2, "A2", 20);
}

/**
 * Test scan query over partitions.
 */
BOOST_AUTO_TEST_CASE(TestScanQueryPartitioned)
{
    // Populate cache with data.
    Cache<int, QueryPerson> cache = GetCache();

    int32_t partCnt = 256;   // Defined in configuration explicitly.   
    int32_t entryCnt = 1000; // Should be greater than partCnt.
    
    for (int i = 0; i < entryCnt; i++) 
    {
        std::stringstream stream; 
        
        stream << "A" << i;
            
        cache.Put(i, QueryPerson(stream.str(), i * 10));
    }

    // Iterate over all partitions and collect data.
    std::set<int> keys;

    for (int i = 0; i < partCnt; i++)
    {
        ScanQuery qry(i);

        QueryCursor<int, QueryPerson> cur = cache.Query(qry);

        while (cur.HasNext())
        {
            CacheEntry<int, QueryPerson> entry = cur.GetNext();

            int key = entry.GetKey();

            keys.insert(key);

            std::stringstream stream;
            stream << "A" << key;
            BOOST_REQUIRE(entry.GetValue().GetName().compare(stream.str()) == 0);

            BOOST_REQUIRE(entry.GetValue().GetAge() == key * 10);
        }
    }

    // Ensure that all keys were read.
    BOOST_REQUIRE(keys.size() == entryCnt);
}

/**
 * Test fields query with single entry.
 */
BOOST_AUTO_TEST_CASE(TestFieldsQuerySingle)
{
    // Test simple query.
    Cache<int, QueryPerson> cache = GetCache();

    // Test query with two fields of different type.
    SqlFieldsQuery qry("select age, name from QueryPerson");

    QueryFieldsCursor cursor = cache.Query(qry);
    CheckEmpty(cursor);
    
    // Test simple query.
    cache.Put(1, QueryPerson("A1", 10));

    cursor = cache.Query(qry);

    IgniteError error;

    BOOST_REQUIRE(cursor.HasNext(error));
    BOOST_REQUIRE(error.GetCode() == IgniteError::IGNITE_SUCCESS);

    QueryFieldsRow row = cursor.GetNext(error);
    BOOST_REQUIRE(error.GetCode() == IgniteError::IGNITE_SUCCESS);

    BOOST_REQUIRE(row.HasNext(error));
    BOOST_REQUIRE(error.GetCode() == IgniteError::IGNITE_SUCCESS);

    int age = row.GetNext<int>(error);
    BOOST_REQUIRE(error.GetCode() == IgniteError::IGNITE_SUCCESS);

    BOOST_REQUIRE(age == 10);

    std::string name = row.GetNext<std::string>(error);
    BOOST_REQUIRE(error.GetCode() == IgniteError::IGNITE_SUCCESS);

    BOOST_REQUIRE(name == "A1");

    BOOST_REQUIRE(!row.HasNext());

    CheckEmpty(cursor);
}

/**
 * Test fields query with two simultaneously handled rows.
 */
BOOST_AUTO_TEST_CASE(TestFieldsQueryTwo)
{
    // Test simple query.
    Cache<int, QueryPerson> cache = GetCache();

    // Test query with two fields of different type.
    SqlFieldsQuery qry("select age, name from QueryPerson");

    QueryFieldsCursor cursor = cache.Query(qry);
    CheckEmpty(cursor);

    // Test simple query.
    cache.Put(1, QueryPerson("A1", 10));
    cache.Put(2, QueryPerson("A2", 20));

    cursor = cache.Query(qry);

    IgniteError error;

    BOOST_REQUIRE(cursor.HasNext(error));
    BOOST_REQUIRE(error.GetCode() == IgniteError::IGNITE_SUCCESS);

    QueryFieldsRow row1 = cursor.GetNext(error);
    BOOST_REQUIRE(error.GetCode() == IgniteError::IGNITE_SUCCESS);

    QueryFieldsRow row2 = cursor.GetNext(error);
    BOOST_REQUIRE(error.GetCode() == IgniteError::IGNITE_SUCCESS);

    BOOST_REQUIRE(row1.HasNext(error));
    BOOST_REQUIRE(error.GetCode() == IgniteError::IGNITE_SUCCESS);

    BOOST_REQUIRE(row2.HasNext(error));
    BOOST_REQUIRE(error.GetCode() == IgniteError::IGNITE_SUCCESS);

    int age2 = row2.GetNext<int>(error);
    BOOST_REQUIRE(error.GetCode() == IgniteError::IGNITE_SUCCESS);

    BOOST_REQUIRE(age2 == 20);

    int age1 = row1.GetNext<int>(error);
    BOOST_REQUIRE(error.GetCode() == IgniteError::IGNITE_SUCCESS);

    BOOST_REQUIRE(age1 == 10);

    std::string name1 = row1.GetNext<std::string>(error);
    BOOST_REQUIRE(error.GetCode() == IgniteError::IGNITE_SUCCESS);

    BOOST_REQUIRE(name1 == "A1");

    std::string name2 = row2.GetNext<std::string>(error);
    BOOST_REQUIRE(error.GetCode() == IgniteError::IGNITE_SUCCESS);

    BOOST_REQUIRE(name2 == "A2");

    BOOST_REQUIRE(!row1.HasNext());
    BOOST_REQUIRE(!row2.HasNext());

    CheckEmpty(cursor);
}

/**
 * Test fields query with several entries.
 */
BOOST_AUTO_TEST_CASE(TestFieldsQuerySeveral)
{
    // Test simple query.
    Cache<int, QueryPerson> cache = GetCache();

    // Test query with two fields of different type.
    SqlFieldsQuery qry("select name, age from QueryPerson");

    QueryFieldsCursor cursor = cache.Query(qry);
    CheckEmpty(cursor);

    int32_t entryCnt = 1000; // Number of entries.

    for (int i = 0; i < entryCnt; i++)
    {
        std::stringstream stream;

        stream << "A" << i;

        cache.Put(i, QueryPerson(stream.str(), i * 10));
    }

    cursor = cache.Query(qry);

    IgniteError error;

    for (int i = 0; i < entryCnt; i++)
    {
        std::stringstream stream;

        stream << "A" << i;

        std::string expected_name = stream.str();
        int expected_age = i * 10;

        BOOST_REQUIRE(cursor.HasNext(error));
        BOOST_REQUIRE(error.GetCode() == IgniteError::IGNITE_SUCCESS);

        QueryFieldsRow row = cursor.GetNext(error);
        BOOST_REQUIRE(error.GetCode() == IgniteError::IGNITE_SUCCESS);

        BOOST_REQUIRE(row.HasNext(error));
        BOOST_REQUIRE(error.GetCode() == IgniteError::IGNITE_SUCCESS);

        std::string name = row.GetNext<std::string>(error);
        BOOST_REQUIRE(error.GetCode() == IgniteError::IGNITE_SUCCESS);

        BOOST_REQUIRE(name == expected_name);

        int age = row.GetNext<int>(error);
        BOOST_REQUIRE(error.GetCode() == IgniteError::IGNITE_SUCCESS);

        BOOST_REQUIRE(age == expected_age);

        BOOST_REQUIRE(!row.HasNext());
    }

    CheckEmpty(cursor);
}

BOOST_AUTO_TEST_SUITE_END()