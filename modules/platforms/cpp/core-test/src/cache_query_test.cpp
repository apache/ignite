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

#include <ignite/common/utils.h>

#include "ignite/cache/cache.h"
#include "ignite/cache/query/query_cursor.h"
#include "ignite/cache/query/query_sql.h"
#include "ignite/cache/query/query_text.h"
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
 * Person class for query tests.
 */
class IGNITE_IMPORT_EXPORT QueryPerson
{
public:
    /**
     * Constructor.
     */
    QueryPerson() : 
        name(NULL),
        age(0),
        birthday(),
        recordCreated()
    {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param name Name.
     * @param age Age.
     */
    QueryPerson(const std::string& name, int age,
        const Date& birthday, const Timestamp& recordCreated) : 
        name(CopyChars(name.c_str())),
        age(age),
        birthday(birthday),
        recordCreated(recordCreated)
    {
        // No-op.
    }

    /**
     * Copy constructor.
     *
     * @param other Other instance.
     */
    QueryPerson(const QueryPerson& other) :
        name(CopyChars(other.name)),
        age(other.age),
        birthday(other.birthday),
        recordCreated(other.recordCreated)
    {
        // No-op.
    }

    /**
     * Assignment operator.
     *
     * @param other Other instance.
     * @return This instance.
     */
    QueryPerson& operator=(const QueryPerson& other)
    {
        using std::swap;

        if (&other != this)
        {
            QueryPerson tmp(other);

            swap(name, tmp.name);
            swap(age, tmp.age);
            swap(birthday, tmp.birthday);
            swap(recordCreated, tmp.recordCreated);
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
    std::string GetName() const
    {
        return name ? std::string(name) : std::string();
    }

    /**
     * Get age.
     * 
     * @return Age.
     */
    int32_t GetAge() const
    {
        return age;
    }

    /**
     * Get birthday.
     * 
     * @return Birthday date.
     */
    const Date& GetBirthday() const
    {
        return birthday;
    }

    /**
     * Get creation time.
     * 
     * @return Creation time.
     */
    const Timestamp& GetCreationTime() const
    {
        return recordCreated;
    }

private:
    /** Name. */
    char* name;

    /** Age. */
    int age;

    /** Birthday. */
    Date birthday;

    /** Record creation timestamp. */
    Timestamp recordCreated;
};

/**
 * Relation class for query tests.
 */
class IGNITE_IMPORT_EXPORT QueryRelation
{
public:
    /**
     * Constructor.
     */
    QueryRelation() :
        personId(),
        someVal()
    {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param personId Id.
     * @param someVal Int value.
     */
    QueryRelation(int32_t personId, int32_t someVal) :
        personId(personId),
        someVal(someVal)
    {
        // No-op.
    }

    /**
     * Get person ID.
     * 
     * @return Person ID.
     */
    int32_t GetPersonId() const
    {
        return personId;
    }

    /**
     * Get hobby ID.
     * 
     * @return Some test value.
     */
    int32_t GetHobbyId() const
    {
        return someVal;
    }

private:
    /** Person ID. */
    int32_t personId;

    /** Some test value. */
    int32_t someVal;
};

namespace ignite
{
    namespace binary
    {
        /**
         * Binary type definition for QueryPerson.
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
                writer.WriteDate("birthday", obj.GetBirthday());
                writer.WriteTimestamp("recordCreated", obj.GetCreationTime());
            }

            QueryPerson Read(BinaryReader& reader)
            {
                std::string name = reader.ReadString("name");
                int age = reader.ReadInt32("age");
                Date birthday = reader.ReadDate("birthday");
                Timestamp recordCreated = reader.ReadTimestamp("recordCreated");
            
                return QueryPerson(name, age, birthday, recordCreated);
            }
        IGNITE_BINARY_TYPE_END

        /**
         * Binary type definition for QueryRelation.
         */
        IGNITE_BINARY_TYPE_START(QueryRelation)
            IGNITE_BINARY_GET_TYPE_ID_AS_HASH(QueryRelation)
            IGNITE_BINARY_GET_TYPE_NAME_AS_IS(QueryRelation)
            IGNITE_BINARY_GET_FIELD_ID_AS_HASH
            IGNITE_BINARY_GET_HASH_CODE_ZERO(QueryRelation)
            IGNITE_BINARY_IS_NULL_FALSE(QueryRelation)
            IGNITE_BINARY_GET_NULL_DEFAULT_CTOR(QueryRelation)

            void Write(BinaryWriter& writer, QueryRelation obj)
            {
                writer.WriteInt32("personId", obj.GetPersonId());
                writer.WriteInt32("someVal", obj.GetHobbyId());
            }

            QueryRelation Read(BinaryReader& reader)
            {
                int32_t personId = reader.ReadInt32("personId");
                int32_t someVal = reader.ReadInt32("someVal");

                return QueryRelation(personId, someVal);
            }
        IGNITE_BINARY_TYPE_END
    }
}

/**
 * Count number of records returned by cursor.
 *
 * @param cur Cursor.
 */
template<typename Cursor>
int CountRecords(Cursor& cur)
{
    int number = 0;
    while (cur.HasNext())
    {
        ++number;
        cur.GetNext();
    }

    return number;
}

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
void CheckGetAllFail(QueryCursor<int, QueryPerson>& cur)
{
    try 
    {
        std::vector<CacheEntry<int, QueryPerson> > res;

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
void CheckEmptyGetAll(QueryCursor<int, QueryPerson>& cur)
{
    std::vector<CacheEntry<int, QueryPerson> > res;

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
void CheckSingle(QueryCursor<int, QueryPerson>& cur, int key, const std::string& name, int age)
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
 * Check single result through iteration.
 *
 * @param cur Cursor.
 * @param key Key.
 * @param name Name.
 * @param age Age.
 */
void CheckSingle(QueryFieldsCursor& cur, int key, const std::string& name, int age)
{
    BOOST_REQUIRE(cur.HasNext());

    QueryFieldsRow row = cur.GetNext();

    BOOST_REQUIRE_EQUAL(row.GetNext<int32_t>(), key);
    BOOST_REQUIRE_EQUAL(row.GetNext<std::string>(), name);
    BOOST_REQUIRE_EQUAL(row.GetNext<int32_t>(), age);

    BOOST_REQUIRE(!row.HasNext());
    BOOST_REQUIRE(!cur.HasNext());

    CheckGetNextFail(cur);
}

/**
 * Check single result through GetAll().
 *
 * @param cur Cursor.
 * @param key1 Key.
 * @param name1 Name.
 * @param age1 Age.
 */
void CheckSingleGetAll(QueryCursor<int, QueryPerson>& cur, int key, const std::string& name, int age)
{
    std::vector<CacheEntry<int, QueryPerson> > res;

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
void CheckMultiple(QueryCursor<int, QueryPerson>& cur, int key1, const std::string& name1,
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
void CheckMultipleGetAll(QueryCursor<int, QueryPerson>& cur, int key1, const std::string& name1,
    int age1, int key2, const std::string& name2, int age2)
{
    std::vector<CacheEntry<int, QueryPerson> > res;

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

/**
 * Test setup fixture.
 */
struct CacheQueryTestSuiteFixture
{
    Ignite StartNode(const char* name)
    {
        return ignite_test::StartNode("cache-query.xml", name);
    }

    void CheckFieldsQueryPages(int32_t pageSize, int32_t pagesNum, int32_t additionalNum)
    {
        // Test simple query.
        Cache<int, QueryPerson> cache = GetPersonCache();

        // Test query with two fields of different type.
        SqlFieldsQuery qry("select name, age from QueryPerson");

        QueryFieldsCursor cursor = cache.Query(qry);
        CheckEmpty(cursor);

        const int32_t entryCnt = pageSize * pagesNum + additionalNum; // Number of entries.

        qry.SetPageSize(pageSize);

        for (int i = 0; i < entryCnt; i++)
        {
            std::stringstream stream;

            stream << "A" << i;

            cache.Put(i, QueryPerson(stream.str(), i * 10, MakeDateGmt(1970 + i),
                MakeTimestampGmt(2016, 1, 1, i / 60, i % 60)));
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

    /**
     * Constructor.
     */
    CacheQueryTestSuiteFixture() : 
        grid(StartNode("Node1"))
    {
        // No-op.
    }

    /**
     * Destructor.
     */
    ~CacheQueryTestSuiteFixture()
    {
        Ignition::StopAll(true);
    }

    /** Person cache accessor. */
    Cache<int, QueryPerson> GetPersonCache()
    {
        return grid.GetCache<int, QueryPerson>("QueryPerson");
    }

    /** Relation cache accessor. */
    Cache<int, QueryRelation> GetRelationCache()
    {
        return grid.GetCache<int, QueryRelation>("QueryRelation");
    }

    /** Node started during the test. */
    Ignite grid;
};

BOOST_FIXTURE_TEST_SUITE(CacheQueryTestSuite, CacheQueryTestSuiteFixture)

/**
 * Test SQL query.
 */
BOOST_AUTO_TEST_CASE(TestSqlQuery)
{    
    Cache<int, QueryPerson> cache = GetPersonCache();

    // Test query with no results.
    SqlQuery qry("QueryPerson", "age < 20");

    QueryCursor<int, QueryPerson> cursor = cache.Query(qry);
    CheckEmpty(cursor);

    cursor = cache.Query(qry);
    CheckEmptyGetAll(cursor);

    // Test simple query.
    cache.Put(1, QueryPerson("A1", 10, MakeDateGmt(1990, 03, 18),
        MakeTimestampGmt(2016, 02, 10, 17, 39, 34, 579304685)));

    cache.Put(2, QueryPerson("A2", 20, MakeDateGmt(1989, 10, 26),
        MakeTimestampGmt(2016, 02, 10, 17, 39, 35, 678403201)));
    
    cursor = cache.Query(qry);
    CheckSingle(cursor, 1, "A1", 10);
    
    cursor = cache.Query(qry);
    CheckSingleGetAll(cursor, 1, "A1", 10);

    // Test simple distributed joins query.
    BOOST_CHECK(!qry.IsDistributedJoins());
    qry.SetDistributedJoins(true);
    BOOST_CHECK(qry.IsDistributedJoins());

    cursor = cache.Query(qry);
    CheckSingle(cursor, 1, "A1", 10);

    cursor = cache.Query(qry);
    CheckSingleGetAll(cursor, 1, "A1", 10);

    qry.SetDistributedJoins(false);

    // Test simple local query.
    BOOST_CHECK(!qry.IsLocal());
    qry.SetLocal(true);
    BOOST_CHECK(qry.IsLocal());

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
 * Test SQL query distributed joins.
 */
BOOST_AUTO_TEST_CASE(TestSqlQueryDistributedJoins)
{    
    Cache<int, QueryPerson> cache1 = GetPersonCache();
    Cache<int, QueryRelation> cache2 = GetRelationCache();

    // Starting second node.
    Ignite node2 = StartNode("Node2");

    int entryCnt = 1000;

    // Filling caches
    for (int i = 0; i < entryCnt; i++)
    {
        std::stringstream stream;

        stream << "A" << i;

        cache1.Put(i, QueryPerson(stream.str(), i * 10, MakeDateGmt(1970 + i),
            MakeTimestampGmt(2016, 1, 1, i / 60, i % 60)));

        cache2.Put(i + 1, QueryRelation(i, i * 10));
    }

    // Test query with no results.
    SqlQuery qry("QueryPerson",
        "from \"QueryPerson\".QueryPerson, \"QueryRelation\".QueryRelation "
        "where \"QueryPerson\".QueryPerson.age = \"QueryRelation\".QueryRelation.someVal");

    QueryCursor<int, QueryPerson> cursor = cache1.Query(qry);

    // Ensure that data is not collocated, so not full result set is returned.
    int recordsNum = CountRecords(cursor);

    BOOST_CHECK_GT(recordsNum, 0);
    BOOST_CHECK_LT(recordsNum, entryCnt);

    qry.SetDistributedJoins(true);

    cursor = cache1.Query(qry);

    // Check that full result set is returned.
    recordsNum = CountRecords(cursor);

    BOOST_CHECK_EQUAL(recordsNum, entryCnt);
}

/**
 * Test text query.
 */
BOOST_AUTO_TEST_CASE(TestTextQuery)
{
    Cache<int, QueryPerson> cache = GetPersonCache();

    // Test query with no results.
    TextQuery qry("QueryPerson", "A1");

    QueryCursor<int, QueryPerson> cursor = cache.Query(qry);
    CheckEmpty(cursor);

    cursor = cache.Query(qry);
    CheckEmptyGetAll(cursor);

    // Test simple query.
    cache.Put(1, QueryPerson("A1", 10, MakeDateGmt(1990, 03, 18),
        MakeTimestampGmt(2016, 02, 10, 17, 39, 34, 579304685)));

    cache.Put(2, QueryPerson("A2", 20, MakeDateGmt(1989, 10, 26),
        MakeTimestampGmt(2016, 02, 10, 17, 39, 35, 678403201)));

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
    Cache<int, QueryPerson> cache = GetPersonCache();

    // Test query with no results.
    ScanQuery qry;

    QueryCursor<int, QueryPerson> cursor = cache.Query(qry);
    CheckEmpty(cursor);

    cursor = cache.Query(qry);
    CheckEmptyGetAll(cursor);

    // Test simple query.
    cache.Put(1, QueryPerson("A1", 10, MakeDateGmt(1990, 03, 18),
        MakeTimestampGmt(2016, 02, 10, 17, 39, 34, 579304685)));

    cursor = cache.Query(qry);
    CheckSingle(cursor, 1, "A1", 10);

    cursor = cache.Query(qry);
    CheckSingleGetAll(cursor, 1, "A1", 10);

    // Test query returning multiple entries.
    cache.Put(2, QueryPerson("A2", 20, MakeDateGmt(1989, 10, 26),
        MakeTimestampGmt(2016, 02, 10, 17, 39, 35, 678403201)));

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
    Cache<int, QueryPerson> cache = GetPersonCache();

    int32_t partCnt = 256;   // Defined in configuration explicitly.
    int32_t entryCnt = 1000; // Should be greater than partCnt.
    
    for (int i = 0; i < entryCnt; i++) 
    {
        std::stringstream stream; 

        stream << "A" << i;

        cache.Put(i, QueryPerson(stream.str(), i * 10, MakeDateGmt(1970 + i),
            MakeTimestampGmt(2016, 1, 1, i / 60, i % 60)));
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
 * Basic test for SQL fields query.
 */
BOOST_AUTO_TEST_CASE(TestSqlFieldsQueryBasic)
{
    Cache<int, QueryPerson> cache = GetPersonCache();

    // Test query with no results.
    SqlFieldsQuery qry("select _key, name, age from QueryPerson where age < 20");

    QueryFieldsCursor cursor = cache.Query(qry);
    CheckEmpty(cursor);

    // Test simple query.
    cache.Put(1, QueryPerson("A1", 10, MakeDateGmt(1990, 03, 18),
        MakeTimestampGmt(2016, 02, 10, 17, 39, 34, 579304685)));

    cache.Put(2, QueryPerson("A2", 20, MakeDateGmt(1989, 10, 26),
        MakeTimestampGmt(2016, 02, 10, 17, 39, 35, 678403201)));

    cursor = cache.Query(qry);
    CheckSingle(cursor, 1, "A1", 10);

    // Test simple distributed joins query.
    BOOST_CHECK(!qry.IsDistributedJoins());
    BOOST_CHECK(!qry.IsEnforceJoinOrder());

    qry.SetDistributedJoins(true);

    BOOST_CHECK(qry.IsDistributedJoins());
    BOOST_CHECK(!qry.IsEnforceJoinOrder());

    qry.SetEnforceJoinOrder(true);

    BOOST_CHECK(qry.IsDistributedJoins());
    BOOST_CHECK(qry.IsEnforceJoinOrder());

    cursor = cache.Query(qry);
    CheckSingle(cursor, 1, "A1", 10);
    
    qry.SetDistributedJoins(false);
    qry.SetEnforceJoinOrder(false);

    // Test simple local query.
    BOOST_CHECK(!qry.IsLocal());

    qry.SetLocal(true);

    BOOST_CHECK(qry.IsLocal());

    cursor = cache.Query(qry);
    CheckSingle(cursor, 1, "A1", 10);

    // Test query with arguments.
    qry.SetSql("select _key, name, age from QueryPerson where age < ? AND name = ?");
    qry.AddArgument<int>(20);
    qry.AddArgument<std::string>("A1");

    cursor = cache.Query(qry);
    CheckSingle(cursor, 1, "A1", 10);
}

/**
 * Test SQL fields query distributed joins.
 */
BOOST_AUTO_TEST_CASE(TestSqlFieldsQueryDistributedJoins)
{
    Cache<int, QueryPerson> cache1 = GetPersonCache();
    Cache<int, QueryRelation> cache2 = GetRelationCache();

    // Starting second node.
    Ignite node2 = StartNode("Node2");

    int entryCnt = 1000;

    // Filling caches
    for (int i = 0; i < entryCnt; i++)
    {
        std::stringstream stream;

        stream << "A" << i;

        cache1.Put(i, QueryPerson(stream.str(), i * 10, MakeDateGmt(1970 + i),
            MakeTimestampGmt(2016, 1, 1, i / 60, i % 60)));

        cache2.Put(i + 1, QueryRelation(i, i * 10));
    }

    // Test query with no results.
    SqlFieldsQuery qry(
        "select age, name "
        "from \"QueryPerson\".QueryPerson "
        "inner join \"QueryRelation\".QueryRelation "
        "on \"QueryPerson\".QueryPerson.age = \"QueryRelation\".QueryRelation.someVal");

    QueryFieldsCursor cursor = cache1.Query(qry);

    // Ensure that data is not collocated, so not full result set is returned.
    int recordsNum = CountRecords(cursor);

    BOOST_CHECK_GT(recordsNum, 0);
    BOOST_CHECK_LT(recordsNum, entryCnt);

    qry.SetDistributedJoins(true);

    cursor = cache1.Query(qry);

    // Check that full result set is returned.
    recordsNum = CountRecords(cursor);

    BOOST_CHECK_EQUAL(recordsNum, entryCnt);
}

/**
 * Test fields query with single entry.
 */
BOOST_AUTO_TEST_CASE(TestFieldsQuerySingle)
{
    // Test simple query.
    Cache<int, QueryPerson> cache = GetPersonCache();

    // Test query with two fields of different type.
    SqlFieldsQuery qry("select age, name from QueryPerson");

    QueryFieldsCursor cursor = cache.Query(qry);
    CheckEmpty(cursor);
    
    // Test simple query.
    cache.Put(1, QueryPerson("A1", 10, MakeDateGmt(1990, 03, 18),
        MakeTimestampGmt(2016, 02, 10, 17, 39, 34, 579304685)));

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
 * Test fields query with single entry.
 */
BOOST_AUTO_TEST_CASE(TestFieldsQueryExceptions)
{
    // Test simple query.
    Cache<int, QueryPerson> cache = GetPersonCache();

    // Test query with two fields of different type.
    SqlFieldsQuery qry("select age, name from QueryPerson");

    QueryFieldsCursor cursor = cache.Query(qry);
    CheckEmpty(cursor);

    // Test simple query.
    cache.Put(1, QueryPerson("A1", 10, MakeDateGmt(1990, 03, 18),
        MakeTimestampGmt(2016, 02, 10, 17, 39, 34, 579304685)));

    cursor = cache.Query(qry);

    try
    {
        BOOST_REQUIRE(cursor.HasNext());

        QueryFieldsRow row = cursor.GetNext();

        BOOST_REQUIRE(row.HasNext());

        int age = row.GetNext<int>();

        BOOST_REQUIRE(age == 10);

        std::string name = row.GetNext<std::string>();

        BOOST_REQUIRE(name == "A1");

        BOOST_REQUIRE(!row.HasNext());

        CheckEmpty(cursor);
    }
    catch (IgniteError& error)
    {
        BOOST_FAIL(error.GetText());
    }
}

/**
 * Test fields query with two simultaneously handled rows.
 */
BOOST_AUTO_TEST_CASE(TestFieldsQueryTwo)
{
    // Test simple query.
    Cache<int, QueryPerson> cache = GetPersonCache();

    // Test query with two fields of different type.
    SqlFieldsQuery qry("select age, name from QueryPerson");

    QueryFieldsCursor cursor = cache.Query(qry);
    CheckEmpty(cursor);

    // Test simple query.
    cache.Put(1, QueryPerson("A1", 10, MakeDateGmt(1990, 03, 18),
        MakeTimestampGmt(2016, 02, 10, 17, 39, 34, 579304685)));

    cache.Put(2, QueryPerson("A2", 20, MakeDateGmt(1989, 10, 26),
        MakeTimestampGmt(2016, 02, 10, 17, 39, 35, 678403201)));

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
    Cache<int, QueryPerson> cache = GetPersonCache();

    // Test query with two fields of different type.
    SqlFieldsQuery qry("select name, age from QueryPerson");

    QueryFieldsCursor cursor = cache.Query(qry);
    CheckEmpty(cursor);

    int32_t entryCnt = 1000; // Number of entries.

    for (int i = 0; i < entryCnt; i++)
    {
        std::stringstream stream;

        stream << "A" << i;

        QueryPerson val(stream.str(), i * 10, MakeDateGmt(1980 + i, 1, 1),
            MakeTimestampGmt(2016, 1, 1, i / 60, i % 60));

        cache.Put(i, val);
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

/**
 * Test query for Date type.
 */
BOOST_AUTO_TEST_CASE(TestFieldsQueryDateLess)
{
    // Test simple query.
    Cache<int, QueryPerson> cache = GetPersonCache();

    // Test query with field of type 'Date'.
    SqlFieldsQuery qry("select birthday from QueryPerson where birthday<'1990-01-01'");

    QueryFieldsCursor cursor = cache.Query(qry);
    CheckEmpty(cursor);

    int32_t entryCnt = 100; // Number of entries.

    for (int i = 0; i < entryCnt; i++)
    {
        std::stringstream stream;

        stream << "A" << i;

        QueryPerson val(stream.str(), i * 10, MakeDateGmt(1980 + i, 1, 1),
            MakeTimestampGmt(2016, 1, 1, i / 60, i % 60));

        cache.Put(i, val);
    }

    cursor = cache.Query(qry);

    IgniteError error;

    int32_t resultSetSize = 0; // Number of entries in query result set.

    while (cursor.HasNext(error))
    {
        BOOST_REQUIRE(error.GetCode() == IgniteError::IGNITE_SUCCESS);

        QueryFieldsRow row = cursor.GetNext(error);
        BOOST_REQUIRE(error.GetCode() == IgniteError::IGNITE_SUCCESS);

        BOOST_REQUIRE(row.HasNext(error));
        BOOST_REQUIRE(error.GetCode() == IgniteError::IGNITE_SUCCESS);

        Date birthday = row.GetNext<Date>(error);
        BOOST_REQUIRE(error.GetCode() == IgniteError::IGNITE_SUCCESS);

        BOOST_CHECK(birthday == MakeDateGmt(1980 + resultSetSize, 1, 1));

        BOOST_CHECK(birthday < MakeDateGmt(1990, 1, 1));

        BOOST_REQUIRE(!row.HasNext());

        ++resultSetSize;
    }

    BOOST_CHECK_EQUAL(resultSetSize, 10);

    CheckEmpty(cursor);
}

/**
 * Test query for Date type.
 */
BOOST_AUTO_TEST_CASE(TestFieldsQueryDateMore)
{
    // Test simple query.
    Cache<int, QueryPerson> cache = GetPersonCache();

    // Test query with field of type 'Date'.
    SqlFieldsQuery qry("select birthday from QueryPerson where birthday>'2070-01-01'");

    QueryFieldsCursor cursor = cache.Query(qry);
    CheckEmpty(cursor);

    int32_t entryCnt = 100; // Number of entries.

    for (int i = 0; i < entryCnt; i++)
    {
        std::stringstream stream;

        stream << "A" << i;

        QueryPerson val(stream.str(), i * 10, MakeDateGmt(1980 + i, 1, 1),
            MakeTimestampGmt(2016, 1, 1, i / 60, i % 60));

        cache.Put(i, val);
    }

    cursor = cache.Query(qry);

    IgniteError error;

    int32_t resultSetSize = 0; // Number of entries in query result set.

    while (cursor.HasNext(error))
    {
        BOOST_REQUIRE(error.GetCode() == IgniteError::IGNITE_SUCCESS);

        QueryFieldsRow row = cursor.GetNext(error);
        BOOST_REQUIRE(error.GetCode() == IgniteError::IGNITE_SUCCESS);

        BOOST_REQUIRE(row.HasNext(error));
        BOOST_REQUIRE(error.GetCode() == IgniteError::IGNITE_SUCCESS);

        Date birthday = row.GetNext<Date>(error);
        BOOST_REQUIRE(error.GetCode() == IgniteError::IGNITE_SUCCESS);

        BOOST_CHECK(birthday == MakeDateGmt(2071 + resultSetSize, 1, 1));

        BOOST_CHECK(birthday > MakeDateGmt(2070, 1, 1));

        BOOST_REQUIRE(!row.HasNext());

        ++resultSetSize;
    }

    BOOST_CHECK_EQUAL(resultSetSize, 9);

    CheckEmpty(cursor);
}

/**
 * Test query for Date type.
 */
BOOST_AUTO_TEST_CASE(TestFieldsQueryDateEqual)
{
    // Test simple query.
    Cache<int, QueryPerson> cache = GetPersonCache();

    // Test query with field of type 'Date'.
    SqlFieldsQuery qry("select birthday from QueryPerson where birthday='2032-01-01'");

    QueryFieldsCursor cursor = cache.Query(qry);
    CheckEmpty(cursor);

    int32_t entryCnt = 100; // Number of entries.

    for (int i = 0; i < entryCnt; i++)
    {
        std::stringstream stream;

        stream << "A" << i;

        QueryPerson val(stream.str(), i * 10, MakeDateGmt(1980 + i, 1, 1),
            MakeTimestampGmt(2016, 1, 1, i / 60, i % 60));

        cache.Put(i, val);
    }

    cursor = cache.Query(qry);

    IgniteError error;

    BOOST_REQUIRE(cursor.HasNext(error));

    BOOST_REQUIRE(error.GetCode() == IgniteError::IGNITE_SUCCESS);

    QueryFieldsRow row = cursor.GetNext(error);
    BOOST_REQUIRE(error.GetCode() == IgniteError::IGNITE_SUCCESS);

    BOOST_REQUIRE(row.HasNext(error));
    BOOST_REQUIRE(error.GetCode() == IgniteError::IGNITE_SUCCESS);

    Date birthday = row.GetNext<Date>(error);
    BOOST_REQUIRE(error.GetCode() == IgniteError::IGNITE_SUCCESS);

    BOOST_CHECK(birthday == MakeDateGmt(2032, 1, 1));

    BOOST_REQUIRE(!row.HasNext());

    CheckEmpty(cursor);
}

/**
 * Test query for Timestamp type.
 */
BOOST_AUTO_TEST_CASE(TestFieldsQueryTimestampLess)
{
    // Test simple query.
    Cache<int, QueryPerson> cache = GetPersonCache();

    // Test query with field of type 'Timestamp'.
    SqlFieldsQuery qry("select recordCreated from QueryPerson where recordCreated<'2016-01-01 01:00:00'");

    QueryFieldsCursor cursor = cache.Query(qry);
    CheckEmpty(cursor);

    int32_t entryCnt = 1000; // Number of entries.

    for (int i = 0; i < entryCnt; i++)
    {
        std::stringstream stream;

        stream << "A" << i;

        QueryPerson val(stream.str(), i * 10, MakeDateGmt(1980 + i, 1, 1),
            MakeTimestampGmt(2016, 1, 1, i / 60, i % 60));

        cache.Put(i, val);
    }

    cursor = cache.Query(qry);

    IgniteError error;

    int32_t resultSetSize = 0; // Number of entries in query result set.

    while (cursor.HasNext(error))
    {
        BOOST_REQUIRE(error.GetCode() == IgniteError::IGNITE_SUCCESS);

        QueryFieldsRow row = cursor.GetNext(error);
        BOOST_REQUIRE(error.GetCode() == IgniteError::IGNITE_SUCCESS);

        BOOST_REQUIRE(row.HasNext(error));
        BOOST_REQUIRE(error.GetCode() == IgniteError::IGNITE_SUCCESS);

        Timestamp recordCreated = row.GetNext<Timestamp>(error);
        BOOST_REQUIRE(error.GetCode() == IgniteError::IGNITE_SUCCESS);

        BOOST_CHECK(recordCreated == MakeTimestampGmt(2016, 1, 1, 0, resultSetSize % 60, 0));

        BOOST_CHECK(recordCreated < MakeTimestampGmt(2016, 1, 1, 1, 0, 0));

        BOOST_REQUIRE(!row.HasNext());

        ++resultSetSize;
    }

    BOOST_CHECK_EQUAL(resultSetSize, 60);

    CheckEmpty(cursor);
}

/**
 * Test query for Timestamp type.
 */
BOOST_AUTO_TEST_CASE(TestFieldsQueryTimestampMore)
{
    // Test simple query.
    Cache<int, QueryPerson> cache = GetPersonCache();

    // Test query with field of type 'Timestamp'.
    SqlFieldsQuery qry("select recordCreated from QueryPerson where recordCreated>'2016-01-01 15:30:00'");

    QueryFieldsCursor cursor = cache.Query(qry);
    CheckEmpty(cursor);

    int32_t entryCnt = 1000; // Number of entries.

    for (int i = 0; i < entryCnt; i++)
    {
        std::stringstream stream;

        stream << "A" << i;

        QueryPerson val(stream.str(), i * 10, MakeDateGmt(1980 + i, 1, 1),
            MakeTimestampGmt(2016, 1, 1, i / 60, i % 60));

        cache.Put(i, val);
    }

    cursor = cache.Query(qry);

    IgniteError error;

    int32_t resultSetSize = 0; // Number of entries in query result set.

    while (cursor.HasNext(error))
    {
        BOOST_REQUIRE(error.GetCode() == IgniteError::IGNITE_SUCCESS);

        QueryFieldsRow row = cursor.GetNext(error);
        BOOST_REQUIRE(error.GetCode() == IgniteError::IGNITE_SUCCESS);

        BOOST_REQUIRE(row.HasNext(error));
        BOOST_REQUIRE(error.GetCode() == IgniteError::IGNITE_SUCCESS);

        Timestamp recordCreated = row.GetNext<Timestamp>(error);
        BOOST_REQUIRE(error.GetCode() == IgniteError::IGNITE_SUCCESS);

        int32_t minutes = resultSetSize + 31;

        BOOST_CHECK(recordCreated == MakeTimestampGmt(2016, 1, 1, 15 + minutes / 60, minutes % 60, 0));

        BOOST_CHECK(recordCreated > MakeTimestampGmt(2016, 1, 1, 15, 30, 0));

        BOOST_REQUIRE(!row.HasNext());

        ++resultSetSize;
    }

    BOOST_CHECK_EQUAL(resultSetSize, 69);

    CheckEmpty(cursor);
}

/**
 * Test query for Timestamp type.
 */
BOOST_AUTO_TEST_CASE(TestFieldsQueryTimestampEqual)
{
    // Test simple query.
    Cache<int, QueryPerson> cache = GetPersonCache();

    // Test query with field of type 'Timestamp'.
    SqlFieldsQuery qry("select recordCreated from QueryPerson where recordCreated='2016-01-01 09:18:00'");

    QueryFieldsCursor cursor = cache.Query(qry);
    CheckEmpty(cursor);

    int32_t entryCnt = 1000; // Number of entries.

    for (int i = 0; i < entryCnt; i++)
    {
        std::stringstream stream;

        stream << "A" << i;

        QueryPerson val(stream.str(), i * 10, MakeDateGmt(1980 + i, 1, 1),
            MakeTimestampGmt(2016, 1, 1, i / 60, i % 60));

        cache.Put(i, val);
    }

    cursor = cache.Query(qry);

    IgniteError error;

    BOOST_REQUIRE(cursor.HasNext(error));

    BOOST_REQUIRE(error.GetCode() == IgniteError::IGNITE_SUCCESS);

    QueryFieldsRow row = cursor.GetNext(error);
    BOOST_REQUIRE(error.GetCode() == IgniteError::IGNITE_SUCCESS);

    BOOST_REQUIRE(row.HasNext(error));
    BOOST_REQUIRE(error.GetCode() == IgniteError::IGNITE_SUCCESS);

    Timestamp recordCreated = row.GetNext<Timestamp>(error);
    BOOST_REQUIRE(error.GetCode() == IgniteError::IGNITE_SUCCESS);

    BOOST_CHECK(recordCreated == MakeTimestampGmt(2016, 1, 1, 9, 18, 0));

    BOOST_REQUIRE(!row.HasNext());

    CheckEmpty(cursor);
}

/**
 * Test fields query with several pages.
 */
BOOST_AUTO_TEST_CASE(TestFieldsQueryPagesSeveral)
{
    CheckFieldsQueryPages(32, 8, 1);
}

/**
 * Test fields query with page size 1.
 */
BOOST_AUTO_TEST_CASE(TestFieldsQueryPageSingle)
{
    CheckFieldsQueryPages(1, 100, 0);
}

/**
 * Test fields query with page size 0.
 */
BOOST_AUTO_TEST_CASE(TestFieldsQueryPageZero)
{
    try
    {
        CheckFieldsQueryPages(0, 100, 0);

        BOOST_FAIL("Exception expected.");
    }
    catch (IgniteError&)
    {
        // Expected.
    }
}

BOOST_AUTO_TEST_SUITE_END()
