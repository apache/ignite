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

#include <sstream>
#include <iterator>

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
#include "teamcity_messages.h"

using namespace boost::unit_test;

using namespace ignite;
using namespace ignite::cache;
using namespace ignite::cache::query;
using namespace ignite::common;

using ignite::impl::binary::BinaryUtils;

/**
 * Person class for query tests.
 */
class QueryPerson
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

    /**
     * @return true if objects are equal.
     */
    friend bool operator==(QueryPerson const& lhs, QueryPerson const& rhs)
    {
        return lhs.GetName() == rhs.GetName() && lhs.GetAge() == rhs.GetAge() &&
            lhs.GetBirthday() == rhs.GetBirthday() && lhs.GetCreationTime() == rhs.GetCreationTime();
    }

    /**
     * Outputs the object to stream.
     *
     * @return Stream.
     */
    friend std::ostream& operator<<(std::ostream& str, QueryPerson const& obj)
    {
        str << "QueryPerson::name: " << obj.GetName()
            << "QueryPerson::age: " << obj.GetAge()
            << "QueryPerson::birthday: " << obj.GetBirthday().GetMilliseconds()
            << "QueryPerson::recordCreated: " << obj.GetCreationTime().GetSeconds() << "." << obj.GetCreationTime().GetSecondFraction();
        return str;
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
class QueryRelation
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

/**
 * Byte array test type.
 */
struct ByteArrayType
{
    /**
     * Test constructor.
     *
     * @param val Init value.
     */
    ByteArrayType(int32_t val) :
        intVal(val),
        arrayVal(val + 1, val + 1)
    {
        // No-op.
    }

    /**
     * Default constructor.
     */
    ByteArrayType() :
        intVal(0),
        arrayVal()
    {
        // No-op.
    }

    /** Int field. */
    int32_t intVal;

    /** Array field. */
    std::vector<int8_t> arrayVal;
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
            IGNITE_BINARY_IS_NULL_FALSE(QueryPerson)
            IGNITE_BINARY_GET_NULL_DEFAULT_CTOR(QueryPerson)

            static void Write(BinaryWriter& writer, const QueryPerson& obj)
            {
                writer.WriteString("name", obj.GetName());
                writer.WriteInt32("age", obj.GetAge());
                writer.WriteDate("birthday", obj.GetBirthday());
                writer.WriteTimestamp("recordCreated", obj.GetCreationTime());
            }

            static void Read(BinaryReader& reader, QueryPerson& dst)
            {
                std::string name = reader.ReadString("name");
                int age = reader.ReadInt32("age");
                Date birthday = reader.ReadDate("birthday");
                Timestamp recordCreated = reader.ReadTimestamp("recordCreated");

                dst = QueryPerson(name, age, birthday, recordCreated);
            }
        IGNITE_BINARY_TYPE_END

        /**
         * Binary type definition for QueryRelation.
         */
        IGNITE_BINARY_TYPE_START(QueryRelation)
            IGNITE_BINARY_GET_TYPE_ID_AS_HASH(QueryRelation)
            IGNITE_BINARY_GET_TYPE_NAME_AS_IS(QueryRelation)
            IGNITE_BINARY_GET_FIELD_ID_AS_HASH
            IGNITE_BINARY_IS_NULL_FALSE(QueryRelation)
            IGNITE_BINARY_GET_NULL_DEFAULT_CTOR(QueryRelation)

            static void Write(BinaryWriter& writer, const QueryRelation& obj)
            {
                writer.WriteInt32("personId", obj.GetPersonId());
                writer.WriteInt32("someVal", obj.GetHobbyId());
            }

            static void Read(BinaryReader& reader, QueryRelation& dst)
            {
                int32_t personId = reader.ReadInt32("personId");
                int32_t someVal = reader.ReadInt32("someVal");

                dst = QueryRelation(personId, someVal);
            }
        IGNITE_BINARY_TYPE_END

        /**
         * Binary type definition for ByteArrayType.
         */
        IGNITE_BINARY_TYPE_START(ByteArrayType)
            IGNITE_BINARY_GET_TYPE_ID_AS_HASH(ByteArrayType)
            IGNITE_BINARY_GET_TYPE_NAME_AS_IS(ByteArrayType)
            IGNITE_BINARY_GET_FIELD_ID_AS_HASH
            IGNITE_BINARY_IS_NULL_FALSE(ByteArrayType)
            IGNITE_BINARY_GET_NULL_DEFAULT_CTOR(ByteArrayType)

            static void Write(BinaryWriter& writer, const ByteArrayType& obj)
            {
                writer.WriteInt32("intVal", obj.intVal);
                writer.WriteInt8Array("arrayVal", &obj.arrayVal[0], static_cast<int32_t>(obj.arrayVal.size()));
            }

            static void Read(BinaryReader& reader, ByteArrayType& dst)
            {
                dst.intVal = reader.ReadInt32("intVal");
                int32_t arrayValSize = reader.ReadInt8Array("arrayVal", 0, 0);

                dst.arrayVal.resize(static_cast<size_t>(arrayValSize));
                reader.ReadInt8Array("arrayVal", &dst.arrayVal[0], arrayValSize);
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
    BOOST_CHECK_EXCEPTION(cur.HasNext(), IgniteError, ignite_test::IsGenericError);
}

/**
 * Ensure that GetNext() fails.
 *
 * @param cur Cursor.
 */
template<typename Cursor>
void CheckGetNextFail(Cursor& cur)
{
    BOOST_CHECK_EXCEPTION(cur.GetNext(), IgniteError, ignite_test::IsGenericError);
}

/**
 * Ensure that GetAll() fails.
 *
 * @param cur Cursor.
 */
void CheckGetAllFail(QueryCursor<int, QueryPerson>& cur)
{
    std::vector<CacheEntry<int, QueryPerson> > res;

    BOOST_CHECK_EXCEPTION(cur.GetAll(res), IgniteError, ignite_test::IsGenericError);
}

/**
 * Ensure that iter version of GetAll() fails.
 *
 * @param cur Cursor.
 */
void CheckGetAllFailIter(QueryCursor<int, QueryPerson>& cur)
{
    std::vector<CacheEntry<int, QueryPerson> > res;

    BOOST_CHECK_EXCEPTION(cur.GetAll(std::back_inserter(res)), IgniteError, ignite_test::IsGenericError);
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
 * Check empty result through iter version of GetAll().
 *
 * @param cur Cursor.
 */
void CheckEmptyGetAllIter(QueryCursor<int, QueryPerson>& cur)
{
    std::vector<CacheEntry<int, QueryPerson> > res;

    cur.GetAll(std::back_inserter(res));

    BOOST_REQUIRE(res.size() == 0);

    CheckHasNextFail(cur);
    CheckGetNextFail(cur);
}

/**
 * Check single result through iteration.
 *
 * @param cur Cursor.
 * @param key Key.
 * @param name Name.
 * @param age Age.
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
 * @param key Key.
 * @param name Name.
 * @param age Age.
 */
void CheckSingleGetAll(QueryCursor<int, QueryPerson>& cur, int key, const std::string& name, int age)
{
    std::vector<CacheEntry<int, QueryPerson> > res;

    cur.GetAll(res);

    CheckHasNextFail(cur);
    CheckGetNextFail(cur);
    CheckGetAllFail(cur);

    BOOST_CHECK_EQUAL(res.size(), 1);

    BOOST_CHECK_EQUAL(res[0].GetKey(), key);
    BOOST_CHECK_EQUAL(res[0].GetValue().GetName(), name);
    BOOST_CHECK_EQUAL(res[0].GetValue().GetAge(), age);

    CheckHasNextFail(cur);
    CheckGetNextFail(cur);
    CheckGetAllFail(cur);
}

/**
 * Check single result through iter version of GetAll().
 *
 * @param cur Cursor.
 * @param key Key.
 * @param name Name.
 * @param age Age.
 */
void CheckSingleGetAllIter(QueryCursor<int, QueryPerson>& cur, int key, const std::string& name, int age)
{
    std::vector<CacheEntry<int, QueryPerson> > res;

    cur.GetAll(std::back_inserter(res));

    CheckHasNextFail(cur);
    CheckGetNextFail(cur);
    CheckGetAllFail(cur);

    BOOST_CHECK_EQUAL(res.size(), 1);

    BOOST_CHECK_EQUAL(res[0].GetKey(), key);
    BOOST_CHECK_EQUAL(res[0].GetValue().GetName(), name);
    BOOST_CHECK_EQUAL(res[0].GetValue().GetAge(), age);

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
            BOOST_CHECK_EQUAL(entry.GetValue().GetName(), name1);
            BOOST_CHECK_EQUAL(entry.GetValue().GetAge(), age1);
        }
        else if (entry.GetKey() == key2)
        {
            BOOST_CHECK_EQUAL(entry.GetValue().GetName(), name2);
            BOOST_CHECK_EQUAL(entry.GetValue().GetAge(), age2);
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

    BOOST_REQUIRE_EQUAL(res.size(), 2);

    for (int i = 0; i < 2; i++)
    {
        CacheEntry<int, QueryPerson> entry = res[i];

        if (entry.GetKey() == key1)
        {
            BOOST_CHECK_EQUAL(entry.GetValue().GetName(), name1);
            BOOST_CHECK_EQUAL(entry.GetValue().GetAge(), age1);
        }
        else if (entry.GetKey() == key2)
        {
            BOOST_CHECK_EQUAL(entry.GetValue().GetName(), name2);
            BOOST_CHECK_EQUAL(entry.GetValue().GetAge(), age2);
        }
        else
            BOOST_FAIL("Unexpected entry.");
    }
}

/**
 * Check multiple results through iter verion of GetAll().
 *
 * @param cur Cursor.
 * @param key1 Key 1.
 * @param name1 Name 1.
 * @param age1 Age 1.
 * @param key2 Key 2.
 * @param name2 Name 2.
 * @param age2 Age 2.
 */
void CheckMultipleGetAllIter(QueryCursor<int, QueryPerson>& cur, int key1, const std::string& name1,
    int age1, int key2, const std::string& name2, int age2)
{
    std::vector<CacheEntry<int, QueryPerson> > res;

    cur.GetAll(std::back_inserter(res));

    CheckHasNextFail(cur);
    CheckGetNextFail(cur);
    CheckGetAllFail(cur);

    BOOST_REQUIRE_EQUAL(res.size(), 2);

    for (int i = 0; i < 2; i++)
    {
        CacheEntry<int, QueryPerson> entry = res[i];

        if (entry.GetKey() == key1)
        {
            BOOST_CHECK_EQUAL(entry.GetValue().GetName(), name1);
            BOOST_CHECK_EQUAL(entry.GetValue().GetAge(), age1);
        }
        else if (entry.GetKey() == key2)
        {
            BOOST_CHECK_EQUAL(entry.GetValue().GetName(), name2);
            BOOST_CHECK_EQUAL(entry.GetValue().GetAge(), age2);
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
#ifdef IGNITE_TESTS_32
        return ignite_test::StartNode("cache-query-32.xml", name);
#else
        return ignite_test::StartNode("cache-query.xml", name);
#endif
    }

    void CheckFieldsQueryPages(int32_t pageSize, int32_t pagesNum, int32_t additionalNum)
    {
        // Test simple query.
        Cache<int, QueryPerson> cache = GetPersonCache();

        // Test query with two fields of different type.
        SqlFieldsQuery qry("select name, age from QueryPerson order by age");

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

            BOOST_REQUIRE_EQUAL(name, expected_name);

            int age = row.GetNext<int>(error);
            BOOST_REQUIRE(error.GetCode() == IgniteError::IGNITE_SUCCESS);

            BOOST_REQUIRE_EQUAL(age, expected_age);

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

    cursor = cache.Query(qry);
    CheckEmptyGetAllIter(cursor);

    // Test simple query.
    cache.Put(1, QueryPerson("A1", 10, MakeDateGmt(1990, 03, 18),
        MakeTimestampGmt(2016, 02, 10, 17, 39, 34, 579304685)));

    cache.Put(2, QueryPerson("A2", 20, MakeDateGmt(1989, 10, 26),
        MakeTimestampGmt(2016, 02, 10, 17, 39, 35, 678403201)));

    cursor = cache.Query(qry);
    CheckSingle(cursor, 1, "A1", 10);

    cursor = cache.Query(qry);
    CheckSingleGetAll(cursor, 1, "A1", 10);

    cursor = cache.Query(qry);
    CheckSingleGetAllIter(cursor, 1, "A1", 10);

    // Test simple distributed joins query.
    BOOST_CHECK(!qry.IsDistributedJoins());
    qry.SetDistributedJoins(true);
    BOOST_CHECK(qry.IsDistributedJoins());

    cursor = cache.Query(qry);
    CheckSingle(cursor, 1, "A1", 10);

    cursor = cache.Query(qry);
    CheckSingleGetAll(cursor, 1, "A1", 10);

    cursor = cache.Query(qry);
    CheckSingleGetAllIter(cursor, 1, "A1", 10);

    qry.SetDistributedJoins(false);

    // Test simple local query.
    BOOST_CHECK(!qry.IsLocal());
    qry.SetLocal(true);
    BOOST_CHECK(qry.IsLocal());

    cursor = cache.Query(qry);
    CheckSingle(cursor, 1, "A1", 10);

    cursor = cache.Query(qry);
    CheckSingleGetAll(cursor, 1, "A1", 10);

    cursor = cache.Query(qry);
    CheckSingleGetAllIter(cursor, 1, "A1", 10);

    // Test query with arguments.
    qry.SetSql("age < ? AND name = ?");
    qry.AddArgument<int>(20);
    qry.AddArgument<std::string>("A1");

    cursor = cache.Query(qry);
    CheckSingle(cursor, 1, "A1", 10);

    cursor = cache.Query(qry);
    CheckSingleGetAll(cursor, 1, "A1", 10);

    cursor = cache.Query(qry);
    CheckSingleGetAllIter(cursor, 1, "A1", 10);

    // Test resetting query arguments.
    qry.ClearArguments();
    qry.AddArgument<int>(30);
    qry.AddArgument<std::string>("A2");

    cursor = cache.Query(qry);
    CheckSingle(cursor, 2, "A2", 20);

    cursor = cache.Query(qry);
    CheckSingleGetAll(cursor, 2, "A2", 20);

    cursor = cache.Query(qry);
    CheckSingleGetAllIter(cursor, 2, "A2", 20);

    // Test query returning multiple entries.
    qry = SqlQuery("QueryPerson", "age < 30");

    cursor = cache.Query(qry);
    CheckMultiple(cursor, 1, "A1", 10, 2, "A2", 20);

    cursor = cache.Query(qry);
    CheckMultipleGetAll(cursor, 1, "A1", 10, 2, "A2", 20);

    cursor = cache.Query(qry);
    CheckMultipleGetAllIter(cursor, 1, "A1", 10, 2, "A2", 20);
}

/**
 * Test SQL query distributed joins.
 */
BOOST_AUTO_TEST_CASE(TestSqlQueryDistributedJoins)
{
    MUTE_TEST_FOR_TEAMCITY;

    Cache<int, QueryPerson> cache1 = GetPersonCache();
    Cache<int, QueryRelation> cache2 = GetRelationCache();

    // Starting second node.
    Ignite node2 = StartNode("Node2");

    int firstKey = 0;
    int entryCnt = 1000;

    // Filling caches
    for (int i = firstKey; i < firstKey + entryCnt; i++)
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
        "where (\"QueryPerson\".QueryPerson.age = \"QueryRelation\".QueryRelation.someVal) "
        "and (\"QueryPerson\".QueryPerson._key < 1000)");

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

    cursor = cache.Query(qry);
    CheckEmptyGetAllIter(cursor);

    // Test simple query.
    cache.Put(1, QueryPerson("A1", 10, MakeDateGmt(1990, 03, 18),
        MakeTimestampGmt(2016, 02, 10, 17, 39, 34, 579304685)));

    cache.Put(2, QueryPerson("A2", 20, MakeDateGmt(1989, 10, 26),
        MakeTimestampGmt(2016, 02, 10, 17, 39, 35, 678403201)));

    cursor = cache.Query(qry);
    CheckSingle(cursor, 1, "A1", 10);

    cursor = cache.Query(qry);
    CheckSingleGetAll(cursor, 1, "A1", 10);

    cursor = cache.Query(qry);
    CheckSingleGetAllIter(cursor, 1, "A1", 10);

    // Test simple local query.
    qry.SetLocal(true);

    cursor = cache.Query(qry);
    CheckSingle(cursor, 1, "A1", 10);

    cursor = cache.Query(qry);
    CheckSingleGetAll(cursor, 1, "A1", 10);

    cursor = cache.Query(qry);
    CheckSingleGetAllIter(cursor, 1, "A1", 10);

    // Test query returning multiple entries.
    qry = TextQuery("QueryPerson", "A*");

    cursor = cache.Query(qry);
    CheckMultiple(cursor, 1, "A1", 10, 2, "A2", 20);

    cursor = cache.Query(qry);
    CheckMultipleGetAll(cursor, 1, "A1", 10, 2, "A2", 20);

    cursor = cache.Query(qry);
    CheckMultipleGetAllIter(cursor, 1, "A1", 10, 2, "A2", 20);
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

    cursor = cache.Query(qry);
    CheckEmptyGetAllIter(cursor);

    // Test simple query.
    cache.Put(1, QueryPerson("A1", 10, MakeDateGmt(1990, 03, 18),
        MakeTimestampGmt(2016, 02, 10, 17, 39, 34, 579304685)));

    cursor = cache.Query(qry);
    CheckSingle(cursor, 1, "A1", 10);

    cursor = cache.Query(qry);
    CheckSingleGetAll(cursor, 1, "A1", 10);

    cursor = cache.Query(qry);
    CheckSingleGetAllIter(cursor, 1, "A1", 10);

    // Test query returning multiple entries.
    cache.Put(2, QueryPerson("A2", 20, MakeDateGmt(1989, 10, 26),
        MakeTimestampGmt(2016, 02, 10, 17, 39, 35, 678403201)));

    cursor = cache.Query(qry);
    CheckMultiple(cursor, 1, "A1", 10, 2, "A2", 20);

    cursor = cache.Query(qry);
    CheckMultipleGetAll(cursor, 1, "A1", 10, 2, "A2", 20);

    cursor = cache.Query(qry);
    CheckMultipleGetAllIter(cursor, 1, "A1", 10, 2, "A2", 20);
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
            BOOST_REQUIRE_EQUAL(entry.GetValue().GetName().compare(stream.str()), 0);

            BOOST_REQUIRE_EQUAL(entry.GetValue().GetAge(), key * 10);
        }
    }

    // Ensure that all keys were read.
    BOOST_CHECK_EQUAL(keys.size(), entryCnt);
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

    // Test resetting query arguments.
    qry.ClearArguments();
    qry.AddArgument<int>(30);
    qry.AddArgument<std::string>("A2");

    cursor = cache.Query(qry);
    CheckSingle(cursor, 2, "A2", 20);
}

/**
 * Test SQL fields query distributed joins.
 */
BOOST_AUTO_TEST_CASE(TestSqlFieldsQueryDistributedJoins)
{
    MUTE_TEST_FOR_TEAMCITY;

    Cache<int, QueryPerson> cache1 = GetPersonCache();
    Cache<int, QueryRelation> cache2 = GetRelationCache();

    // Starting second node.
    Ignite node2 = StartNode("Node2");

    int beginFrom = 2000;
    int entryCnt = 1000;

    // Filling caches
    for (int i = beginFrom; i < entryCnt + beginFrom; ++i)
    {
        std::stringstream stream;

        stream << "A" << i;

        cache1.Put(i, QueryPerson(stream.str(), i * 10, MakeDateGmt(1970 + (i / 100)),
            MakeTimestampGmt(2016, 1, 1, (i / 60) % 24, i % 60)));

        cache2.Put(i + 1, QueryRelation(i, i * 10));
    }

    // Test query with no results.
    SqlFieldsQuery qry(
        "select age, name "
        "from \"QueryPerson\".QueryPerson as QP "
        "inner join \"QueryRelation\".QueryRelation as QR "
        "on QP.age = QR.someVal "
        "where QP._key >= 2000 and QP._key < 3000");

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
    SqlFieldsQuery qry("select name, age from QueryPerson order by age");

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

        BOOST_REQUIRE_EQUAL(name, expected_name);

        int age = row.GetNext<int>(error);
        BOOST_REQUIRE(error.GetCode() == IgniteError::IGNITE_SUCCESS);

        BOOST_REQUIRE_EQUAL(age, expected_age);

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
 * Test query for Time type.
 */
BOOST_AUTO_TEST_CASE(TestFieldsQueryTimeEqual)
{
    // Test simple query.
    Cache<int, Time> cache = grid.GetOrCreateCache<int, Time>("TimeCache");

    // Test query with field of type 'Timestamp'.
    SqlFieldsQuery qry("select _key from Time where _val='04:11:02'");

    QueryFieldsCursor cursor = cache.Query(qry);
    CheckEmpty(cursor);

    int32_t entryCnt = 1000; // Number of entries.

    for (int i = 0; i < entryCnt; i++)
    {
        int secs = i % 60;
        int mins = i / 60;
        cache.Put(i, MakeTimeGmt(4, mins, secs));
    }

    cursor = cache.Query(qry);

    BOOST_REQUIRE(cursor.HasNext());

    QueryFieldsRow row = cursor.GetNext();

    BOOST_REQUIRE(row.HasNext());

    int key = row.GetNext<int>();

    BOOST_CHECK(key == 662);

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
    BOOST_CHECK_THROW(CheckFieldsQueryPages(0, 100, 0), IgniteError);
}

/**
 * Test query for key and value fields.
 */
BOOST_AUTO_TEST_CASE(TestKeyValFields)
{
    Cache<int, QueryPerson> cache = GetPersonCache();

    QueryPerson person("John", 30, MakeDateGmt(1987), MakeTimestampGmt(2017, 1, 1, 1, 1));

    cache.Put(1, person);

    for (int i = 0; i < 2; i++)
    {
        SqlFieldsQuery qry(i == 0 ?
            "select _key, _val, k, v, name, age, birthday, recordCreated from QueryPerson" :
            "select _key, _val, * from QueryPerson");

        QueryFieldsCursor cursor = cache.Query(qry);

        BOOST_REQUIRE(cursor.HasNext());

        QueryFieldsRow row = cursor.GetNext();

        BOOST_REQUIRE(row.HasNext());
        int id = row.GetNext<int>();
        BOOST_CHECK_EQUAL(1, id);

        BOOST_REQUIRE(row.HasNext());
        QueryPerson p = row.GetNext<QueryPerson>();
        BOOST_CHECK_EQUAL(p, person);

        BOOST_REQUIRE(row.HasNext());
        id = row.GetNext<int>();
        BOOST_CHECK_EQUAL(1, id);

        BOOST_REQUIRE(row.HasNext());
        p = row.GetNext<QueryPerson>();
        BOOST_CHECK_EQUAL(p, person);

        BOOST_REQUIRE(row.HasNext());
        std::string name = row.GetNext<std::string>();
        BOOST_CHECK_EQUAL(name, person.GetName());

        BOOST_REQUIRE(row.HasNext());
        int age = row.GetNext<int>();
        BOOST_CHECK_EQUAL(age, person.GetAge());

        BOOST_REQUIRE(row.HasNext());
        Date birthday = row.GetNext<Date>();
        BOOST_CHECK(birthday == person.GetBirthday());

        BOOST_REQUIRE(row.HasNext());
        Timestamp recordCreated = row.GetNext<Timestamp>();
        BOOST_CHECK(recordCreated == person.GetCreationTime());

        BOOST_CHECK(!row.HasNext());
    }
}

/**
 * Test query for Public schema.
 */
BOOST_AUTO_TEST_CASE(TestFieldsQuerySetSchema)
{
    Cache<int32_t, Time> timeCache = grid.GetCache<int32_t, Time>("TimeCache");

    int32_t entryCnt = 1000; // Number of entries.

    for (int i = 0; i < entryCnt; i++)
    {
        int secs = i % 60;
        int mins = i / 60;
        timeCache.Put(i, MakeTimeGmt(4, mins, secs));
    }

    Cache<int32_t, int32_t> intCache = grid.GetCache<int32_t, int32_t>("IntCache");

    SqlFieldsQuery qry("select _key from Time where _val='04:11:02'");

    BOOST_CHECK_EXCEPTION(intCache.Query(qry), IgniteError, ignite_test::IsGenericError);

    qry.SetSchema("TimeCache");

    QueryFieldsCursor cursor = intCache.Query(qry);

    BOOST_REQUIRE(cursor.HasNext());

    QueryFieldsRow row = cursor.GetNext();

    BOOST_REQUIRE(row.HasNext());

    int32_t key = row.GetNext<int32_t>();

    BOOST_CHECK(key == 662);

    BOOST_REQUIRE(!row.HasNext());

    CheckEmpty(cursor);
}

/**
 * Test query for byte arrays.
 */
BOOST_AUTO_TEST_CASE(TestFieldsQueryByteArraySelect)
{
    Cache<int32_t, ByteArrayType> byteArrayCache = grid.GetCache<int32_t, ByteArrayType>("ByteArrayCache");

    int32_t entryCnt = 100; // Number of entries.

    for (int32_t i = 0; i < entryCnt; i++)
        byteArrayCache.Put(i, ByteArrayType(i));

    SqlFieldsQuery qry("select intVal, arrayVal, intVal + 1 from ByteArrayType where _key=42");

    QueryFieldsCursor cursor = byteArrayCache.Query(qry);

    BOOST_REQUIRE(cursor.HasNext());

    QueryFieldsRow row = cursor.GetNext();

    BOOST_REQUIRE(row.HasNext());

    int32_t intVal1 = row.GetNext<int32_t>();

    BOOST_CHECK_EQUAL(intVal1, 42);

    BOOST_REQUIRE(row.HasNext());

    std::vector<int8_t> arrayVal;
    int32_t arrayValSize = row.GetNextInt8Array(0, 0);

    arrayVal.resize(static_cast<size_t>(arrayValSize));
    row.GetNextInt8Array(&arrayVal[0], arrayValSize);

    BOOST_CHECK_EQUAL(arrayValSize, 43);

    for (int32_t i = 0; i < arrayValSize; ++i)
        BOOST_CHECK_EQUAL(arrayVal[i], 43);

    BOOST_REQUIRE(row.HasNext());

    int32_t intVal2 = row.GetNext<int32_t>();

    BOOST_CHECK_EQUAL(intVal2, 43);

    BOOST_REQUIRE(!row.HasNext());

    CheckEmpty(cursor);
}

/**
 * Test query for byte arrays.
 */
BOOST_AUTO_TEST_CASE(TestFieldsQueryByteArrayInsert)
{
    Cache<int32_t, ByteArrayType> byteArrayCache = grid.GetCache<int32_t, ByteArrayType>("ByteArrayCache");

    SqlFieldsQuery qry("insert into ByteArrayType(_key, intVal, arrayVal) values (?, ?, ?)");

    int32_t entryCnt = 100; // Number of entries.

    for (int32_t i = 0; i < entryCnt; i++)
    {
        int32_t key = i;
        int32_t intVal = i;
        std::vector<int8_t> arrayVal(i + 1, i + 1);

        qry.AddArgument(key);
        qry.AddArgument(intVal);
        qry.AddInt8ArrayArgument(&arrayVal[0], i + 1);

        byteArrayCache.Query(qry);

        qry.ClearArguments();
    }

    ByteArrayType val = byteArrayCache.Get(42);

    BOOST_CHECK_EQUAL(val.intVal, 42);
    BOOST_CHECK_EQUAL(val.arrayVal.size(), 43);

    for (int32_t i = 0; i < 43; ++i)
        BOOST_CHECK_EQUAL(val.arrayVal[i], 43);
}

/**
 * Test query for byte arrays.
 */
BOOST_AUTO_TEST_CASE(TestFieldsQueryByteArrayInsertSelect)
{
    Cache<int32_t, ByteArrayType> byteArrayCache = grid.GetCache<int32_t, ByteArrayType>("ByteArrayCache");

    SqlFieldsQuery qry("insert into ByteArrayType(_key, intVal, arrayVal) values (?, ?, ?)");

    int32_t entryCnt = 100; // Number of entries.

    for (int32_t i = 0; i < entryCnt; i++)
    {
        int32_t key = i;
        int32_t intVal = i;
        std::vector<int8_t> arrayVal(i + 1, i + 1);

        qry.AddArgument(key);
        qry.AddArgument(intVal);
        qry.AddInt8ArrayArgument(&arrayVal[0], i + 1);

        byteArrayCache.Query(qry);

        qry.ClearArguments();
    }

    qry = SqlFieldsQuery("select intVal, arrayVal, intVal + 1 from ByteArrayType where _key=42");

    QueryFieldsCursor cursor = byteArrayCache.Query(qry);

    BOOST_REQUIRE(cursor.HasNext());

    QueryFieldsRow row = cursor.GetNext();

    BOOST_REQUIRE(row.HasNext());

    int32_t intVal1 = row.GetNext<int32_t>();

    BOOST_CHECK_EQUAL(intVal1, 42);

    BOOST_REQUIRE(row.HasNext());

    std::vector<int8_t> arrayVal;
    int32_t arrayValSize = row.GetNextInt8Array(0, 0);

    arrayVal.resize(static_cast<size_t>(arrayValSize));
    row.GetNextInt8Array(&arrayVal[0], arrayValSize);

    BOOST_CHECK_EQUAL(arrayValSize, 43);

    for (int32_t i = 0; i < arrayValSize; ++i)
        BOOST_CHECK_EQUAL(arrayVal[i], 43);

    BOOST_REQUIRE(row.HasNext());

    int32_t intVal2 = row.GetNext<int32_t>();

    BOOST_CHECK_EQUAL(intVal2, 43);

    BOOST_REQUIRE(!row.HasNext());

    CheckEmpty(cursor);
}

BOOST_AUTO_TEST_SUITE_END()
