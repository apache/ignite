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

#include "ignite/cache/cache_peek_mode.h"
#include "ignite/ignite.h"
#include "ignite/ignition.h"
#include "ignite/test_utils.h"
#include "ignite/binary_test_defs.h"

using namespace ignite;
using namespace boost::unit_test;

struct Person
{
    std::string name;
    int age;

    Person() : name(""), age(0)
    {
        // No-op.
    }

    Person(std::string name, int age) : name(name), age(age)
    {
        // No-op.
    }
};

namespace ignite
{
    namespace binary
    {
        IGNITE_BINARY_TYPE_START(Person)
            IGNITE_BINARY_GET_TYPE_ID_AS_HASH(Person)
            IGNITE_BINARY_GET_TYPE_NAME_AS_IS(Person)
            IGNITE_BINARY_GET_FIELD_ID_AS_HASH
            IGNITE_BINARY_IS_NULL_FALSE(Person)
            IGNITE_BINARY_GET_NULL_DEFAULT_CTOR(Person)

            static void Write(BinaryWriter& writer, const Person& obj)
            {
                writer.WriteString("name", obj.name);
                writer.WriteInt32("age", obj.age);
            }

            static void Read(BinaryReader& reader, Person& dst)
            {
                dst.name = reader.ReadString("name");
                dst.age = reader.ReadInt32("age");
            }

        IGNITE_BINARY_TYPE_END
    }
}

/*
 * Test setup fixture.
 */
struct CacheTestSuiteFixture
{
    /* Nodes started during the test. */
    Ignite grid0;
    Ignite grid1;

    /** Cache accessor. */
    cache::Cache<int, int> Cache()
    {
        return grid0.GetCache<int, int>("partitioned");
    }

    /*
     * Constructor.
     */
    CacheTestSuiteFixture()
    {
#ifdef IGNITE_TESTS_32
        grid0 = ignite_test::StartNode("cache-test-32.xml", "grid-0");
        grid1 = ignite_test::StartNode("cache-test-32.xml", "grid-1");
#else
        grid0 = ignite_test::StartNode("cache-test.xml", "grid-0");
        grid1 = ignite_test::StartNode("cache-test.xml", "grid-1");
#endif
    }

    void PutGetStructWithEnumField(int32_t i32Field, ignite_test::core::binary::TestEnum::Type enumField,
        const std::string& strField)
    {
        typedef ignite_test::core::binary::TypeWithEnumField TypeWithEnumField;

        TypeWithEnumField val;
        val.i32Field = i32Field;
        val.enumField = enumField;
        val.strField = strField;

        cache::Cache<int, TypeWithEnumField> cache = grid0.GetOrCreateCache<int, TypeWithEnumField>("PutGetStructWithEnumField");

        BOOST_TEST_CHECKPOINT("Putting value into the cache");
        cache.Put(i32Field, val);

        BOOST_TEST_CHECKPOINT("Getting value from the cache");
        TypeWithEnumField res = cache.Get(i32Field);

        BOOST_CHECK_EQUAL(val.i32Field, res.i32Field);
        BOOST_CHECK_EQUAL(val.enumField, res.enumField);
        BOOST_CHECK_EQUAL(val.strField, res.strField);
    }

    /*
     * Destructor.
     */
    ~CacheTestSuiteFixture()
    {
        grid0 = Ignite();
        grid1 = Ignite();

        Ignition::StopAll(true);
    }
};

/*
 * Test setup fixture.
 */
struct CacheNativePersistenceTestSuiteFixture
{
    /* Nodes started during the test. */
    Ignite grid0;

    /** Cache accessor. */
    cache::Cache<int, int> Cache()
    {
        return grid0.GetCache<int, int>("partitioned");
    }

    /*
     * Constructor.
     */
    CacheNativePersistenceTestSuiteFixture()
    {
        ignite_test::ClearLfs();

#ifdef IGNITE_TESTS_32
        grid0 = ignite_test::StartNode("cache-native-persistence-test-32.xml", "grid-0");
#else
        grid0 = ignite_test::StartNode("cache-native-persistence-test.xml", "grid-0");
#endif
    }

    /*
     * Destructor.
     */
    ~CacheNativePersistenceTestSuiteFixture()
    {
        grid0 = Ignite();

        Ignition::StopAll(true);

        ignite_test::ClearLfs();
    }
};

BOOST_FIXTURE_TEST_SUITE(CacheTestSuite, CacheTestSuiteFixture)

BOOST_AUTO_TEST_CASE(TestRemoveAllKeys)
{
    cache::Cache<int, int> cache = Cache();

    cache.Put(1, 1);
    cache.Put(2, 2);
    cache.Put(3, 3);

    int size = cache.Size(cache::CachePeekMode::PRIMARY);

    BOOST_CHECK_EQUAL(3, size);

    cache.RemoveAll();

    size = cache.Size(cache::CachePeekMode::ALL);

    BOOST_CHECK_EQUAL(0, size);

    cache.Put(1, 1);
    cache.Put(2, 2);
    cache.Put(3, 3);

    int keys[] = { 1, 2, 4, 5 };

    std::set<int> keySet(keys, keys + 4);

    cache.RemoveAll(keySet);

    size = cache.Size(cache::CachePeekMode::PRIMARY);

    BOOST_CHECK_EQUAL(1, size);
}

BOOST_AUTO_TEST_CASE(TestRemoveAllKeysIterVector)
{
    cache::Cache<int, int> cache = Cache();

    int size = cache.Size(cache::CachePeekMode::ALL);

    BOOST_CHECK_EQUAL(0, size);

    cache.Put(1, 1);
    cache.Put(2, 2);
    cache.Put(3, 3);

    int keys[] = { 1, 2, 4, 5 };

    std::vector<int> keySet(keys, keys + 4);

    cache.RemoveAll(keySet.begin(), keySet.end());

    size = cache.Size(cache::CachePeekMode::PRIMARY);

    BOOST_CHECK_EQUAL(1, size);
}

BOOST_AUTO_TEST_CASE(TestRemoveAllKeysIterArray)
{
    cache::Cache<int, int> cache = Cache();

    int size = cache.Size(cache::CachePeekMode::ALL);

    BOOST_CHECK_EQUAL(0, size);

    cache.Put(1, 1);
    cache.Put(2, 2);
    cache.Put(3, 3);

    int keys[] = { 1, 2, 4, 5 };

    cache.RemoveAll(keys, keys + 4);

    size = cache.Size(cache::CachePeekMode::PRIMARY);

    BOOST_CHECK_EQUAL(1, size);
}

BOOST_AUTO_TEST_CASE(TestPut)
{
    cache::Cache<int, int> cache = Cache();

    cache.Put(1, 1);

    BOOST_REQUIRE(1 == cache.Get(1));
}

BOOST_AUTO_TEST_CASE(TestPutAll)
{
    std::map<int, int> map;

    for (int i = 0; i < 100; i++)
        map[i] = i + 1;

    cache::Cache<int, int> cache = Cache();

    cache.PutAll(map);

    for (int i = 0; i < 100; i++)
        BOOST_REQUIRE(i + 1 == cache.Get(i));
}

BOOST_AUTO_TEST_CASE(TestPutAllIterMap)
{
    std::map<int, int> map;

    for (int i = 0; i < 100; i++)
        map[i] = i + 1;

    cache::Cache<int, int> cache = Cache();

    cache.PutAll(map.begin(), map.end());

    for (int i = 0; i < 100; i++)
        BOOST_REQUIRE(i + 1 == cache.Get(i));
}

BOOST_AUTO_TEST_CASE(TestPutAllIterVector)
{
    std::vector< cache::CacheEntry<int, int> > entries;

    for (int i = 0; i < 100; i++)
        entries.push_back(cache::CacheEntry<int, int>(i, i + 1));

    cache::Cache<int, int> cache = Cache();

    cache.PutAll(entries.begin(), entries.end());

    for (int i = 0; i < 100; i++)
        BOOST_REQUIRE(i + 1 == cache.Get(i));
}

BOOST_AUTO_TEST_CASE(TestPutIfAbsent)
{
    cache::Cache<int, int> cache = Cache();

    BOOST_REQUIRE(true == cache.PutIfAbsent(1, 3));
    BOOST_REQUIRE(false == cache.PutIfAbsent(1, 3));
}

BOOST_AUTO_TEST_CASE(TestGet)
{
    cache::Cache<int, int> cache = Cache();

    cache.Put(1, 1);
    cache.Put(2, 2);

    BOOST_REQUIRE(1 == cache.Get(1));
    BOOST_REQUIRE(2 == cache.Get(2));

    BOOST_REQUIRE(0 == cache.Get(3));
}

BOOST_AUTO_TEST_CASE(TestGetAll)
{
    cache::Cache<int, int> cache = Cache();

    int keys[] = { 1, 2, 3, 4, 5 };

    std::set<int> keySet (keys, keys + 5);

    for (int i = 0; i < static_cast<int>(keySet.size()); i++)
        cache.Put(i + 1, i + 1);

    std::map<int, int> map = cache.GetAll(keySet);

    for (int i = 0; i < static_cast<int>(keySet.size()); i++)
        BOOST_REQUIRE(i + 1 == map[i + 1]);
}

BOOST_AUTO_TEST_CASE(TestGetAllIterMap)
{
    cache::Cache<int, int> cache = Cache();

    int keys[] = { 1, 2, 3, 4, 5 };

    for (int i = 0; i < 5; ++i)
        cache.Put(keys[i], i + 1);

    std::map<int, int> map;

    cache.GetAll(keys, keys + 5, std::inserter(map, map.begin()));

    for (int i = 0; i < 5; ++i)
        BOOST_REQUIRE(i + 1 == map[keys[i]]);
}

BOOST_AUTO_TEST_CASE(TestGetAllIterArray)
{
    cache::Cache<int, int> cache = Cache();

    int keys[] = { 1, 2, 3, 4, 5 };

    cache::CacheEntry<int, int> res[5];

    for (int i = 0; i < 5; ++i)
        cache.Put(keys[i], i + 1);

    cache.GetAll(keys, keys + 5, res);

    for (int i = 0; i < 5; ++i)
        BOOST_REQUIRE(res[i].GetKey() == res[i].GetValue());
}

BOOST_AUTO_TEST_CASE(TestGetAndPut)
{
    cache::Cache<int, int> cache = Cache();

    BOOST_REQUIRE(0 == cache.GetAndPut(1, 3));
    BOOST_REQUIRE(3 == cache.GetAndPut(1, 1));
    BOOST_REQUIRE(1 == cache.GetAndPut(1, 0));
}

BOOST_AUTO_TEST_CASE(TestGetAndPutIfAbsent)
{
    cache::Cache<int, int> cache = Cache();

    BOOST_REQUIRE(0 == cache.GetAndPutIfAbsent(1, 3));
    BOOST_REQUIRE(3 == cache.GetAndPutIfAbsent(1, 1));
    BOOST_REQUIRE(3 == cache.GetAndPutIfAbsent(1, 1));
}

BOOST_AUTO_TEST_CASE(TestGetAndRemove)
{
    cache::Cache<int, int> cache = Cache();

    cache.Put(1, 3);

    BOOST_REQUIRE(3 == cache.GetAndRemove(1));
    BOOST_REQUIRE(0 == cache.GetAndRemove(1));
}

BOOST_AUTO_TEST_CASE(TestGetAndReplace)
{
    cache::Cache<int, int> cache = Cache();

    BOOST_REQUIRE(0 == cache.GetAndReplace(1, 3));
    BOOST_REQUIRE(0 == cache.GetAndReplace(1, 3));

    cache.Put(1, 5);

    BOOST_REQUIRE(5 == cache.GetAndReplace(1, 3));
    BOOST_REQUIRE(3 == cache.GetAndReplace(1, 3));
}

BOOST_AUTO_TEST_CASE(TestContainsKey)
{
    cache::Cache<int, int> cache = Cache();

    BOOST_REQUIRE(false == cache.ContainsKey(1));

    cache.Put(1, 1);

    BOOST_REQUIRE(true == cache.ContainsKey(1));

    BOOST_REQUIRE(true == cache.Remove(1));

    BOOST_REQUIRE(false == cache.ContainsKey(1));
}

BOOST_AUTO_TEST_CASE(TestContainsKeys)
{
    cache::Cache<int, int> cache = Cache();

    int keys[] = { 1, 2 };

    std::set<int> keySet(keys, keys + 2);

    BOOST_REQUIRE(false == cache.ContainsKeys(keySet));

    cache.Put(1, 1);
    cache.Put(2, 2);

    BOOST_REQUIRE(true == cache.ContainsKeys(keySet));

    cache.Remove(1);

    BOOST_REQUIRE(false == cache.ContainsKeys(keySet));
}

BOOST_AUTO_TEST_CASE(TestContainsKeysIter)
{
    cache::Cache<int, int> cache = Cache();

    int keys[] = { 1, 2 };

    BOOST_REQUIRE(false == cache.ContainsKeys(keys, keys + 2));

    cache.Put(1, 1);
    cache.Put(2, 2);

    BOOST_REQUIRE(true == cache.ContainsKeys(keys, keys + 2));

    cache.Remove(1);

    BOOST_REQUIRE(false == cache.ContainsKeys(keys, keys + 2));
}

BOOST_AUTO_TEST_CASE(TestIsEmpty)
{
    cache::Cache<int, int> cache = Cache();

    BOOST_REQUIRE(true == cache.IsEmpty());

    cache.Put(1, 1);

    BOOST_REQUIRE(false == cache.IsEmpty());

    cache.Remove(1);

    BOOST_REQUIRE(true == cache.IsEmpty());
}

BOOST_AUTO_TEST_CASE(TestRemove)
{
    cache::Cache<int, int> cache = Cache();

    BOOST_REQUIRE(false == cache.Remove(1));

    cache.Put(1, 1);

    BOOST_REQUIRE(true == cache.Remove(1));
    BOOST_REQUIRE(false == cache.Remove(1));
    BOOST_REQUIRE(false == cache.ContainsKey(1));
}

BOOST_AUTO_TEST_CASE(TestClear)
{
    cache::Cache<int, int> cache = Cache();

    cache.Put(1, 1);

    BOOST_REQUIRE(true == cache.ContainsKey(1));

    cache.Clear(1);

    BOOST_REQUIRE(false == cache.ContainsKey(1));
}

BOOST_AUTO_TEST_CASE(TestLocalClear)
{
    cache::Cache<int, int> cache = Cache();

    cache.Put(0, 2);

    BOOST_REQUIRE(2 == cache.LocalPeek(0, cache::CachePeekMode::ALL));

    cache.LocalClear(0);

    BOOST_REQUIRE(0 == cache.LocalPeek(0, cache::CachePeekMode::ALL));
}

BOOST_AUTO_TEST_CASE(TestLocalClearAll)
{
    cache::Cache<int, int> cache = Cache();

    cache.Put(0, 3);
    cache.Put(1, 3);

    int keys[] = { 0, 1 };

    std::set<int> keySet(keys, keys + 2);

    BOOST_REQUIRE(3 == cache.LocalPeek(0, cache::CachePeekMode::ALL));
    BOOST_REQUIRE(3 == cache.LocalPeek(1, cache::CachePeekMode::ALL));

    cache.LocalClearAll(keySet);

    BOOST_REQUIRE(0 == cache.LocalPeek(0, cache::CachePeekMode::ALL));
    BOOST_REQUIRE(0 == cache.LocalPeek(1, cache::CachePeekMode::ALL));
}

BOOST_AUTO_TEST_CASE(TestLocalClearAllIterList)
{
    cache::Cache<int, int> cache = Cache();

    cache.Put(0, 3);
    cache.Put(1, 3);

    int keys[] = { 0, 1 };

    std::list<int> keySet(keys, keys + 2);

    BOOST_REQUIRE(3 == cache.LocalPeek(0, cache::CachePeekMode::ALL));
    BOOST_REQUIRE(3 == cache.LocalPeek(1, cache::CachePeekMode::ALL));

    cache.LocalClearAll(keySet.begin(), keySet.end());

    BOOST_REQUIRE(0 == cache.LocalPeek(0, cache::CachePeekMode::ALL));
    BOOST_REQUIRE(0 == cache.LocalPeek(1, cache::CachePeekMode::ALL));
}

BOOST_AUTO_TEST_CASE(TestLocalClearAllIterArray)
{
    cache::Cache<int, int> cache = Cache();

    cache.Put(0, 3);
    cache.Put(1, 3);

    int keys[] = { 0, 1 };

    BOOST_REQUIRE(3 == cache.LocalPeek(0, cache::CachePeekMode::ALL));
    BOOST_REQUIRE(3 == cache.LocalPeek(1, cache::CachePeekMode::ALL));

    cache.LocalClearAll(keys, keys + 2);

    BOOST_REQUIRE(0 == cache.LocalPeek(0, cache::CachePeekMode::ALL));
    BOOST_REQUIRE(0 == cache.LocalPeek(1, cache::CachePeekMode::ALL));
}

BOOST_AUTO_TEST_CASE(TestSizes)
{
    cache::Cache<int, int> cache = Cache();

    BOOST_REQUIRE(0 == cache.Size());

    cache.Put(1, 1);
    cache.Put(2, 2);

    BOOST_REQUIRE(2 <= cache.Size());

    BOOST_REQUIRE(1 <= cache.LocalSize(cache::CachePeekMode::ALL));
}

BOOST_AUTO_TEST_CASE(TestLocalEvict)
{
    cache::Cache<int, int> cache = Cache();

    cache.Put(1, 5);

    BOOST_REQUIRE(5 == cache.LocalPeek(1, cache::CachePeekMode::ONHEAP));

    int keys[] = { 0, 1 };

    std::set<int> keySet(keys, keys + 2);

    cache.LocalEvict(keySet);

    BOOST_REQUIRE(0 == cache.LocalPeek(1, cache::CachePeekMode::ONHEAP));

    BOOST_REQUIRE(5 == cache.Get(1));

    BOOST_REQUIRE(5 == cache.LocalPeek(1, cache::CachePeekMode::ONHEAP));
}

BOOST_AUTO_TEST_CASE(TestLocalEvictIterSet)
{
    cache::Cache<int, int> cache = Cache();

    cache.Put(1, 5);

    BOOST_REQUIRE(5 == cache.LocalPeek(1, cache::CachePeekMode::ONHEAP));

    int keys[] = { 0, 1 };

    std::set<int> keySet(keys, keys + 2);

    cache.LocalEvict(keySet.begin(), keySet.end());

    BOOST_REQUIRE(0 == cache.LocalPeek(1, cache::CachePeekMode::ONHEAP));

    BOOST_REQUIRE(5 == cache.Get(1));

    BOOST_REQUIRE(5 == cache.LocalPeek(1, cache::CachePeekMode::ONHEAP));
}

BOOST_AUTO_TEST_CASE(TestLocalEvictIterArray)
{
    cache::Cache<int, int> cache = Cache();

    cache.Put(1, 5);

    BOOST_REQUIRE(5 == cache.LocalPeek(1, cache::CachePeekMode::ONHEAP));

    int keys[] = { 0, 1 };

    cache.LocalEvict(keys, keys + 2);

    BOOST_REQUIRE(0 == cache.LocalPeek(1, cache::CachePeekMode::ONHEAP));

    BOOST_REQUIRE(5 == cache.Get(1));

    BOOST_REQUIRE(5 == cache.LocalPeek(1, cache::CachePeekMode::ONHEAP));
}

BOOST_AUTO_TEST_CASE(TestBinary)
{
    cache::Cache<int, Person> cache = grid0.GetCache<int, Person>("partitioned");

    Person person("John Johnson", 3);

    cache.Put(1, person);

    Person person0 = cache.Get(1);

    BOOST_REQUIRE(person.age == person0.age);
    BOOST_REQUIRE(person.name.compare(person0.name) == 0);
}

BOOST_AUTO_TEST_CASE(TestCreateCache)
{
    // Create new cache
    cache::Cache<int, int> cache = grid0.CreateCache<int, int>("dynamic_cache");

    cache.Put(5, 7);

    BOOST_REQUIRE(7 == cache.Get(5));

    // Attempt to create cache with existing name
    IgniteError err;

    grid0.CreateCache<int, int>("dynamic_cache", err);

    BOOST_REQUIRE(err.GetCode() != IgniteError::IGNITE_SUCCESS);
}

BOOST_AUTO_TEST_CASE(TestGetOrCreateCache)
{
    // Get existing cache
    cache::Cache<int, int> cache = grid0.GetOrCreateCache<int, int>("partitioned");

    cache.Put(5, 7);

    BOOST_REQUIRE(7 == cache.Get(5));

    // Create new cache
    cache::Cache<int, int> cache2 = grid0.GetOrCreateCache<int, int>("partitioned_new");

    cache2.Put(5, 7);

    BOOST_REQUIRE(7 == cache2.Get(5));
}

BOOST_AUTO_TEST_CASE(TestPutGetDate)
{
    // Get existing cache
    cache::Cache<int, Date> cache = grid0.GetOrCreateCache<int, Date>("partitioned");

    Date now = Date(time(NULL) * 1000);

    cache.Put(5, now);

    BOOST_REQUIRE(now == cache.Get(5));
}

BOOST_AUTO_TEST_CASE(TestPutGetTime)
{
    // Get existing cache
    cache::Cache<int, Time> cache = grid0.GetOrCreateCache<int, Time>("partitioned");

    Time now = Time(time(NULL) * 1000);

    cache.Put(5, now);

    BOOST_REQUIRE(now == cache.Get(5));
}

BOOST_AUTO_TEST_CASE(TestPutGetTimestamp)
{
    // Get existing cache
    cache::Cache<int, Timestamp> cache = grid0.GetOrCreateCache<int, Timestamp>("partitioned");

    Timestamp now = Timestamp(time(NULL), 0);

    cache.Put(42, now);

    BOOST_REQUIRE(now == cache.Get(42));
}

BOOST_AUTO_TEST_CASE(TestGetBigString)
{
    // Get existing cache
    cache::Cache<int, std::string> cache = grid0.GetOrCreateCache<int, std::string>("partitioned");

    std::string longStr(impl::IgniteEnvironment::DEFAULT_ALLOCATION_SIZE * 10, 'a');

    cache.Put(5, longStr);

    BOOST_REQUIRE(longStr == cache.Get(5));
}

BOOST_AUTO_TEST_CASE(TestPutGetStructWithEnumField)
{
    typedef ignite_test::core::binary::TestEnum TestEnum;

    PutGetStructWithEnumField(0, TestEnum::TEST_ZERO, "");
    PutGetStructWithEnumField(1, TestEnum::TEST_ZERO, "");
    PutGetStructWithEnumField(0, TestEnum::TEST_NON_ZERO, "");
    PutGetStructWithEnumField(0, TestEnum::TEST_ZERO, "Lorem ipsum");
    PutGetStructWithEnumField(1, TestEnum::TEST_NON_ZERO, "Lorem ipsum");

    PutGetStructWithEnumField(13, TestEnum::TEST_NEGATIVE_42, "hishib");
    PutGetStructWithEnumField(1337, TestEnum::TEST_SOME_BIG, "Some test value");
}

BOOST_AUTO_TEST_SUITE_END()

BOOST_FIXTURE_TEST_SUITE(CacheTestSuiteNativePersistence, CacheNativePersistenceTestSuiteFixture);

BOOST_AUTO_TEST_CASE(TestWal)
{
    cluster::IgniteCluster cluster = grid0.GetCluster();

    cluster.SetActive(true);

    cache::Cache<int, int> cache = Cache();

    BOOST_REQUIRE(cluster.IsWalEnabled(cache.GetName()));
    cluster.DisableWal(cache.GetName());
    BOOST_REQUIRE(!cluster.IsWalEnabled(cache.GetName()));
    cluster.EnableWal(cache.GetName());
    BOOST_REQUIRE(cluster.IsWalEnabled(cache.GetName()));

    BOOST_CHECK_THROW(cluster.IsWalEnabled("foo"), IgniteError);
    BOOST_CHECK_THROW(cluster.DisableWal("foo"), IgniteError);
    BOOST_CHECK_THROW(cluster.EnableWal("foo"), IgniteError);
}

BOOST_AUTO_TEST_SUITE_END()
