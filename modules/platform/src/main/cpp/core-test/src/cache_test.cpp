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

#include <boost/test/unit_test.hpp>

#include "ignite/cache/cache_peek_mode.h"
#include "ignite/ignite.h"
#include "ignite/ignition.h"

using namespace ignite;
using namespace boost::unit_test;

/* Nodes started during the test. */
Ignite grid0 = Ignite();
Ignite grid1 = Ignite();

/** Cache accessor. */
cache::Cache<int, int> Cache()
{
    return grid0.GetCache<int, int>("partitioned");
}

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
    namespace portable
    {
        IGNITE_PORTABLE_TYPE_START(Person)
        IGNITE_PORTABLE_GET_TYPE_ID_AS_HASH(Person)
        IGNITE_PORTABLE_GET_TYPE_NAME_AS_IS(Person)
        IGNITE_PORTABLE_GET_FIELD_ID_AS_HASH
        IGNITE_PORTABLE_GET_HASH_CODE_ZERO(Person)
        IGNITE_PORTABLE_IS_NULL_FALSE(Person)
        IGNITE_PORTABLE_GET_NULL_DEFAULT_CTOR(Person)
            
        void Write(PortableWriter& writer, Person obj)
        {
            writer.WriteString("name", obj.name);
            writer.WriteInt32("age", obj.age);            
        }

        Person Read(PortableReader& reader)
        {
            std::string name = reader.ReadString("name");
            int age = reader.ReadInt32("age");
            
            return Person(name, age);
        }

        IGNITE_PORTABLE_TYPE_END
    }
}

/*
 * Test setup fixture.
 */
struct CacheTestSuiteFixture {
    /*
     * Constructor.
     */
    CacheTestSuiteFixture()
    {
        IgniteConfiguration cfg;

        IgniteJvmOption opts[5];

        opts[0] = IgniteJvmOption("-Xdebug");
        opts[1] = IgniteJvmOption("-Xnoagent");
        opts[2] = IgniteJvmOption("-Djava.compiler=NONE");
        opts[3] = IgniteJvmOption("-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005");
        opts[4] = IgniteJvmOption("-XX:+HeapDumpOnOutOfMemoryError");

        cfg.jvmOptsLen = 5;
        cfg.jvmOpts = opts;

        cfg.jvmInitMem = 1024;
        cfg.jvmMaxMem = 4096;

        char* cfgPath = getenv("IGNITE_NATIVE_TEST_CPP_CONFIG_PATH");

        std::string cfgPathStr = std::string(cfgPath).append("/").append("cache-test.xml");

        cfg.springCfgPath = const_cast<char*>(cfgPathStr.c_str());
        
        for (int i = 0; i < 2; i++) 
        {
            std::stringstream stream;

            stream << "grid-" << i;

            IgniteError err;

            Ignite grid = Ignition::Start(cfg, stream.str().c_str(), &err);
                
            if (err.GetCode() != IgniteError::IGNITE_SUCCESS)
                BOOST_FAIL(err.GetText());

            if (i == 0)
                grid0 = grid;
            else
                grid1 = grid;
        }
    }

    /*
     * Destructor.
     */
    ~CacheTestSuiteFixture()
    {
        Ignition::Stop(grid0.GetName(), true);
        Ignition::Stop(grid1.GetName(), true);

        grid0 = Ignite();
        grid1 = Ignite();
    }
};

BOOST_FIXTURE_TEST_SUITE(CacheTestSuite, CacheTestSuiteFixture)

BOOST_AUTO_TEST_CASE(TestRemoveAllKeys)
{
    cache::Cache<int, int> cache = Cache();

    cache.Put(1, 1);
    cache.Put(2, 2);
    cache.Put(3, 3);

    int size = cache.Size(cache::IGNITE_PEEK_MODE_PRIMARY);

    BOOST_REQUIRE(3 == size);

    cache.RemoveAll();

    size = cache.Size(cache::IGNITE_PEEK_MODE_ALL);

    BOOST_REQUIRE(0 == size);

    cache.Put(1, 1);
    cache.Put(2, 2);
    cache.Put(3, 3);

    int keys[] = { 1, 2, 4, 5 };

    std::set<int> keySet(keys, keys + 4);

    cache.RemoveAll(keySet);

    size = cache.Size(cache::IGNITE_PEEK_MODE_PRIMARY);

    BOOST_REQUIRE(1 == size);
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

    for (int i = 0; i < keySet.size(); i++)
        cache.Put(i + 1, i + 1);

    std::map<int, int> map = cache.GetAll(keySet);

    for (int i = 0; i < keySet.size(); i++)
        BOOST_REQUIRE(i + 1 == map[i + 1]);
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

    BOOST_REQUIRE(2 == cache.LocalPeek(0, cache::IGNITE_PEEK_MODE_PRIMARY));

    cache.LocalClear(0);

    BOOST_REQUIRE(0 == cache.LocalPeek(0, cache::IGNITE_PEEK_MODE_PRIMARY));
}

BOOST_AUTO_TEST_CASE(TestLocalClearAll)
{
    cache::Cache<int, int> cache = Cache();

    cache.Put(0, 3);
    cache.Put(1, 3);

    int keys[] = { 0, 1 };

    std::set<int> keySet(keys, keys + 2);

    BOOST_REQUIRE(3 == cache.LocalPeek(0, cache::IGNITE_PEEK_MODE_PRIMARY));
    BOOST_REQUIRE(3 == cache.LocalPeek(1, cache::IGNITE_PEEK_MODE_PRIMARY));

    cache.LocalClearAll(keySet);

    BOOST_REQUIRE(0 == cache.LocalPeek(0, cache::IGNITE_PEEK_MODE_PRIMARY));
    BOOST_REQUIRE(0 == cache.LocalPeek(1, cache::IGNITE_PEEK_MODE_PRIMARY));
}

BOOST_AUTO_TEST_CASE(TestSizes)
{
    cache::Cache<int, int> cache = Cache();

    BOOST_REQUIRE(0 == cache.Size());

    cache.Put(1, 1);
    cache.Put(2, 2);

    BOOST_REQUIRE(2 <= cache.Size());

    BOOST_REQUIRE(1 <= cache.LocalSize(cache::IGNITE_PEEK_MODE_PRIMARY));
}

BOOST_AUTO_TEST_CASE(TestLocalEvict)
{
    cache::Cache<int, int> cache = Cache();

    cache.Put(1, 5);

    BOOST_REQUIRE(5 == cache.LocalPeek(1, cache::IGNITE_PEEK_MODE_ONHEAP));

    int keys[] = { 0, 1 };

    std::set<int> keySet(keys, keys + 2);

    cache.LocalEvict(keySet);

    BOOST_REQUIRE(0 == cache.LocalPeek(1, cache::IGNITE_PEEK_MODE_ONHEAP));

    BOOST_REQUIRE(5 == cache.Get(1));

    BOOST_REQUIRE(5 == cache.LocalPeek(1, cache::IGNITE_PEEK_MODE_ONHEAP));
}

BOOST_AUTO_TEST_CASE(TestPortable)
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

    grid0.CreateCache<int, int>("dynamic_cache", &err);

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

BOOST_AUTO_TEST_SUITE_END()