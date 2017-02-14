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
#   define BOOST_TEST_DYN_LINK
#endif

#include <boost/test/unit_test.hpp>

#include "ignite/ignite.h"
#include "ignite/ignition.h"
#include "ignite/test_utils.h"

using namespace ignite;
using namespace boost::unit_test;


struct Person
{
    std::string name;
    int age;

    Person() : name(), age(0)
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
        IGNITE_BINARY_GET_HASH_CODE_ZERO(Person)
        IGNITE_BINARY_IS_NULL_FALSE(Person)
        IGNITE_BINARY_GET_NULL_DEFAULT_CTOR(Person)

        void Write(BinaryWriter& writer, Person obj)
        {
            writer.WriteString("name", obj.name);
            writer.WriteInt32("age", obj.age);            
        }

        Person Read(BinaryReader& reader)
        {
            std::string name = reader.ReadString("name");
            int age = reader.ReadInt32("age");
            
            return Person(name, age);
        }

        IGNITE_BINARY_TYPE_END
    }
}

/*
 * Test setup fixture.
 */
struct CacheStoreTestSuiteFixture
{
    /* Nodes started during the test. */
    Ignite node;

    /*
     * Constructor.
     */
    CacheStoreTestSuiteFixture() : 
        node(ignite_test::StartNode("cache-store.xml", "node"))
    {
        // No-op.
    }

    /*
     * Destructor.
     */
    ~CacheStoreTestSuiteFixture()
    {
        Ignition::Stop(node.GetName(), true);

        node = Ignite();
    }

    /**
     * Cache accessor.
     */
    cache::Cache<int32_t, Person> GetCache()
    {
        return node.GetOrCreateCache<int32_t, Person>("cache1");
    }
};

BOOST_FIXTURE_TEST_SUITE(CacheStoreTestSuite, CacheStoreTestSuiteFixture)

BOOST_AUTO_TEST_CASE(LoadCacheNoPredicate)
{
    cache::Cache<int32_t, Person> cache = GetCache();

    cache.LoadCache();

    BOOST_CHECK(!cache.IsEmpty());

    BOOST_CHECK_EQUAL(cache.Size(), 100);

    Person person42 = cache.Get(42);

    BOOST_CHECK_EQUAL(person42.age, 42);
    BOOST_CHECK_EQUAL(person42.name, "John Doe");
}

BOOST_AUTO_TEST_SUITE_END()