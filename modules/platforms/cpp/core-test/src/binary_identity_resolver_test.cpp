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
#include "ignite/cache/query/query_sql_fields.h"
#include "ignite/ignite.h"
#include "ignite/ignition.h"
#include "ignite/binary/binary_array_identity_resolver.h"

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
struct CompositeKey
{
    /**
     * Default constructor.
     */
    CompositeKey() : 
        str(),
        ts(),
        guid()
    {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param str String part.
     * @param ts Timestamp part.
     * @param guid Guid part.
     */
    CompositeKey(const std::string& str, const Timestamp& ts, const Guid& guid) : 
        str(str),
        ts(ts),
        guid(guid)
    {
        // No-op.
    }

    /** String part. */
    std::string str;

    /** Timestamp. */
    Timestamp ts;

    /** Guid. */
    Guid guid;
};


namespace ignite
{
    namespace binary
    {
        /**
         * Binary type definition for QueryPerson.
         */
        template<>
        struct BinaryType<CompositeKey>
        {
            IGNITE_BINARY_GET_TYPE_ID_AS_HASH(CompositeKey)
            IGNITE_BINARY_GET_TYPE_NAME_AS_IS(CompositeKey)
            IGNITE_BINARY_GET_FIELD_ID_AS_HASH
            IGNITE_BINARY_IS_NULL_FALSE(CompositeKey)
            IGNITE_BINARY_GET_NULL_DEFAULT_CTOR(CompositeKey)

            int32_t GetHashCode(const CompositeKey& obj)
            {
                return BinaryArrayIdentityResolver::GetHashCode(obj);
            }

            void Write(BinaryWriter& writer, const CompositeKey& obj)
            {
                writer.WriteString("str", obj.str);
                writer.WriteTimestamp("ts", obj.ts);
                writer.WriteGuid("guid", obj.guid);
            }

            CompositeKey Read(BinaryReader& reader)
            {
                CompositeKey val;

                val.str = reader.ReadString("str");
                val.ts = reader.ReadTimestamp("ts");
                val.guid = reader.ReadGuid("guid");
            
                return val;
            }
        };
    }
}

/**
 * Test setup fixture.
 */
struct BinaryIdentityResolverTestSuiteFixture
{
    /**
     * Constructor.
     */
    BinaryIdentityResolverTestSuiteFixture() :
        grid(ignite_test::StartNode("cache-identity.xml"))
    {
        // No-op.
    }

    /**
     * Destructor.
     */
    ~BinaryIdentityResolverTestSuiteFixture()
    {
        Ignition::StopAll(true);
    }

    /** Node started during the test. */
    Ignite grid;
};

BOOST_FIXTURE_TEST_SUITE(BinaryIdentityResolverTestSuite, BinaryIdentityResolverTestSuiteFixture)

/**
 * Test SQL query.
 */
BOOST_AUTO_TEST_CASE(TestIdentityEquility)
{
    CompositeKey key("Key String", Timestamp(123851, 562304134), Guid(0x4A950C6206FE4502, 0xAC06145097E56F02));
    int32_t value = 12321;

    Cache<CompositeKey, int32_t> cache = grid.GetOrCreateCache<CompositeKey, int32_t>("cache1");

    SqlFieldsQuery qry("INSERT INTO Integer (str, ts, guid, _val) VALUES (?, ?, ?, ?)");

    qry.AddArgument(key.str);
    qry.AddArgument(key.ts);
    qry.AddArgument(key.guid);
    qry.AddArgument(value);

    cache.Query(qry);

    int32_t realValue = cache.Get(key);

    BOOST_CHECK_EQUAL(value, realValue);
}

BOOST_AUTO_TEST_SUITE_END()
