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
using namespace ignite::binary;
using namespace ignite::impl::interop;
using namespace ignite::impl::binary;

/**
 * Composite key class.
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

/**
 * Simple composite key class.
 */
struct CompositeKeySimple
{
    /**
     * Default constructor.
     */
    CompositeKeySimple() :
        str(),
        ts(),
        i64(0)
    {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param str String part.
     * @param ts Timestamp part.
     * @param i64 Integer part.
     */
    CompositeKeySimple(const std::string& str, const Timestamp& ts, int64_t i64) :
        str(str),
        ts(ts),
        i64(i64)
    {
        // No-op.
    }

    /** String part. */
    std::string str;

    /** Timestamp. */
    Timestamp ts;

    /** Integer 64-bit. */
    int64_t i64;
};

namespace ignite
{
    namespace binary
    {
        /**
         * Binary type definition for CompositeKey.
         */
        template<>
        struct BinaryType<CompositeKey>
        {
            IGNITE_BINARY_GET_TYPE_ID_AS_HASH(CompositeKey)
            IGNITE_BINARY_GET_TYPE_NAME_AS_IS(CompositeKey)
            IGNITE_BINARY_GET_FIELD_ID_AS_HASH
            IGNITE_BINARY_IS_NULL_FALSE(CompositeKey)
            IGNITE_BINARY_GET_NULL_DEFAULT_CTOR(CompositeKey)

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

        /**
         * Binary type definition for CompositeKey.
         */
        template<>
        struct BinaryType<CompositeKeySimple>
        {
            IGNITE_BINARY_GET_TYPE_ID_AS_HASH(CompositeKeySimple)
            IGNITE_BINARY_GET_TYPE_NAME_AS_IS(CompositeKeySimple)
            IGNITE_BINARY_GET_FIELD_ID_AS_HASH
            IGNITE_BINARY_IS_NULL_FALSE(CompositeKeySimple)
            IGNITE_BINARY_GET_NULL_DEFAULT_CTOR(CompositeKeySimple)

            void Write(BinaryWriter& writer, const CompositeKeySimple& obj)
            {
                writer.WriteString("str", obj.str);
                writer.WriteTimestamp("ts", obj.ts);
                writer.WriteInt64("i64", obj.i64);
            }

            CompositeKeySimple Read(BinaryReader& reader)
            {
                CompositeKeySimple val;

                val.str = reader.ReadString("str");
                val.ts = reader.ReadTimestamp("ts");
                val.i64 = reader.ReadInt64("i64");

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

template<typename T>
void FillMem(InteropMemory& mem, const T& value)
{
    InteropOutputStream stream(&mem);
    BinaryWriterImpl writer(&stream, 0);

    writer.WriteObject<T>(value);

    stream.Synchronize();
}

template<typename R, typename T>
int32_t CalculateHashCode(const T& value)
{
    InteropUnpooledMemory mem(1024);

    FillMem<T>(mem, value);

    BinaryObject obj(mem, 0);

    R resolver;

    return resolver.GetHashCode(obj);
}

BOOST_FIXTURE_TEST_SUITE(BinaryIdentityResolverTestSuite, BinaryIdentityResolverTestSuiteFixture)

BOOST_AUTO_TEST_CASE(TestGetDataHashCode)
{
    int8_t data1[] = { 0 };
    int8_t data2[] = { 0, 0, 0, 0 };
    int8_t data3[] = { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
    int8_t data4[] = { 1 };
    int8_t data5[] = { -1 };
    int8_t data6[] = { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };
    int8_t data7[] = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 0 };
    uint8_t data8[] = { 0xFF };
    uint8_t data9[] = { 0xFF, 0xFF, 0xFF, 0xFF };

    BOOST_CHECK_EQUAL(binary::GetDataHashCode(data1, sizeof(data1)), 0x0000001F);
    BOOST_CHECK_EQUAL(binary::GetDataHashCode(data2, sizeof(data2)), 0x000e1781);
    BOOST_CHECK_EQUAL(binary::GetDataHashCode(data3, sizeof(data3)), 0x94E4B2C1);
    BOOST_CHECK_EQUAL(binary::GetDataHashCode(data4, sizeof(data4)), 0x00000020);
    BOOST_CHECK_EQUAL(binary::GetDataHashCode(data5, sizeof(data5)), 0x0000001E);
    BOOST_CHECK_EQUAL(binary::GetDataHashCode(data6, sizeof(data6)), 0x9EBADAC6);
    BOOST_CHECK_EQUAL(binary::GetDataHashCode(data7, sizeof(data7)), 0xC5D38B5C);
    BOOST_CHECK_EQUAL(binary::GetDataHashCode(data8, sizeof(data8)), 0x0000001E);
    BOOST_CHECK_EQUAL(binary::GetDataHashCode(data9, sizeof(data9)), 0x000D9F41);
}

BOOST_AUTO_TEST_CASE(TestArrayIdentityResolver)
{
    using namespace binary;

    CompositeKey key1("Some test garbage, one-two-three...",
        Timestamp(109917, 130347199), Guid(0xACC064DF54EE9670, 0x065CF938F56E5E3B));

    CompositeKeySimple key2("!!!!!!!!!!!!!!!!", Timestamp(324140, 334685375), 89563963);

    BOOST_CHECK_EQUAL(CalculateHashCode<BinaryArrayIdentityResolver>(key1), 0xC298792B);
    BOOST_CHECK_EQUAL(CalculateHashCode<BinaryArrayIdentityResolver>(key2), 0x53207175);
}

BOOST_AUTO_TEST_CASE(TestIdentityEquilityWithGuid)
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

BOOST_AUTO_TEST_CASE(TestIdentityEquilityWithoutGuid)
{
    CompositeKeySimple key("Lorem ipsum", Timestamp(112460, 163002155), 1337);
    int32_t value = 42;

    Cache<CompositeKeySimple, int32_t> cache = grid.GetOrCreateCache<CompositeKeySimple, int32_t>("cache2");

    SqlFieldsQuery qry("INSERT INTO Integer (str, ts, i64, _val) VALUES (?, ?, ?, ?)");

    qry.AddArgument(key.str);
    qry.AddArgument(key.ts);
    qry.AddArgument(key.i64);
    qry.AddArgument(value);

    cache.Query(qry);

    int32_t realValue = cache.Get(key);

    BOOST_CHECK_EQUAL(value, realValue);
}

BOOST_AUTO_TEST_SUITE_END()
