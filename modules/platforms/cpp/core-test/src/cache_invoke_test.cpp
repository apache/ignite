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

#include "ignite/common/utils.h"
#include "ignite/ignite.h"
#include "ignite/ignition.h"

using namespace boost::unit_test;

using namespace ignite;
using namespace ignite::cache;
using namespace ignite::common;

/**
 * CacheEntryModifier class for invoke tests.
 */
class CacheEntryModifier
{
public:
    /**
     * Constructor.
     */
    CacheEntryModifier() : num(0)
    {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param num Number to substract from the entry.
     */
    CacheEntryModifier(int num) : num(num)
    {
        // No-op.
    }

    /**
     * Copy constructor.
     *
     * @param other Other instance.
     */
    CacheEntryModifier(const CacheEntryModifier& other) : num(other.num)
    {
        // No-op.
    }

    /**
     * Assignment operator.
     *
     * @param other Other instance.
     * @return This instance.
     */
    CacheEntryModifier& operator=(const CacheEntryModifier& other)
    {
        num = other.num;

        return *this;
    }

    /**
     * Call instance.
     *
     * @return New value of entry multiplied by two.
     */
    int Process(MutableCacheEntry<int, int>& entry, const int& arg)
    {
        if (entry.IsExists())
            entry.SetValue(entry.GetValue() - arg - num);
        else
            entry.SetValue(42);

        return entry.GetValue() * 2;
    }

    /**
     * Get number.
     *
     * @return Number to substract from entry value.
     */
    int GetNum() const
    {
        return num;
    }

    /**
     * Get Job Id.
     *
     * @return Job id.
     */
    static int64_t GetJobId()
    {
        return 100500;
    }

private:
    /** Number to substract. */
    int num;
};

/**
 * Binary type definition for CacheEntryModifier.
 */
IGNITE_BINARY_TYPE_START(CacheEntryModifier)
    IGNITE_BINARY_GET_TYPE_ID_AS_HASH(CacheEntryModifier)
    IGNITE_BINARY_GET_TYPE_NAME_AS_IS(CacheEntryModifier)
    IGNITE_BINARY_GET_FIELD_ID_AS_HASH
    IGNITE_BINARY_GET_HASH_CODE_ZERO(CacheEntryModifier)
    IGNITE_BINARY_IS_NULL_FALSE(CacheEntryModifier)
    IGNITE_BINARY_GET_NULL_DEFAULT_CTOR(CacheEntryModifier)

    void Write(BinaryWriter& writer, CacheEntryModifier obj)
    {
        writer.WriteInt32("num", obj.GetNum());
    }

    CacheEntryModifier Read(BinaryReader& reader)
    {
        int num = reader.ReadInt32("num");

        return CacheEntryModifier(num);
    }
IGNITE_BINARY_TYPE_END

/**
 * Divisor class for invoke tests.
 */
class Divisor
{
public:
    /**
     * Constructor.
     */
    Divisor() : scale(1.0)
    {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param scale Scale.
     */
    Divisor(double scale) : scale(scale)
    {
        // No-op.
    }

    /**
     * Copy constructor.
     *
     * @param other Other instance.
     */
    Divisor(const Divisor& other) : scale(other.scale)
    {
        // No-op.
    }

    /**
     * Assignment operator.
     *
     * @param other Other instance.
     * @return This instance.
     */
    Divisor& operator=(const Divisor& other)
    {
        scale = other.scale;

        return *this;
    }

    /**
     * Call instance.
     *
     * @return New value before cast to int.
     */
    double Process(MutableCacheEntry<int, int>& entry, const double& arg)
    {
        double res = 0.0;

        if (entry.IsExists())
        {
            res = (entry.GetValue() / arg) * scale;

            entry.SetValue(static_cast<int>(res));
        }

        return res;
    }

    /**
     * Get scale.
     *
     * @return Scale.
     */
    double GetScale() const
    {
        return scale;
    }

    /**
     * Get Job Id.
     *
     * @return Job id.
     */
    static int64_t GetJobId()
    {
        return 42;
    }

private:
    /** Number to substract. */
    double scale;
};

/**
 * Binary type definition for Divisor.
 */
IGNITE_BINARY_TYPE_START(Divisor)
    IGNITE_BINARY_GET_TYPE_ID_AS_HASH(Divisor)
    IGNITE_BINARY_GET_TYPE_NAME_AS_IS(Divisor)
    IGNITE_BINARY_GET_FIELD_ID_AS_HASH
    IGNITE_BINARY_GET_HASH_CODE_ZERO(Divisor)
    IGNITE_BINARY_IS_NULL_FALSE(Divisor)
    IGNITE_BINARY_GET_NULL_DEFAULT_CTOR(Divisor)

    void Write(BinaryWriter& writer, Divisor obj)
    {
        writer.WriteDouble("scale", obj.GetScale());
    }

    Divisor Read(BinaryReader& reader)
    {
        double scale = reader.ReadDouble("scale");

        return Divisor(scale);
    }
IGNITE_BINARY_TYPE_END

/**
 * List CacheEntryModifier as a cache entry processor.
 */
IGNITE_CACHE_ENTRY_PROCESSOR_LIST_BEGIN
    IGNITE_CACHE_ENTRY_PROCESSOR_DECLARE(CacheEntryModifier, int, int, int, int)
    IGNITE_CACHE_ENTRY_PROCESSOR_DECLARE(Divisor, int, int, double, double)
IGNITE_CACHE_ENTRY_PROCESSOR_LIST_END

/**
 * Test setup fixture.
 */
struct CacheInvokeTestSuiteFixture {

    Ignite CreateGrid()
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
        cfg.jvmInitMem = 512;
        cfg.jvmMaxMem = 2048;
#endif

        cfg.springCfgPath = std::string(getenv("IGNITE_NATIVE_TEST_CPP_CONFIG_PATH")) + "/cache-query.xml";

        IgniteError err;

        Ignite grid0 = Ignition::Start(cfg, &err);

        if (err.GetCode() != IgniteError::IGNITE_SUCCESS)
            BOOST_ERROR(err.GetText());

        return grid0;
    }

    /**
     * Constructor.
     */
    CacheInvokeTestSuiteFixture()
    {
        grid = CreateGrid();
    }

    /**
     * Destructor.
     */
    ~CacheInvokeTestSuiteFixture()
    {
        Ignition::Stop(grid.GetName(), true);
    }

    Ignite grid;
};

BOOST_FIXTURE_TEST_SUITE(CacheInvokeTestSuite, CacheInvokeTestSuiteFixture)

/**
 * Test cache invoke on existing entry.
 */
BOOST_AUTO_TEST_CASE(TestExisting)
{
    Cache<int, int> cache = grid.GetOrCreateCache<int, int>("TestCache");

    cache.Put(5, 20);

    CacheEntryModifier ced(5);

    int res = cache.Invoke<int>(5, ced, 4);

    BOOST_REQUIRE(res == 22);

    BOOST_REQUIRE(cache.Get(5) == 11);
}

/**
 * Test cache invoke on non-existing entry.
 */
BOOST_AUTO_TEST_CASE(TestNonExisting)
{
    Cache<int, int> cache = grid.GetOrCreateCache<int, int>("TestCache");

    CacheEntryModifier ced;

    int res = cache.Invoke<int>(4, ced, 4);

    BOOST_CHECK_EQUAL(res, 84);

    BOOST_CHECK_EQUAL(cache.Get(4), 42);
}

/**
 * Test cache invoke on non-existing entry.
 */
BOOST_AUTO_TEST_CASE(TestSeveral)
{
    Cache<int, int> cache = grid.GetOrCreateCache<int, int>("TestCache");

    CacheEntryModifier ced(2);
    Divisor div(10.0);

    int res1 = cache.Invoke<int>(100, ced, 0);

    BOOST_CHECK_EQUAL(res1, 84);

    BOOST_CHECK_EQUAL(cache.Get(100), 42);

    double res2 = cache.Invoke<double>(100, div, 200.0);

    BOOST_CHECK_CLOSE(res2, 2.1, 1E-6);

    BOOST_CHECK_EQUAL(cache.Get(100), 2);

    res2 = cache.Invoke<double>(100, div, 3.0);

    BOOST_CHECK_CLOSE(res2, 6.6666666, 1E-6);

    BOOST_CHECK_EQUAL(cache.Get(100), 6);

    res1 = cache.Invoke<int>(100, ced, -12);

    BOOST_CHECK_EQUAL(res1, 32);

    BOOST_CHECK_EQUAL(cache.Get(100), 16);
}

BOOST_AUTO_TEST_SUITE_END()