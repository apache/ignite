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
#include "ignite/ignite.h"
#include "ignite/ignition.h"

using namespace boost::unit_test;

using namespace ignite;
using namespace ignite::cache;
using namespace ignite::impl::utils;

/**
 * NumberSum class for compute tests.
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
     * Destructor.
     */
    ~CacheEntryModifier()
    {
        // No-op.
    }

    /**
     * Call instance.
     * @return New value of entry multiplied by two.
     */
    int Process(MutableCacheEntry<int, int>& entry, const int& arg)
    {
        if (entry.Exists())
        {
            int entryValue = entry.GetValue() - arg - num;

            entry.SetValue(entryValue);
        }
        else
        {
            entry.SetValue(42);
        }

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

private:
    /** Number to substract. */
    int num;
};

namespace ignite
{
    namespace portable
    {
        /**
         * Portable type definition for CacheEntryModifier.
         */
        IGNITE_PORTABLE_TYPE_START(CacheEntryModifier)
            IGNITE_PORTABLE_GET_TYPE_ID_AS_HASH(CacheEntryModifier)
            IGNITE_PORTABLE_GET_TYPE_NAME_AS_IS(CacheEntryModifier)
            IGNITE_PORTABLE_GET_FIELD_ID_AS_HASH
            IGNITE_PORTABLE_GET_HASH_CODE_ZERO(CacheEntryModifier)
            IGNITE_PORTABLE_IS_NULL_FALSE(CacheEntryModifier)
            IGNITE_PORTABLE_GET_NULL_DEFAULT_CTOR(CacheEntryModifier)

            void Write(PortableWriter& writer, CacheEntryModifier obj)
            {
                writer.WriteInt32("num", obj.GetNum());
            }

            CacheEntryModifier Read(PortableReader& reader)
            {
                int num = reader.ReadInt32("num");

                return CacheEntryModifier(num);
            }

        IGNITE_PORTABLE_TYPE_END
    }
}

IGNITE_REMOTE_JOB_LIST_BEGIN
    IGNITE_REMOTE_CACHE_ENTRY_PROCESSOR_DECLARE(CacheEntryModifier, int, int, int, int)
IGNITE_REMOTE_JOB_LIST_END

/**
 * Test setup fixture.
 */
struct CacheInvokeTestSuiteFixture {

    Ignite CreateGrid()
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

#ifdef IGNITE_TESTS_32
        cfg.jvmInitMem = 256;
        cfg.jvmMaxMem = 768;
#else
        cfg.jvmInitMem = 512;
        cfg.jvmMaxMem = 2048;
#endif

        char* cfgPath = getenv("IGNITE_NATIVE_TEST_CPP_CONFIG_PATH");

        std::string cfgPathStr = std::string(cfgPath).append("/").append("cache-query.xml");

        cfg.springCfgPath = const_cast<char*>(cfgPathStr.c_str());

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
    try
    {
        Cache<int, int> cache = grid.GetOrCreateCache<int, int>("TestCache");

        cache.Put(5, 20);

        CacheEntryModifier ced(5);

        int res = cache.Invoke<int>(5, ced, 4);

        BOOST_REQUIRE(res == 22);

        BOOST_REQUIRE(cache.Get(5) == 11); 
    }
    catch (IgniteError& err)
    {
        BOOST_FAIL(err.GetText());
    }
    catch (std::exception& err)
    {
        BOOST_FAIL(err.what());
    }
    catch (...)
    {
        BOOST_FAIL("Unknown error");
    }
}

/**
 * Test cache invoke on non-existing entry.
 */
BOOST_AUTO_TEST_CASE(TestNonExisting)
{
    try
    {
        Cache<int, int> cache = grid.GetOrCreateCache<int, int>("TestCache");

        CacheEntryModifier ced;

        int res = cache.Invoke<int>(4, ced, 4);

        std::cout << res << std::endl;

        BOOST_REQUIRE(res == 84);

        BOOST_REQUIRE(cache.Get(4) == 42); 
    }
    catch (IgniteError& err)
    {
        BOOST_FAIL(err.GetText());
    }
    catch (std::exception& err)
    {
        BOOST_FAIL(err.what());
    }
    catch (...)
    {
        BOOST_FAIL("Unknown error");
    }
}

BOOST_AUTO_TEST_SUITE_END()