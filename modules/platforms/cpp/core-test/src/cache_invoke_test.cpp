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

#include <sstream>
#include <algorithm>

#include <boost/test/unit_test.hpp>

#include "ignite/common/utils.h"
#include "ignite/ignite.h"
#include "ignite/ignition.h"

#include "ignite/test_utils.h"

#include "ignite/ignite_binding_context.h"
#include "ignite/cache/cache_entry_processor.h"

#include "ignite/test_utils.h"

using namespace boost::unit_test;

using namespace ignite;
using namespace ignite::cache;
using namespace ignite::common;

/**
 * CacheEntryModifier class for invoke tests.
 */
class CacheEntryModifier : public CacheEntryProcessor<int, int, int, int>
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
    virtual int Process(MutableCacheEntry<int, int>& entry, const int& arg)
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

private:
    /** Number to substract. */
    int num;
};

namespace ignite
{
    namespace binary
    {
        /**
         * Binary type definition for CacheEntryModifier.
         */
        IGNITE_BINARY_TYPE_START(CacheEntryModifier)
            IGNITE_BINARY_GET_TYPE_ID_AS_HASH(CacheEntryModifier)
            IGNITE_BINARY_GET_TYPE_NAME_AS_IS(CacheEntryModifier)
            IGNITE_BINARY_GET_FIELD_ID_AS_HASH
            IGNITE_BINARY_IS_NULL_FALSE(CacheEntryModifier)
            IGNITE_BINARY_GET_NULL_DEFAULT_CTOR(CacheEntryModifier)

            static void Write(BinaryWriter& writer, const CacheEntryModifier& obj)
            {
                writer.WriteInt32("num", obj.GetNum());
            }

            static void Read(BinaryReader& reader, CacheEntryModifier& dst)
            {
                int num = reader.ReadInt32("num");

                dst = CacheEntryModifier(num);
            }
        IGNITE_BINARY_TYPE_END
    }
}

/**
 * Divisor class for invoke tests.
 */
class Divisor : public CacheEntryProcessor<int, int, double, double>
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
    virtual double Process(MutableCacheEntry<int, int>& entry, const double& arg)
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

private:
    /** Scale. */
    double scale;
};

namespace ignite
{
    namespace binary
    {
        /**
         * Binary type definition for Divisor.
         */
        IGNITE_BINARY_TYPE_START(Divisor)
            IGNITE_BINARY_GET_TYPE_ID_AS_HASH(Divisor)
            IGNITE_BINARY_GET_TYPE_NAME_AS_IS(Divisor)
            IGNITE_BINARY_GET_FIELD_ID_AS_HASH
            IGNITE_BINARY_IS_NULL_FALSE(Divisor)
            IGNITE_BINARY_GET_NULL_DEFAULT_CTOR(Divisor)

            static void Write(BinaryWriter& writer, const Divisor& obj)
            {
                writer.WriteDouble("scale", obj.GetScale());
            }

            static void Read(BinaryReader& reader, Divisor& dst)
            {
                double scale = reader.ReadDouble("scale");

                dst = Divisor(scale);
            }
        IGNITE_BINARY_TYPE_END
    }
}

/**
 * Character remover class for invoke tests.
 */
class CharRemover : public CacheEntryProcessor<std::string, std::string, int, bool>
{
public:
    /**
     * Constructor.
     */
    CharRemover() : toRemove(0)
    {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param toRemove Char to remove.
     */
    CharRemover(char toRemove) : toRemove(toRemove)
    {
        // No-op.
    }

    /**
     * Copy constructor.
     *
     * @param other Other instance.
     */
    CharRemover(const CharRemover& other) : toRemove(other.toRemove)
    {
        // No-op.
    }

    /**
     * Assignment operator.
     *
     * @param other Other instance.
     * @return This instance.
     */
    CharRemover& operator=(const CharRemover& other)
    {
        toRemove = other.toRemove;

        return *this;
    }

    /**
     * Call instance.
     *
     * @return New value before cast to int.
     */
    virtual int Process(MutableCacheEntry<std::string, std::string>& entry, const bool& replaceWithSpace)
    {
        int res = 0;

        if (entry.IsExists())
        {
            std::string val(entry.GetValue());

            res = static_cast<int>(std::count(val.begin(), val.end(), toRemove));

            if (replaceWithSpace)
                std::replace(val.begin(), val.end(), toRemove, ' ');
            else
                val.erase(std::remove(val.begin(), val.end(), toRemove), val.end());

            if (val.empty())
                entry.Remove();
            else
                entry.SetValue(val);
        }

        return res;
    }

    /**
     * Get scale.
     *
     * @return Scale.
     */
    char GetCharToRemove() const
    {
        return toRemove;
    }

private:
    /** Char to remove. */
    char toRemove;
};

namespace ignite
{
    namespace binary
    {
        /**
         * Binary type definition for CharRemover.
         */
        IGNITE_BINARY_TYPE_START(CharRemover)
            IGNITE_BINARY_GET_TYPE_ID_AS_HASH(CharRemover)
            IGNITE_BINARY_GET_TYPE_NAME_AS_IS(CharRemover)
            IGNITE_BINARY_GET_FIELD_ID_AS_HASH
            IGNITE_BINARY_IS_NULL_FALSE(CharRemover)
            IGNITE_BINARY_GET_NULL_DEFAULT_CTOR(CharRemover)

            static void Write(BinaryWriter& writer, const CharRemover& obj)
            {
                writer.WriteInt8("toRemove", obj.GetCharToRemove());
            }

            static void Read(BinaryReader& reader, CharRemover& dst)
            {
                char toRemove = static_cast<char>(reader.ReadInt8("toRemove"));

                dst = CharRemover(toRemove);
            }
        IGNITE_BINARY_TYPE_END
    }
}

/**
 * PlatformComputeBinarizable class representation for the Java class with the
 * same name for invoke tests.
 */
struct PlatformComputeBinarizable
{
public:
    /**
     * Constructor.
     */
    PlatformComputeBinarizable() : field(0)
    {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param field Field.
     */
    PlatformComputeBinarizable(int32_t field) : field(field)
    {
        // No-op.
    }

    /**
     * Copy constructor.
     *
     * @param other Other instance.
     */
    PlatformComputeBinarizable(const PlatformComputeBinarizable& other) : field(other.field)
    {
        // No-op.
    }

    /**
     * Assignment operator.
     *
     * @param other Other instance.
     * @return This instance.
     */
    PlatformComputeBinarizable& operator=(const PlatformComputeBinarizable& other)
    {
        field = other.field;

        return *this;
    }

    /** Field. */
    int32_t field;
};

namespace ignite
{
    namespace binary
    {
        /**
         * Binary type definition for PlatformComputeBinarizable.
         */
        IGNITE_BINARY_TYPE_START(PlatformComputeBinarizable)
            IGNITE_BINARY_GET_TYPE_ID_AS_HASH(PlatformComputeBinarizable)
            IGNITE_BINARY_GET_TYPE_NAME_AS_IS(PlatformComputeBinarizable)
            IGNITE_BINARY_GET_FIELD_ID_AS_HASH
            IGNITE_BINARY_IS_NULL_FALSE(PlatformComputeBinarizable)
            IGNITE_BINARY_GET_NULL_DEFAULT_CTOR(PlatformComputeBinarizable)

            static void Write(BinaryWriter& writer, const PlatformComputeBinarizable& obj)
            {
                writer.WriteInt32("field", obj.field);
            }

            static void Read(BinaryReader& reader, PlatformComputeBinarizable& dst)
            {
                dst.field = reader.ReadInt32("field");
            }
        IGNITE_BINARY_TYPE_END
    }
}

IGNITE_EXPORTED_CALL void IgniteModuleInit(ignite::IgniteBindingContext& context)
{
    IgniteBinding binding = context.GetBinding();

    binding.RegisterCacheEntryProcessor<CacheEntryModifier>();
    binding.RegisterCacheEntryProcessor<Divisor>();
}

/**
 * Test setup fixture.
 */
struct CacheInvokeTestSuiteFixture
{
    Ignite node;

    /**
     * Constructor.
     */
    CacheInvokeTestSuiteFixture() :
#ifdef IGNITE_TESTS_32
        node(ignite_test::StartNode("cache-query-32.xml", "InvokeTest"))
#else
        node(ignite_test::StartNode("cache-query.xml", "InvokeTest"))
#endif
    {
        // No-op.
    }

    /**
     * Destructor.
     */
    ~CacheInvokeTestSuiteFixture()
    {
        Ignition::StopAll(true);
    }
};

BOOST_FIXTURE_TEST_SUITE(CacheInvokeTestSuite, CacheInvokeTestSuiteFixture)

/**
 * Test cache invoke on existing entry.
 */
BOOST_AUTO_TEST_CASE(TestExisting)
{
    Cache<int, int> cache = node.GetOrCreateCache<int, int>("TestCache");

    cache.Put(5, 20);

    CacheEntryModifier ced(5);

    int res = cache.Invoke<int>(5, ced, 4);

    BOOST_CHECK_EQUAL(res, 22);

    BOOST_CHECK_EQUAL(cache.Get(5), 11);
}

/**
 * Test cache invoke on non-existing entry.
 */
BOOST_AUTO_TEST_CASE(TestNonExisting)
{
    Cache<int, int> cache = node.GetOrCreateCache<int, int>("TestCache");

    CacheEntryModifier ced;

    int res = cache.Invoke<int>(4, ced, 4);

    BOOST_CHECK_EQUAL(res, 84);

    BOOST_CHECK_EQUAL(cache.Get(4), 42);
}

/**
 * Test cache several invokes on the same entry.
 */
BOOST_AUTO_TEST_CASE(TestSeveral)
{
    Cache<int, int> cache = node.GetOrCreateCache<int, int>("TestCache");

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

/**
 * Test cache several invokes on the string entry.
 */
BOOST_AUTO_TEST_CASE(TestStrings)
{
    IgniteBinding binding = node.GetBinding();

    binding.RegisterCacheEntryProcessor<CharRemover>();

    Cache<std::string, std::string> cache = node.GetOrCreateCache<std::string, std::string>("TestCache");

    CharRemover cr('.');

    int res = cache.Invoke<int>("some key", cr, false);

    BOOST_CHECK_EQUAL(res, 0);
    BOOST_CHECK(!cache.ContainsKey("some key"));

    cache.Put("some key", "Some.Value.Separated.By.Dots");

    res = cache.Invoke<int>("some key", cr, false);

    BOOST_CHECK_EQUAL(res, 4);
    BOOST_CHECK_EQUAL(cache.Get("some key"), std::string("SomeValueSeparatedByDots"));

    cache.Put("some key", "Some.Other.Weird.Value");

    res = cache.Invoke<int>("some key", cr, true);

    BOOST_CHECK_EQUAL(res, 3);
    BOOST_CHECK_EQUAL(cache.Get("some key"), std::string("Some Other Weird Value"));

    cache.Put("some key", "...........");

    res = cache.Invoke<int>("some key", cr, false);

    BOOST_CHECK_EQUAL(res, 11);
    BOOST_CHECK(!cache.ContainsKey("some key"));
}

/**
 * Test cache invoke of Java CacheEntryProcessor.
 */
BOOST_AUTO_TEST_CASE(TestJavaProcessor)
{
    const static std::string procName = "org.apache.ignite.platform.PlatformAddArgEntryProcessor";

    Cache<int64_t, int64_t> cache = node.GetOrCreateCache<int64_t, int64_t>("TestJavaProcessorCache");

    cache.Put(5, 10);

    int64_t res = cache.InvokeJava<int64_t, int64_t>(5, procName, 8);

    BOOST_CHECK_EQUAL(res, 18);
    BOOST_CHECK_EQUAL(cache.Get(5), 18);

    res = cache.InvokeJava<int64_t, int64_t>(4, procName, 42);

    BOOST_CHECK_EQUAL(res, 42);
    BOOST_CHECK_EQUAL(cache.Get(4), 42);
}


/**
 * Test cache invoke of Java CacheEntryProcessor binarizable.
 */
BOOST_AUTO_TEST_CASE(TestJavaProcessorBinarizable)
{
    const static std::string procName = "org.apache.ignite.platform.PlatformAddArgEntryProcessorBinarizable";

    Cache<int64_t, int64_t> cache = node.GetOrCreateCache<int64_t, int64_t>("TestJavaProcessorCacheBinarizable");

    cache.Put(15, 40);

    PlatformComputeBinarizable res = cache.InvokeJava<PlatformComputeBinarizable, int64_t>(15, procName, -12);

    BOOST_CHECK_EQUAL(res.field, 28);
    BOOST_CHECK_EQUAL(cache.Get(15), 28);

    res = cache.InvokeJava<PlatformComputeBinarizable, int64_t>(4, procName, 42);

    BOOST_CHECK_EQUAL(res.field, 42);
    BOOST_CHECK_EQUAL(cache.Get(4), 42);
}

BOOST_AUTO_TEST_SUITE_END()
