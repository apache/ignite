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

#include "ignite/ignition.h"

using namespace ignite;
using namespace cache;
using namespace boost::unit_test;

void InitConfig(IgniteConfiguration& cfg)
{
    cfg.jvmOpts.push_back("-Xdebug");
    cfg.jvmOpts.push_back("-Xnoagent");
    cfg.jvmOpts.push_back("-Djava.compiler=NONE");
    cfg.jvmOpts.push_back("-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005");
    cfg.jvmOpts.push_back("-XX:+HeapDumpOnOutOfMemoryError");

#ifdef IGNITE_TESTS_32
    cfg.jvmInitMem = 256;
    cfg.jvmMaxMem = 768;
#else
    cfg.jvmInitMem = 1024;
    cfg.jvmMaxMem = 4096;
#endif

    char* cfgPath = getenv("IGNITE_NATIVE_TEST_CPP_CONFIG_PATH");

    cfg.springCfgPath = std::string(cfgPath).append("/").append("cache-test.xml");
}

BOOST_AUTO_TEST_SUITE(InteropTestSuite)

#ifdef ENABLE_STRING_SERIALIZATION_VER_2_TESTS

BOOST_AUTO_TEST_CASE(StringUtfInvalidSequence)
{
    IgniteConfiguration cfg;

    InitConfig(cfg);

    Ignite ignite = Ignition::Start(cfg);

    Cache<std::string, std::string> cache = ignite.CreateCache<std::string, std::string>("Test");

    std::string initialValue;

    initialValue.push_back(static_cast<unsigned char>(0xD8));
    initialValue.push_back(static_cast<unsigned char>(0x00));

    try
    {
        cache.Put("key", initialValue);

        std::string cachedValue = cache.Get("key");

        BOOST_ERROR("Exception is expected due to invalid format.");
    }
    catch (const IgniteError&)
    {
        // Expected in this mode.
    }

    Ignition::StopAll(false);
}

BOOST_AUTO_TEST_CASE(StringUtfInvalidCodePoint)
{
    IgniteConfiguration cfg;

    InitConfig(cfg);

    putenv("IGNITE_BINARY_MARSHALLER_USE_STRING_SERIALIZATION_VER_2=true");

    Ignite ignite = Ignition::Start(cfg);

    Cache<std::string, std::string> cache = ignite.CreateCache<std::string, std::string>("Test");

    std::string initialValue;

    //    1110xxxx 10xxxxxx 10xxxxxx |
    // <=          11011000 00000000 | U+D8
    //  = 11101101 10100000 10000000 | ED A0 80
    initialValue.push_back(static_cast<unsigned char>(0xED));
    initialValue.push_back(static_cast<unsigned char>(0xA0));
    initialValue.push_back(static_cast<unsigned char>(0x80));

    cache.Put("key", initialValue);
    std::string cachedValue = cache.Get("key");

    // This is a valid case. Invalid code points are supported in this mode.
    BOOST_CHECK_EQUAL(initialValue, cachedValue);

    Ignition::StopAll(false);
}

#endif

BOOST_AUTO_TEST_CASE(StringUtfValid4ByteCodePoint)
{
    IgniteConfiguration cfg;

    InitConfig(cfg);

    Ignite ignite = Ignition::Start(cfg);

    Cache<std::string, std::string> cache = ignite.CreateCache<std::string, std::string>("Test");

    std::string initialValue;

    //    11110xxx 10xxxxxx 10xxxxxx 10xxxxxx |
    // <=             00001 00000001 01001011 | U+1014B
    // <=      000   010000   000101   001011 | U+1014B
    //  = 11110000 10010000 10000101 10001011 | F0 90 85 8B
    initialValue.push_back(static_cast<unsigned char>(0xF0));
    initialValue.push_back(static_cast<unsigned char>(0x90));
    initialValue.push_back(static_cast<unsigned char>(0x85));
    initialValue.push_back(static_cast<unsigned char>(0x8B));

    cache.Put("key", initialValue);
    std::string cachedValue = cache.Get("key");

    // This is a valid UTF-8 code point. Should be supported in default mode.
    BOOST_CHECK_EQUAL(initialValue, cachedValue);

    Ignition::StopAll(false);
}

BOOST_AUTO_TEST_SUITE_END()
