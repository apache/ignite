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

#include "ignite/ignition.h"

using namespace ignite;
using namespace ignite::transactions;
using namespace ignite::cache;
using namespace boost::unit_test;

BOOST_AUTO_TEST_SUITE(IgniteTransactionsTestSuite)

BOOST_AUTO_TEST_CASE(TestIgniteTransactionGet)
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
    cfg.jvmInitMem = 1024;
    cfg.jvmMaxMem = 4096;
#endif

    char* cfgPath = getenv("IGNITE_NATIVE_TEST_CPP_CONFIG_PATH");

    cfg.springCfgPath = std::string(cfgPath).append("/").append("cache-test.xml");

    // Start Ignite instance.
    Ignite grid = Ignition::Start(cfg, "txTest");

    Cache<int, int> cache = grid.CreateCache<int, int>("txCache");

    Transactions transactions = grid.GetTransactions();

    Transaction tx = transactions.GetTx();

    BOOST_REQUIRE(!tx.IsValid());

    tx = transactions.TxStart();

    Transaction tx2 = transactions.GetTx();

    BOOST_REQUIRE(tx2.IsValid());

    //cache.Put(1, 1);

    //cache.Put(2, 2);

    //tx.Commit();

    //BOOST_REQUIRE_EQUAL(1, cache.Get(1));

    //BOOST_REQUIRE_EQUAL(2, cache.Get(2));

    //tx = transactions.GetTx();

    //BOOST_REQUIRE(!tx.IsValid());

    Ignition::Stop(grid.GetName(), true);
}

BOOST_AUTO_TEST_SUITE_END()
