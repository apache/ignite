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
#include <iostream>

#include "ignite/ignite.h"
#include "ignite/ignition.h"

using namespace ignite;
using namespace cache;
using namespace transactions;

int main()
{
    IgniteConfiguration cfg;
    cfg.springCfgPath = "/path/to/configuration.xml";

    Ignite ignite = Ignition::Start(cfg);
    
    Cache<int32_t, int32_t> cache = ignite.GetOrCreateCache<int32_t, int32_t>("myCache");

    //tag::transactions-pessimistic[]
    try {
        Transaction tx = ignite.GetTransactions().TxStart(
            TransactionConcurrency::PESSIMISTIC, TransactionIsolation::READ_COMMITTED, 300, 0);
        cache.Put(1, 1);
    
        cache.Put(2, 1);
    
        tx.Commit();
    }
    catch (IgniteError& err)
    {
        std::cout << "An error occurred: " << err.GetText() << std::endl;
        std::cin.get();
        return err.GetCode();
    }
    //end::transactions-pessimistic[]
}
