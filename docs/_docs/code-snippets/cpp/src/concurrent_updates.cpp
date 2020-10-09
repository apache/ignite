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

    Ignition::Start(cfg);

    Ignite ignite = Ignition::Get();

    Cache<std::int32_t, std::string> cache = ignite.GetOrCreateCache<std::int32_t, std::string>("myCache");

    //tag::concurrent-updates[]
    for (int i = 1; i <= 5; i++)
    {
        Transaction tx = ignite.GetTransactions().TxStart();
        std::cout << "attempt #" << i << ", value: " << cache.Get(1) << std::endl;
        try {
            cache.Put(1, "new value");
            tx.Commit();
            std::cout << "attempt #" << i << " succeeded" << std::endl;
            break;
        }
        catch (IgniteError e)
        {
            if (!tx.IsRollbackOnly())
            {
                // Transaction was not marked as "rollback only",
                // so it's not a concurrent update issue.
                // Process the exception here.
                break;
            }
        }
    }
    //end::concurrent-updates[]
}
