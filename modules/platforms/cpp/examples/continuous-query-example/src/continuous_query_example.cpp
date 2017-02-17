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

#include <stdint.h>
#include <iostream>

#include "ignite/ignition.h"
#include "ignite/cache/query/continuous/continuous_query.h"

#include "ignite/examples/organization.h"
#include "ignite/examples/person.h"

using namespace ignite;
using namespace cache;
using namespace query;

using namespace examples;

/** Cache name. */
const char* CACHE_NAME = "cpp_cache_continuous_query";

/*
 * Listener class.
 */
template<typename K, typename V>
class Listener : public event::CacheEntryEventListener<K, V>
{
public:
    /*
     * Default constructor.
     */
    Listener()
    {
        // No-op.
    }

    /**
     * Event callback.
     *
     * @param evts Events.
     * @param num Events number.
     */
    virtual void OnEvent(const CacheEntryEvent<K, V>* evts, uint32_t num)
    {
        for (uint32_t i = 0; i < num; ++i)
        {
            std::cout << "Queried entry [key=" << (evts[i].HasValue() ? evts[i].GetKey() : K())
                      << ", val=" << (evts[i].HasValue() ? evts[i].GetValue() : V()) << ']'
                      << std::endl;
        }
    }
};

int main()
{
    IgniteConfiguration cfg;

    cfg.springCfgPath = "platforms/cpp/examples/continuous-query-example/config/continuous-query-example.xml";

    try
    {
        // Start a node.
        Ignite ignite = Ignition::Start(cfg);

        std::cout << std::endl;
        std::cout << ">>> Cache continuous query example started." << std::endl;
        std::cout << std::endl;

        // Get cache instance.
        Cache<int32_t, std::string> cache = ignite.GetOrCreateCache<int32_t, std::string>(CACHE_NAME);

        cache.Clear();

        const int32_t keyCnt = 20;

        for (int32_t i = 0; i < keyCnt; ++i)
        {
            std::stringstream builder;

            builder << i;

            cache.Put(i, builder.str());
        }

        // Declaring listener.
        Listener<int, std::string> listener;

        // Declaring continuous query.
        continuous::ContinuousQuery<int, std::string> qry(MakeReference(listener));

        {
            // Continous query scope. Query is closed when scope is left.
            continuous::ContinuousQueryHandle<int, std::string> handle = cache.QueryContinuous(qry);

            // Add a few more keys and watch more query notifications.
            for (int32_t i = keyCnt; i < keyCnt + 5; ++i)
            {
                std::stringstream builder;

                builder << i;

                cache.Put(i, builder.str());
            }

            // Let user wait while callback is notified about remaining puts.
            std::cout << std::endl;
            std::cout << ">>> Press 'Enter' to continue..." << std::endl;
            std::cout << std::endl;

            std::cin.get();
        }

        // Stop node.
        Ignition::StopAll(false);
    }
    catch (IgniteError& err)
    {
        std::cout << "An error occurred: " << err.GetText() << std::endl;
    }

    std::cout << std::endl;
    std::cout << ">>> Example finished, press 'Enter' to exit ..." << std::endl;
    std::cout << std::endl;

    std::cin.get();

    return 0;
}
