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
#include <string>

#include "ignite/ignite.h"
#include "ignite/ignition.h"

using namespace ignite;
using namespace cache;

/** Cache name. */
const char* CACHE_NAME = "cacheName";

int main()
{
    //tag::cache-get-put[]
    IgniteConfiguration cfg;
    cfg.springCfgPath = "/path/to/configuration.xml";

    try
    {
        Ignite ignite = Ignition::Start(cfg);

        Cache<int32_t, std::string> cache = ignite.GetOrCreateCache<int32_t, std::string>(CACHE_NAME);

        // Store keys in the cache (the values will end up on different cache nodes).
        for (int32_t i = 0; i < 10; i++)
        {
            cache.Put(i, std::to_string(i));
        }

        for (int i = 0; i < 10; i++)
        {
            std::cout << "Got [key=" << i << ", val=" + cache.Get(i) << "]" << std::endl;
        }
    }
    catch (IgniteError& err)
    {
        std::cout << "An error occurred: " << err.GetText() << std::endl;
        return err.GetCode();
    }
    //end::cache-get-put[]
}
