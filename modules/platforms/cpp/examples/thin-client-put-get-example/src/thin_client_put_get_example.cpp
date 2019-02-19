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

#include <ignite/thin/ignite_client.h>
#include <ignite/thin/cache/cache_client.h>

#include "ignite/examples/organization.h"

using namespace ignite;
using namespace thin;
using namespace cache;

using namespace examples;

/*
 * Execute individual Put and Get operations.
 *
 * @param cache Cache instance.
 */
void PutGet(CacheClient<int32_t, Organization>& cache) 
{
    // Create new Organization to store in cache.
    Organization org("Microsoft", Address("1096 Eddy Street, San Francisco, CA", 94109));

    // Put organization to cache.
    cache.Put(1, org);

    // Get recently created employee as a strongly-typed fully de-serialized instance.
    Organization orgFromCache = cache.Get(1);

    std::cout <<  ">>> Retrieved organization instance from cache: " << std::endl;
    std::cout << orgFromCache.ToString() << std::endl;
    std::cout << std::endl;
}

/*
 * Execute bulk Put and Get operations.
 *
 * @param cache Cache instance.
 */
void PutGetAll(CacheClient<int32_t, Organization>& cache)
{
    // Create new Organizations to store in cache.
    Organization org1("Microsoft", Address("1096 Eddy Street, San Francisco, CA", 94109));
    Organization org2("Red Cross", Address("184 Fidler Drive, San Antonio, TX", 78205));

    // Put created data entries to cache.
    std::map<int, Organization> vals;

    vals[1] = org1;
    vals[2] = org2;

    cache.PutAll(vals);

    // Get recently created organizations as a strongly-typed fully de-serialized instances.
    std::set<int> keys;

    keys.insert(1);
    keys.insert(2);

    std::map<int, Organization> valsFromCache;
    cache.GetAll(keys, valsFromCache);

    std::cout <<  ">>> Retrieved organization instances from cache: " << std::endl;

    for (std::map<int, Organization>::iterator it = valsFromCache.begin(); it != valsFromCache.end(); ++it)
        std::cout <<  it->second.ToString() << std::endl;

    std::cout << std::endl;
}

int main()
{
    IgniteClientConfiguration cfg;

    cfg.SetEndPoints("127.0.0.1");

    try
    {
        // Start a client.
        IgniteClient client = IgniteClient::Start(cfg);

        std::cout << std::endl;
        std::cout << ">>> Cache put-get example started." << std::endl;
        std::cout << std::endl;

        // Get cache instance.
        CacheClient<int32_t, Organization> cache = client.GetOrCreateCache<int32_t, Organization>("PutGetExample");

        // Clear cache.
        cache.Clear();

        PutGet(cache);
        PutGetAll(cache);
    }
    catch (IgniteError& err)
    {
        std::cout << "An error occurred: " << err.GetText() << std::endl;

        return err.GetCode();
    }

    std::cout << std::endl;
    std::cout << ">>> Example finished, press 'Enter' to exit ..." << std::endl;
    std::cout << std::endl;

    std::cin.get();

    return 0;
}