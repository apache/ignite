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
//tag::thin-partition-awareness[]
#include <ignite/thin/ignite_client.h>
#include <ignite/thin/ignite_client_configuration.h>

using namespace ignite::thin;

void TestClientPartitionAwareness()
{
    IgniteClientConfiguration cfg;
    cfg.SetEndPoints("127.0.0.1:10800,217.29.2.1:10800,200.10.33.1:10800");
    cfg.SetPartitionAwareness(true);

    IgniteClient client = IgniteClient::Start(cfg);

    cache::CacheClient<int32_t, std::string> cacheClient =
        client.GetOrCreateCache<int32_t, std::string>("TestCache");

    cacheClient.Put(42, "Hello Ignite Partition Awareness!");

    cacheClient.RefreshAffinityMapping();

    // Getting a value
    std::string val = cacheClient.Get(42);
}
//end::thin-partition-awareness[]

int main()
{
    TestClientPartitionAwareness();
}
