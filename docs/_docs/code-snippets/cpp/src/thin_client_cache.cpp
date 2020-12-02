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
#include <ignite/thin/ignite_client.h>
#include <ignite/thin/ignite_client_configuration.h>

using namespace ignite::thin;

int main()
{
    IgniteClientConfiguration cfg;
    cfg.SetEndPoints("127.0.0.1:10800");

    IgniteClient client = IgniteClient::Start(cfg);

    //tag::thin-getting-cache-instance[]
    cache::CacheClient<int32_t, std::string> cache =
        client.GetOrCreateCache<int32_t, std::string>("TestCache");
    //end::thin-getting-cache-instance[]

    //tag::basic-cache-operations[]
    std::map<int, std::string> vals;
    for (int i = 1; i < 100; i++)
    {
        vals[i] = i;
    }

    cache.PutAll(vals);
    cache.Replace(1, "2");
    cache.Put(101, "101");
    cache.RemoveAll();
    //end::basic-cache-operations[]
}
