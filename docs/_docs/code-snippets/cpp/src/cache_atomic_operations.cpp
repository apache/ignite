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

int main()
{
    //tag::cache-atomic-operations[]
    IgniteConfiguration cfg;
    cfg.springCfgPath = "/path/to/configuration.xml";

    Ignite ignite = Ignition::Start(cfg);

    Cache<std::string, int32_t> cache = ignite.GetOrCreateCache<std::string, int32_t>("myNewCache");

    // Put-if-absent which returns previous value.
    int32_t oldVal = cache.GetAndPutIfAbsent("Hello", 11);

    // Put-if-absent which returns boolean success flag.
    boolean success = cache.PutIfAbsent("World", 22);

    // Replace-if-exists operation (opposite of getAndPutIfAbsent), returns previous value.
    oldVal = cache.GetAndReplace("Hello", 11);

    // Replace-if-exists operation (opposite of putIfAbsent), returns boolean success flag.
    success = cache.Replace("World", 22);

    // Replace-if-matches operation.
    success = cache.Replace("World", 2, 22);

    // Remove-if-matches operation.
    success = cache.Remove("Hello", 1);
    //end::cache-atomic-operations[]
}
