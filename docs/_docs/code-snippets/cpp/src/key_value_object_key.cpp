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
#include "city.h"
#include "city_key.h"

using namespace ignite;
using namespace cache;
using namespace query;

int main()
{
    //tag::key-value-object-key[]
    IgniteConfiguration cfg;
    cfg.springCfgPath = "/path/to/configuration.xml";

    Ignite ignite = Ignition::Start(cfg);

    Cache<CityKey, City> cityCache = ignite.GetOrCreateCache<CityKey, City>("City");

    CityKey key = CityKey(5, "NLD");

    cityCache.Put(key, 100000);

    //getting the city by ID and country code
    City city = cityCache.Get(key);

    std::cout << ">> Updating Amsterdam record:" << std::endl;
    city.population = city.population - 10000;

    cityCache.Put(key, city);

    std::cout << cityCache.Get(key).ToString() << std::endl;
    //end::key-value-object-key[]
}
