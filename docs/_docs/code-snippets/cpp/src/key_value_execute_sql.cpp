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
#include "country.h";

using namespace ignite;
using namespace cache;
using namespace query;

const char* CITY_CACHE_NAME = "City";
const char* COUNTRY_CACHE_NAME = "Country";
const char* COUNTRY_LANGUAGE_CACHE_NAME = "CountryLanguage";

int main()
{
    //tag::key-value-execute-sql[]
    IgniteConfiguration cfg;
    cfg.springCfgPath = "config/sql.xml";

    Ignite ignite = Ignition::Start(cfg);

    Cache<int64_t, std::string> cityCache = ignite.GetOrCreateCache<int64_t, std::string>(CITY_CACHE_NAME);
    Cache<int64_t, Country> countryCache = ignite.GetOrCreateCache<int64_t, Country>(COUNTRY_CACHE_NAME);
    Cache<int64_t, std::string> languageCache = ignite.GetOrCreateCache<int64_t, std::string>(COUNTRY_LANGUAGE_CACHE_NAME);

    // SQL Fields Query can only be performed using fields that have been listed in "QueryEntity" been of the config!
    SqlFieldsQuery query = SqlFieldsQuery("SELECT name, population FROM country ORDER BY population DESC LIMIT 10");

    QueryFieldsCursor cursor = countryCache.Query(query);
    while (cursor.HasNext())
    {
        QueryFieldsRow row = cursor.GetNext();
        std::string name = row.GetNext<std::string>();
        std::string population = row.GetNext<std::string>();
        std::cout << "    >>> " << population << " people live in " << name << std::endl;
    }
    //end::key-value-execute-sql[]
}
