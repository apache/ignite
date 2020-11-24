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
#include "person.h"

using namespace ignite;
using namespace cache;
using namespace query;

int main()
{
    IgniteConfiguration cfg;
    cfg.springCfgPath = "config/sql.xml";

    Ignite ignite = Ignition::Start(cfg);

    //tag::sql-fields-query[]
    Cache<int64_t, Person> cache = ignite.GetOrCreateCache<int64_t, Person>("Person");

    // Iterate over the result set.
    // SQL Fields Query can only be performed using fields that have been listed in "QueryEntity" been of the config!
    QueryFieldsCursor cursor = cache.Query(SqlFieldsQuery("select concat(firstName, ' ', lastName) from Person"));
    while (cursor.HasNext())
    {
        std::cout << "personName=" << cursor.GetNext().GetNext<std::string>() << std::endl;
    }
    //end::sql-fields-query[]

    //tag::sql-fields-query-scheme[]
    // SQL Fields Query can only be performed using fields that have been listed in "QueryEntity" been of the config!
    SqlFieldsQuery sql = SqlFieldsQuery("select name from City");
    sql.SetSchema("PERSON");
    //end::sql-fields-query-scheme[]

    //tag::sql-fields-query-scheme-inline[]
    // SQL Fields Query can only be performed using fields that have been listed in "QueryEntity" been of the config!
    sql = SqlFieldsQuery("select name from Person.City");
    //end::sql-fields-query-scheme-inline[]
}
