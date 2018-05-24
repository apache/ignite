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

#include "ignite/ignite.h"
#include "ignite/ignition.h"

#include "ignite/examples/organization.h"
#include "ignite/examples/person.h"

using namespace ignite;
using namespace cache;
using namespace query;

using namespace examples;

/** Organizations cache name. */
const char* ORG_CACHE = "Organization";

/** Organizations type name. */
const char* ORG_TYPE = "Organization";

/** Persons cache name. */
const char* PERSON_CACHE = "Person";

/** Persons type name. */
const char* PERSON_TYPE = "Person";

/**
 * Example for SQL queries based on all employees working for a specific
 * organization (query uses distributed join).
 */
void DoSqlQueryWithDistributedJoin()
{
    typedef std::vector< CacheEntry<int64_t, Person> > ResVector;

    Cache<int64_t, Person> cache = Ignition::Get().GetCache<int64_t, Person>(PERSON_CACHE);

    // SQL clause query which joins on 2 types to select people for a specific organization.
    std::string joinSql(
        "from Person, \"Organization\".Organization as org "
        "where Person.orgId = org._key "
        "and lower(org.name) = lower(?)");

    SqlQuery qry("Person", joinSql);

    qry.AddArgument<std::string>("ApacheIgnite");

    // Enable distributed joins for query.
    qry.SetDistributedJoins(true);

    // Execute queries for find employees for different organizations.
    ResVector igniters;
    cache.Query(qry).GetAll(igniters);

    // Printing first result set.
    std::cout << "Following people are 'ApacheIgnite' employees (distributed join): " << std::endl;

    for (ResVector::const_iterator i = igniters.begin(); i != igniters.end(); ++i)
        std::cout << i->GetKey() << " : " << i->GetValue().ToString() << std::endl;

    std::cout << std::endl;

    qry.ClearArguments();

    qry.AddArgument<std::string>("Other");

    ResVector others;
    cache.Query(qry).GetAll(others);

    // Printing second result set.
    std::cout << "Following people are 'Other' employees (distributed join): " << std::endl;

    for (ResVector::const_iterator i = others.begin(); i != others.end(); ++i)
        std::cout << i->GetKey() << " : " << i->GetValue().ToString() << std::endl;

    std::cout << std::endl;
}

/**
 * Example for SQL-based fields queries that return only required
 * fields instead of whole key-value pairs.
 *
 * Note that SQL Fields Query can only be performed using fields that have been
 * listed in "QueryEntity" been of the config.
 */
void DoSqlFieldsQueryWithJoin()
{
    Cache<int64_t, Person> cache = Ignition::Get().GetCache<int64_t, Person>(PERSON_CACHE);

    // Execute query to get names of all employees.
    std::string sql(
        "select concat(firstName, ' ', lastName), org.name "
        "from Person, \"Organization\".Organization as org "
        "where Person.orgId = org._key");

    QueryFieldsCursor cursor = cache.Query(SqlFieldsQuery(sql));

    // Print persons' names and organizations' names.
    std::cout << "Names of all employees and organizations they belong to: " << std::endl;

    // In this particular case each row will have two elements with full name
    // of an employees and their organization.
    while (cursor.HasNext())
    {
        QueryFieldsRow row = cursor.GetNext();

        std::cout << row.GetNext<std::string>() << ", ";
        std::cout << row.GetNext<std::string>() << std::endl;
    }

    std::cout << std::endl;
}

/**
 * Example for SQL-based fields queries that return only required fields instead
 * of whole key-value pairs.
 *
 * Note that SQL Fields Query can only be performed using fields that have been
 * listed in "QueryEntity" been of the config.
 */
void DoSqlFieldsQuery()
{
    Cache<int64_t, Person> cache = Ignition::Get().GetCache<int64_t, Person>(PERSON_CACHE);

    // Execute query to get names of all employees.
    QueryFieldsCursor cursor = cache.Query(SqlFieldsQuery(
        "select concat(firstName, ' ', lastName) from Person"));

    // Print names.
    std::cout << "Names of all employees: " << std::endl;

    // In this particular case each row will have one element with full name of an employees.
    while (cursor.HasNext())
        std::cout << cursor.GetNext().GetNext<std::string>() << std::endl;

    std::cout << std::endl;
}

/**
 * Example for SQL queries to calculate average salary for a specific organization.
 *
 * Note that SQL Fields Query can only be performed using fields that have been
 * listed in "QueryEntity" been of the config.
 */
void DoSqlQueryWithAggregation()
{
    Cache<int64_t, Person> cache = Ignition::Get().GetCache<int64_t, Person>(PERSON_CACHE);

    // Calculate average of salary of all persons in ApacheIgnite.
    // Note that we also join on Organization cache as well.
    std::string sql(
        "select avg(salary) "
        "from Person, \"Organization\".Organization as org "
        "where Person.orgId = org._key "
        "and lower(org.name) = lower(?)");

    SqlFieldsQuery qry(sql);

    qry.AddArgument<std::string>("ApacheIgnite");

    QueryFieldsCursor cursor = cache.Query(qry);

    // Calculate average salary for a specific organization.
    std::cout << "Average salary for 'ApacheIgnite' employees: " << std::endl;

    while (cursor.HasNext())
        std::cout << cursor.GetNext().GetNext<double>() << std::endl;

    std::cout << std::endl;
}

/**
 * Example for TEXT queries using LUCENE-based indexing of people's resumes.
 *
 * Note that to be able to do so you have to add FULLTEXT index for the 'resume'
 * field of the Person type. See config for details.
 */
void DoTextQuery()
{
    Cache<int64_t, Person> cache = Ignition::Get().GetCache<int64_t, Person>(PERSON_CACHE);

    typedef std::vector< CacheEntry<int64_t, Person> > ResVector;

    //  Query for all people with "Master" in their resumes.
    ResVector masters;
    cache.Query(TextQuery(PERSON_TYPE, "Master")).GetAll(masters);

    // Query for all people with "Bachelor" in their resumes.
    ResVector bachelors;
    cache.Query(TextQuery(PERSON_TYPE, "Bachelor")).GetAll(bachelors);

    std::cout << "Following people have 'Master' in their resumes: " << std::endl;

    // Printing first result set.
    for (ResVector::const_iterator i = masters.begin(); i != masters.end(); ++i)
        std::cout << i->GetKey() << " : " << i->GetValue().ToString() << std::endl;

    std::cout << std::endl;

    std::cout << "Following people have 'Bachelor' in their resumes: " << std::endl;

    // Printing second result set.
    for (ResVector::const_iterator i = bachelors.begin(); i != bachelors.end(); ++i)
        std::cout << i->GetKey() << " : " << i->GetValue().ToString() << std::endl;

    std::cout << std::endl;
}

/**
 * Example for SQL queries based on all employees working for a specific organization.
 *
 * Note that SQL Query can only be performed using fields that have been
 * listed in "QueryEntity" been of the config.
 */
void DoSqlQueryWithJoin()
{
    Cache<int64_t, Person> cache = Ignition::Get().GetCache<int64_t, Person>(PERSON_CACHE);

    typedef std::vector< CacheEntry<int64_t, Person> > ResVector;

    // SQL clause query which joins on 2 types to select people for a specific organization.
    std::string sql(
        "from Person, \"Organization\".Organization as org "
        "where Person.orgId = org._key "
        "and lower(org.name) = lower(?)");

    SqlQuery qry(PERSON_TYPE, sql);

    // Execute queries for find employees for different organizations.
    std::cout << "Following people are 'ApacheIgnite' employees: " << std::endl;

    qry.AddArgument<std::string>("ApacheIgnite");

    ResVector res;
    cache.Query(qry).GetAll(res);

    for (ResVector::const_iterator i = res.begin(); i != res.end(); ++i)
        std::cout << i->GetKey() << " : " << i->GetValue().ToString() << std::endl;

    std::cout << std::endl;

    std::cout << "Following people are 'Other' employees: " << std::endl;

    qry.ClearArguments();

    qry.AddArgument<std::string>("Other");

    res.clear();
    cache.Query(qry).GetAll(res);

    for (ResVector::const_iterator i = res.begin(); i != res.end(); ++i)
        std::cout << i->GetKey() << " : " << i->GetValue().ToString() << std::endl;

    std::cout << std::endl;
}

/**
 * Example for SQL queries based on salary ranges.
 *
 * Note that SQL Query can only be performed using fields that have been
 * listed in "QueryEntity" been of the config.
 */
void DoSqlQuery()
{
    Cache<int64_t, Person> cache = Ignition::Get().GetCache<int64_t, Person>(PERSON_CACHE);

    // SQL clause which selects salaries based on range.
    std::string sql("salary > ? and salary <= ?");

    SqlQuery qry(PERSON_TYPE, sql);

    typedef std::vector< CacheEntry<int64_t, Person> > ResVector;

    // Execute queries for salary range 0 - 1000.
    std::cout << "People with salaries between 0 and 1000 (queried with SQL query): " << std::endl;

    qry.AddArgument(0);
    qry.AddArgument(1000);

    ResVector res;
    cache.Query(qry).GetAll(res);

    for (ResVector::const_iterator i = res.begin(); i != res.end(); ++i)
            std::cout << i->GetKey() << " : " << i->GetValue().ToString() << std::endl;

    std::cout << std::endl;

    qry.ClearArguments();

    // Execute queries for salary range 1000 - 2000.
    std::cout << "People with salaries between 1000 and 2000 (queried with SQL query): " << std::endl;

    qry.AddArgument(1000);
    qry.AddArgument(2000);

    res.clear();

    cache.Query(qry).GetAll(res);

    for (ResVector::const_iterator i = res.begin(); i != res.end(); ++i)
        std::cout << i->GetKey() << " : " << i->GetValue().ToString() << std::endl;

    std::cout << std::endl;
}

/**
 * Example for scan query based on a predicate.
 */
void DoScanQuery()
{
    Cache<int64_t, Person> cache = Ignition::Get().GetCache<int64_t, Person>(PERSON_CACHE);

    ScanQuery scan;

    typedef std::vector< CacheEntry<int64_t, Person> > ResVector;

    ResVector res;
    cache.Query(scan).GetAll(res);

    // Execute queries for salary ranges.
    std::cout << "People with salaries between 0 and 1000 (queried with SCAN query): " << std::endl;

    for (ResVector::const_iterator i = res.begin(); i != res.end(); ++i)
    {
        Person person(i->GetValue());

        if (person.salary <= 1000)
            std::cout << i->GetKey() << " : " << person.ToString() << std::endl;
    }

    std::cout << std::endl;
}

/**
 * Populate cache with test data.
 */
void Initialize()
{
    Cache<int64_t, Organization> orgCache =
        Ignition::Get().GetCache<int64_t, Organization>(ORG_CACHE);

    // Clear cache before running the example.
    orgCache.Clear();

    // Organizations.
    Organization org1("ApacheIgnite");
    Organization org2("Other");

    const int64_t org1Id = 1;
    const int64_t org2Id = 2;

    orgCache.Put(org1Id, org1);
    orgCache.Put(org2Id, org2);

    Cache<int64_t, Person> personCache =
        Ignition::Get().GetCache<int64_t, Person>(PERSON_CACHE);

    // Clear cache before running the example.
    personCache.Clear();

    // People.
    Person p1(org1Id, "John", "Doe", "John Doe has Master Degree.", 2000);
    Person p2(org1Id, "Jane", "Doe", "Jane Doe has Bachelor Degree.", 1000);
    Person p3(org2Id, "John", "Smith", "John Smith has Bachelor Degree.", 1000);
    Person p4(org2Id, "Jane", "Smith", "Jane Smith has Master Degree.", 2000);

    // Note that in this example we use custom affinity key for Person objects
    // to ensure that all persons are collocated with their organizations.
    personCache.Put(1, p1);
    personCache.Put(2, p2);
    personCache.Put(3, p3);
    personCache.Put(4, p4);
}

int main()
{
    IgniteConfiguration cfg;

    cfg.springCfgPath = "platforms/cpp/examples/query-example/config/query-example.xml";

    try
    {
        // Start a node.
        Ignite ignite = Ignition::Start(cfg);

        std::cout << std::endl;
        std::cout << ">>> Cache query example started." << std::endl;
        std::cout << std::endl;

        // Get organization cache instance.
        Cache<int64_t, Organization> orgCache = ignite.GetCache<int64_t, Organization>(ORG_CACHE);

        // Get person cache instance.
        Cache<int64_t, Person> personCache = ignite.GetCache<int64_t, Person>(PERSON_CACHE);

        // Populate cache.
        Initialize();

        // Example for SCAN-based query based on a predicate.
        DoScanQuery();

        // Example for SQL-based querying employees based on salary ranges.
        DoSqlQuery();

        // Example for SQL-based querying employees for a given organization (includes SQL join).
        DoSqlQueryWithJoin();

        // Example for SQL-based querying employees for a given organization (includes distributed SQL join).
        DoSqlQueryWithDistributedJoin();

        // Example for TEXT-based querying for a given string in peoples resumes.
        DoTextQuery();

        // Example for SQL-based querying to calculate average salary among all employees within a company.
        DoSqlQueryWithAggregation();

        // Example for SQL-based fields queries that return only required
        // fields instead of whole key-value pairs.
        DoSqlFieldsQuery();

        // Example for SQL-based fields queries that uses joins.
        DoSqlFieldsQueryWithJoin();

        // Stop node.
        Ignition::StopAll(false);
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
