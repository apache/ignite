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
 * Example for SQL fields queries based on all employees working for a specific
 * organization (query uses distributed join).
 *
 * Note that SQL Fields Query can only be performed using fields that have been
 * listed in "QueryEntity" been of the config.
 */
void DoSqlQueryWithDistributedJoin()
{
    Cache<PersonKey, Person> cache = Ignition::Get().GetCache<PersonKey, Person>(PERSON_CACHE);

    // SQL clause query which joins on 2 types to select people for a specific organization.
    SqlFieldsQuery qry(
        "select firstName, lastName from Person "
        "inner join \"Organization\".Organization as org "
        "where Person.orgId = org._key "
        "and lower(org.name) = lower(?)");

    qry.AddArgument<std::string>("ApacheIgnite");

    // Enable distributed joins for query.
    qry.SetDistributedJoins(true);

    QueryFieldsCursor cursor = cache.Query(qry);

    // Printing first result set.
    std::cout << "Following people are 'ApacheIgnite' employees (distributed join): " << std::endl;

    while (cursor.HasNext())
    {
        QueryFieldsRow row = cursor.GetNext();

        std::string firstName = row.GetNext<std::string>();
        std::string lastName = row.GetNext<std::string>();

        std::cout << firstName << " " << lastName << std::endl;
    }

    std::cout << std::endl;

    qry.ClearArguments();

    qry.AddArgument<std::string>("Other");

    cursor = cache.Query(qry);

    // Printing second result set.
    std::cout << "Following people are 'Other' employees (distributed join): " << std::endl;

    while (cursor.HasNext())
    {
        QueryFieldsRow row = cursor.GetNext();

        std::string firstName = row.GetNext<std::string>();
        std::string lastName = row.GetNext<std::string>();

        std::cout << firstName << " " << lastName << std::endl;
    }

    std::cout << std::endl;
}

/**
 * Example for SQL-based fields queries that return only person name and company it works in.
 *
 * Note that SQL Fields Query can only be performed using fields that have been
 * listed in "QueryEntity" been of the config.
 */
void DoSqlQueryWithJoin()
{
    Cache<PersonKey, Person> cache = Ignition::Get().GetCache<PersonKey, Person>(PERSON_CACHE);

    // Execute query to get names of all employees.
    std::string sql(
        "select concat(firstName, ' ', lastName), org.name "
        "from Person inner join \"Organization\".Organization as org "
        "on Person.orgId = org._key");

    QueryFieldsCursor cursor = cache.Query(SqlFieldsQuery(sql));

    // Print persons' names and organizations' names.
    std::cout << "Names of all employees and organizations they belong to: " << std::endl;

    // In this particular case each row will have two elements with full name
    // of an employees and their organization.
    while (cursor.HasNext())
    {
        QueryFieldsRow row = cursor.GetNext();

        std::string person = row.GetNext<std::string>();
        std::string organization = row.GetNext<std::string>();

        std::cout << person << " is working in " << organization << std::endl;
    }

    std::cout << std::endl;
}

/**
 * Example for SQL-based fields queries that uses SQL functions.
 *
 * Note that SQL Fields Query can only be performed using fields that have been
 * listed in "QueryEntity" been of the config.
 */
void DoSqlQueryWithFunction()
{
    Cache<PersonKey, Person> cache = Ignition::Get().GetCache<PersonKey, Person>(PERSON_CACHE);

    // Execute query to get names of all employees.
    QueryFieldsCursor cursor = cache.Query(SqlFieldsQuery(
        "select concat(firstName, ' ', lastName) from Person"));

    // Print names.
    std::cout << "Names of all employees: " << std::endl;

    // In this particular case each row will have one element with full name of an employees.
    while (cursor.HasNext())
    {
        std::string person = cursor.GetNext().GetNext<std::string>();

        std::cout << person << std::endl;
    }

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
    Cache<PersonKey, Person> cache = Ignition::Get().GetCache<PersonKey, Person>(PERSON_CACHE);

    // Calculate average of salary of all persons in ApacheIgnite.
    // Note that we also join on Organization cache as well.
    SqlFieldsQuery qry(
        "select avg(salary) "
        "from Person, \"Organization\".Organization as org "
        "where Person.orgId = org._key "
        "and lower(org.name) = lower(?)");

    qry.AddArgument<std::string>("ApacheIgnite");

    QueryFieldsCursor cursor = cache.Query(qry);

    // Calculate average salary for a specific organization.
    std::cout << "Average salary for 'ApacheIgnite' employees: " << std::endl;

    while (cursor.HasNext())
    {
        double salary = cursor.GetNext().GetNext<double>();

        std::cout << salary << std::endl;
    }

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
    Cache<PersonKey, Person> cache = Ignition::Get().GetCache<PersonKey, Person>(PERSON_CACHE);

    // SQL clause which selects salaries based on range.
    SqlFieldsQuery qry(
        "select firstName, lastName, salary "
        "from Person "
        "where salary > ? and salary <= ?");

    // Execute queries for salary range 0 - 1000.
    std::cout << "People with salaries between 0 and 1000 (queried with SQL query): " << std::endl;

    qry.AddArgument(0);
    qry.AddArgument(1000);

    QueryFieldsCursor cursor = cache.Query(qry);

    while (cursor.HasNext())
    {
        QueryFieldsRow row = cursor.GetNext();

        std::string firstName = row.GetNext<std::string>();
        std::string lastName = row.GetNext<std::string>();
        double salary = row.GetNext<double>();

        std::cout << firstName << " " << lastName << " : " << salary << std::endl;
    }

    std::cout << std::endl;

    qry.ClearArguments();

    // Execute queries for salary range 1000 - 2000.
    std::cout << "People with salaries between 1000 and 2000 (queried with SQL query): " << std::endl;

    qry.AddArgument(1000);
    qry.AddArgument(2000);

    cursor = cache.Query(qry);

    while (cursor.HasNext())
    {
        QueryFieldsRow row = cursor.GetNext();

        std::string firstName = row.GetNext<std::string>();
        std::string lastName = row.GetNext<std::string>();
        double salary = row.GetNext<double>();

        std::cout << firstName << " " << lastName << " : " << salary << std::endl;
    }

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
    Cache<PersonKey, Person> cache = Ignition::Get().GetCache<PersonKey, Person>(PERSON_CACHE);

    typedef std::vector< CacheEntry<PersonKey, Person> > ResVector;

    //  Query for all people with "Master" in their resumes.
    ResVector masters;
    cache.Query(TextQuery(PERSON_TYPE, "Master")).GetAll(masters);

    // Query for all people with "Bachelor" in their resumes.
    ResVector bachelors;
    cache.Query(TextQuery(PERSON_TYPE, "Bachelor")).GetAll(bachelors);

    std::cout << "Following people have 'Master' in their resumes: " << std::endl;

    // Printing first result set.
    for (ResVector::const_iterator i = masters.begin(); i != masters.end(); ++i)
        std::cout << i->GetKey().ToString() << " : " << i->GetValue().ToString() << std::endl;

    std::cout << std::endl;

    std::cout << "Following people have 'Bachelor' in their resumes: " << std::endl;

    // Printing second result set.
    for (ResVector::const_iterator i = bachelors.begin(); i != bachelors.end(); ++i)
        std::cout << i->GetKey().ToString() << " : " << i->GetValue().ToString() << std::endl;

    std::cout << std::endl;
}

/**
 * Example for scan query based on a predicate.
 */
void DoScanQuery()
{
    Cache<PersonKey, Person> cache = Ignition::Get().GetCache<PersonKey, Person>(PERSON_CACHE);

    ScanQuery scan;

    typedef std::vector< CacheEntry<PersonKey, Person> > ResVector;

    ResVector res;
    cache.Query(scan).GetAll(res);

    // Execute queries for salary ranges.
    std::cout << "People with salaries between 0 and 1000 (queried with SCAN query): " << std::endl;

    for (ResVector::const_iterator i = res.begin(); i != res.end(); ++i)
    {
        Person person(i->GetValue());

        if (person.salary <= 1000)
            std::cout << i->GetKey().ToString() << " : " << person.ToString() << std::endl;
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

    Cache<PersonKey, Person> personCache =
        Ignition::Get().GetCache<PersonKey, Person>(PERSON_CACHE);

    // Clear cache before running the example.
    personCache.Clear();

    // People.

    // Collocated by 1st organisation:
    Person p1(org1Id, "John", "Doe", "John Doe has Master Degree.", 2000);
    Person p2(org1Id, "Jane", "Doe", "Jane Doe has Bachelor Degree.", 1000);

    PersonKey pKey1 (1, org1Id);
    PersonKey pKey2 (2, org1Id);

    // Collocated by second organisation:
    Person p3(org2Id, "John", "Smith", "John Smith has Bachelor Degree.", 1000);
    Person p4(org2Id, "Jane", "Smith", "Jane Smith has Master Degree.", 2000);

    PersonKey pKey3 (3, org2Id);
    PersonKey pKey4 (4, org2Id);

    // Note that in this example we use custom affinity key for Person objects
    // to ensure that all persons are collocated with their organizations.
    personCache.Put(pKey1, p1);
    personCache.Put(pKey2, p2);
    personCache.Put(pKey3, p3);
    personCache.Put(pKey4, p4);
}

int main()
{
    try
    {
        IgniteConfiguration cfg;
        cfg.springCfgPath = "platforms/cpp/examples/query-example/config/query-example.xml";

        // Start a node.
        Ignite ignite = Ignition::Start(cfg);

        std::cout << std::endl;
        std::cout << ">>> Cache query example started." << std::endl;
        std::cout << std::endl;

        // Get organization cache instance.
        Cache<int64_t, Organization> orgCache = ignite.GetCache<int64_t, Organization>(ORG_CACHE);

        // Get person cache instance.
        Cache<PersonKey, Person> personCache = ignite.GetCache<PersonKey, Person>(PERSON_CACHE);

        // Populate cache.
        Initialize();

        // Example for SCAN-based query based on a predicate.
        DoScanQuery();

        // Example for TEXT-based querying for a given string in peoples resumes.
        DoTextQuery();

        // Example for SQL-based querying employees based on salary ranges.
        DoSqlQuery();

        // Example for SQL-based querying employees for a given organization (includes SQL join).
        DoSqlQueryWithJoin();

        // Example for SQL-based querying employees for a given organization (includes distributed SQL join).
        DoSqlQueryWithDistributedJoin();

        // Example for SQL-based fields queries that uses SQL functions.
        DoSqlQueryWithFunction();

        // Example for SQL-based querying to calculate average salary among all employees within a company.
        DoSqlQueryWithAggregation();

        // Stop node.
        Ignition::StopAll(false);

        std::cout << std::endl;
        std::cout << ">>> Example finished, press 'Enter' to exit ..." << std::endl;
        std::cout << std::endl;

        std::cin.get();
    }
    catch (IgniteError& err)
    {
        std::cout << "An error occurred: " << err.GetText() << std::endl;

        return err.GetCode();
    }

    return 0;
}
