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