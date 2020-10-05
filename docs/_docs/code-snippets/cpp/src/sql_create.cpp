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
    cfg.springCfgPath = "/path/to/configuration.xml";

    Ignite ignite = Ignition::Start(cfg);

    //tag::sql-create[]
    Cache<int64_t, Person> cache = ignite.GetOrCreateCache<int64_t, Person>("Person");

    // Creating City table.
    cache.Query(SqlFieldsQuery("CREATE TABLE City (id int primary key, name varchar, region varchar)"));
    //end::sql-create[]
}