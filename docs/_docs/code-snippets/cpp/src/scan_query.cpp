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

    Ignite grid = Ignition::Start(cfg);

    //tag::query-cursor[]
    Cache<int64_t, Person> cache = ignite.GetOrCreateCache<int64_t, ignite::Person>("personCache");

    QueryCursor<int64_t, Person> cursor = cache.Query(ScanQuery());
    //end::query-cursor[]

    // Iterate over results.
    while (cursor.HasNext())
    {
        std::cout << cursor.GetNext().GetKey() << std::endl;
    }

    //tag::set-local[]
    ScanQuery sq;
    sq.SetLocal(true);

    QueryCursor<int64_t, Person> cursor = cache.Query(sq);
    //end::set-local[]

}