#include <iostream>
#include <string>

#include "ignite/ignite.h"
#include "ignite/ignition.h"

using namespace ignite;
using namespace cache;

int main()
{
    //tag::cache-creating-dynamically[]
    IgniteConfiguration cfg;
    cfg.springCfgPath = "/path/to/configuration.xml";

    Ignite ignite = Ignition::Start(cfg);

    // Create a cache with the given name, if it does not exist.
    Cache<int32_t, std::string> cache = ignite.GetOrCreateCache<int32_t, std::string>("myNewCache");
    //end::cache-creating-dynamically[]
}