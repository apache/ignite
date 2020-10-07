#include <iostream>
#include <string>

#include "ignite/ignite.h"
#include "ignite/ignition.h"

using namespace ignite;
using namespace cache;

int main()
{
    //tag::cache-getting-instance[]
    IgniteConfiguration cfg;
    cfg.springCfgPath = "/path/to/configuration.xml";

    Ignite ignite = Ignition::Start(cfg);

    // Obtain instance of cache named "myCache".
    // Note that different caches may have different generics.
    Cache<int32_t, std::string> cache = ignite.GetCache<int32_t, std::string>("myCache");
    //end::cache-getting-instance[]
}