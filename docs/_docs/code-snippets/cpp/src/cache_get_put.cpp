#include <iostream>
#include <string>

#include "ignite/ignite.h"
#include "ignite/ignition.h"

using namespace ignite;
using namespace cache;

/** Cache name. */
const char* CACHE_NAME = "cacheName";

int main()
{
    //tag::cache-get-put[]
    IgniteConfiguration cfg;
    cfg.springCfgPath = "/path/to/configuration.xml";

    try
    {
        Ignite ignite = Ignition::Start(cfg);

        Cache<int32_t, std::string> cache = ignite.GetOrCreateCache<int32_t, std::string>(CACHE_NAME);

        // Store keys in the cache (the values will end up on different cache nodes).
        for (int32_t i = 0; i < 10; i++)
        {
            cache.Put(i, std::to_string(i));
        }

        for (int i = 0; i < 10; i++)
        {
            std::cout << "Got [key=" << i << ", val=" + cache.Get(i) << "]" << std::endl;
        }
    }
    catch (IgniteError& err)
    {
        std::cout << "An error occurred: " << err.GetText() << std::endl;
        return err.GetCode();
    }
    //end::cache-get-put[]
}