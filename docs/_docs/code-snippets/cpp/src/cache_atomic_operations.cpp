#include <iostream>
#include <string>

#include "ignite/ignite.h"
#include "ignite/ignition.h"

using namespace ignite;
using namespace cache;

int main()
{
    //tag::cache-atomic-operations[]
    IgniteConfiguration cfg;
    cfg.springCfgPath = "/path/to/configuration.xml";

    Ignite ignite = Ignition::Start(cfg);

    Cache<std::string, int32_t> cache = ignite.GetOrCreateCache<std::string, int32_t>("myNewCache");

    // Put-if-absent which returns previous value.
    int32_t oldVal = cache.GetAndPutIfAbsent("Hello", 11);

    // Put-if-absent which returns boolean success flag.
    boolean success = cache.PutIfAbsent("World", 22);

    // Replace-if-exists operation (opposite of getAndPutIfAbsent), returns previous value.
    oldVal = cache.GetAndReplace("Hello", 11);

    // Replace-if-exists operation (opposite of putIfAbsent), returns boolean success flag.
    success = cache.Replace("World", 22);

    // Replace-if-matches operation.
    success = cache.Replace("World", 2, 22);

    // Remove-if-matches operation.
    success = cache.Remove("Hello", 1);
    //end::cache-atomic-operations[]
}