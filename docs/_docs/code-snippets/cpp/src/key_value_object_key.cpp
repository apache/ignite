#include <iostream>

#include "ignite/ignite.h"
#include "ignite/ignition.h"
#include "city.h"
#include "city_key.h"

using namespace ignite;
using namespace cache;
using namespace query;

int main()
{
    //tag::key-value-object-key[]
    IgniteConfiguration cfg;
    cfg.springCfgPath = "/path/to/configuration.xml";

    Ignite ignite = Ignition::Start(cfg);

    Cache<CityKey, City> cityCache = ignite.GetOrCreateCache<CityKey, City>("City");

    CityKey key = CityKey(5, "NLD");

    cityCache.Put(key, 100000);

    //getting the city by ID and country code
    City city = cityCache.Get(key);

    std::cout << ">> Updating Amsterdam record:" << std::endl;
    city.population = city.population - 10000;

    cityCache.Put(key, city);

    std::cout << cityCache.Get(key).ToString() << std::endl;
    //end::key-value-object-key[]
}