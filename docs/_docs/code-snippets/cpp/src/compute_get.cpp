#include <iostream>

#include "ignite/ignite.h"
#include "ignite/ignition.h"

using namespace ignite;
using namespace cache;
using namespace query;

const char* CONFIG_DEFAULT = "/path/to/configuration.xml";

int main()
{
    IgniteConfiguration cfg;
    cfg.springCfgPath = CONFIG_DEFAULT;

    //tag::compute-get[]
    Ignite ignite = Ignition::Start(cfg);

    Compute compute = ignite.GetCompute();
    //end::compute-get[]
}