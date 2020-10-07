#include <iostream>

#include "ignite/ignite.h"
#include "ignite/ignition.h"

using namespace ignite;
using namespace cache;

int main()
{
    //tag::start-all-nodes[]
    IgniteConfiguration cfg;
    cfg.springCfgPath = "/path/to/configuration.xml";

    Ignite ignite = Ignition::Start(cfg);
    //end::start-all-nodes[]

    //tag::activate-cluster[]
    ignite.SetActive(true);
    //end::activate-cluster[]

    //tag::deactivate-cluster[]
    ignite.SetActive(false);
    //end::deactivate-cluster[]

    //tag::stop-node[]
    Ignition::Stop(ignite.GetName(), false);
    //end::stop-node[]
}