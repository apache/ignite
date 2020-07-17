#include <iostream>

#include "ignite/ignite.h"
#include "ignite/ignition.h"

using namespace ignite;
using namespace cache;

int main()
{
    //tag::setting-work-directory[]
    IgniteConfiguration cfg;

    cfg.igniteHome = "/path/to/work/directory";
    //end::setting-work-directory[]
}