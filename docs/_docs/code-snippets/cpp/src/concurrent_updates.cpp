#include <iostream>

#include "ignite/ignite.h"
#include "ignite/ignition.h"

using namespace ignite;
using namespace cache;
using namespace transactions;

int main()
{
    IgniteConfiguration cfg;
    cfg.springCfgPath = "/path/to/configuration.xml";

    Ignition::Start(cfg);

    Ignite ignite = Ignition::Get();

    Cache<std::int32_t, std::string> cache = ignite.GetOrCreateCache<std::int32_t, std::string>("myCache");

    //tag::concurrent-updates[]
    for (int i = 1; i <= 5; i++)
    {
        Transaction tx = ignite.GetTransactions().TxStart();
        std::cout << "attempt #" << i << ", value: " << cache.Get(1) << std::endl;
        try {
            cache.Put(1, "new value");
            tx.Commit();
            std::cout << "attempt #" << i << " succeeded" << std::endl;
            break;
        }
        catch (IgniteError e)
        {
            if (!tx.IsRollbackOnly())
            {
                // Transaction was not marked as "rollback only",
                // so it's not a concurrent update issue.
                // Process the exception here.
                break;
            }
        }
    }
    //end::concurrent-updates[]
}
