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

    Cache<std::string, int32_t> cache = ignite.GetOrCreateCache<std::string, int32_t>("myCache");

    //tag::transactions-execution[]
    Transactions transactions = ignite.GetTransactions();

    Transaction tx = transactions.TxStart();
    int hello = cache.Get("Hello");

    if (hello == 1)
        cache.Put("Hello", 11);

    cache.Put("World", 22);

    tx.Commit();
    //end::transactions-execution[]

    //tag::transactions-optimistic[]
    // Re-try the transaction a limited number of times.
    int const retryCount = 10;
    int retries = 0;
    
    // Start a transaction in the optimistic mode with the serializable isolation level.
    while (retries < retryCount)
    {
        retries++;
    
        try
        {
            Transaction tx = ignite.GetTransactions().TxStart(
                    TransactionConcurrency::OPTIMISTIC, TransactionIsolation::SERIALIZABLE);

            // commit the transaction
            tx.Commit();

            // the transaction succeeded. Leave the while loop.
            break;
        }
        catch (IgniteError e)
        {
            // Transaction has failed. Retry.
        }
    }
    //end::transactions-optimistic[]
}
