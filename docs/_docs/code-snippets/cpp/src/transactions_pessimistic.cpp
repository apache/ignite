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

    Ignite ignite = Ignition::Start(cfg);
    
    Cache<int32_t, int32_t> cache = ignite.GetOrCreateCache<int32_t, int32_t>("myCache");

    //tag::transactions-pessimistic[]
    try {
        Transaction tx = ignite.GetTransactions().TxStart(
            TransactionConcurrency::PESSIMISTIC, TransactionIsolation::READ_COMMITTED, 300, 0);
        cache.Put(1, 1);
    
        cache.Put(2, 1);
    
        tx.Commit();
    }
    catch (IgniteError& err)
    {
        std::cout << "An error occurred: " << err.GetText() << std::endl;
        std::cin.get();
        return err.GetCode();
    }
    //end::transactions-pessimistic[]
}
