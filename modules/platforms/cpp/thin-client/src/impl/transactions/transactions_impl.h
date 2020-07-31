#ifndef TRANSACTIONS_IMPL_H
#define TRANSACTIONS_IMPL_H

#include "impl/data_router.h"

namespace ignite
{
    namespace impl
    {
        namespace thin
        {
            namespace transactions
            {
                class TransactionImpl
                {
                public:
                    TransactionImpl();
                    
                    ~TransactionImpl() {}
                    
                    void commit();
    
                    void rollback() {}
    
                    void close() {}
                };
            
                class TransactionsImpl
                {
                public:
                    /**
                     * Constructor.
                     *
                     * @param router Data router instance.
                     */
                    TransactionsImpl(const SP_DataRouter& router);

                    /**
                     * Destructor.
                     */
                    ~TransactionsImpl();

                    TransactionImpl* txStart();

                private:
                    /** Data router. */
                    SP_DataRouter router;
                };

                typedef common::concurrent::SharedPointer<TransactionsImpl> SP_TransactionsImpl;
            }
        }
    }
}

#endif // TRANSACTIONS_IMPL_H
