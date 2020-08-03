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
                    TransactionImpl(void * _impl, int32_t _txId)
                    {
                        impl = _impl;
                        txId = _txId;
                    }
                    
                    ~TransactionImpl() {}
                    
                    void commit();
    
                    void rollback() {}
    
                    void close() {}
                private:
                    void* impl;

                    int32_t txId;
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

                    template<typename ReqT, typename RspT>
                    void SyncMessage(const ReqT& req, RspT& rsp);
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
