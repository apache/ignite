#ifndef TRANSACTIONS_IMPL_H
#define TRANSACTIONS_IMPL_H

#include "impl/data_router.h"

#include <ignite/thin/transactions/transaction_consts.h>

#include "string"

using namespace ignite::thin::transactions;

namespace ignite
{
    namespace impl
    {
        namespace thin
        {
            namespace transactions
            {
                class TransactionsImpl;

                class TransactionImpl
                {
                    typedef ignite::common::concurrent::SharedPointer<TransactionImpl> SP_TransactionImpl;
                    typedef ignite::common::concurrent::SharedPointer<TransactionsImpl> SP_TransactionsImpl;
                    typedef ignite::common::concurrent::ThreadLocalInstance<SP_TransactionImpl> TL_SP_TransactionsImpl;

                public:
                    TransactionImpl(SP_TransactionsImpl _txs, int32_t id,
                        TransactionConcurrency::Type concurrency, TransactionIsolation::Type isolation, int64_t timeout, int32_t txSize) :
                        txs(_txs),
                        txId(id),
                        concurrency(concurrency),
                        isolation(isolation),
                        timeout(timeout),
                        txSize(txSize),
                        state(TransactionState::UNKNOWN),
                        closed(false)
                    {
                        // No-op.
                    }
                    
                    ~TransactionImpl() {}
                    
                    void commit();
    
                    void rollback();
    
                    void close() {}

                    int32_t TxId()
                    {
                        return txId;
                    }

                    /** Transactions. */
                    SP_TransactionsImpl txs;

                    static SP_TransactionImpl GetCurrent();

                    /**
                     * Check if the transaction has been closed.
                     *
                     * @return True if the transaction has been closed.
                     */
                    bool IsClosed() const;

                    static SP_TransactionImpl Create(
                            SP_TransactionsImpl txs, TransactionConcurrency::Type concurrency, TransactionIsolation::Type isolation, int64_t timeout, int32_t txSize);
                private:
                    int32_t txId;

                    /** Thread local instance of the transaction. */
                    static TL_SP_TransactionsImpl threadTx;

                    /** Concurrency. */
                    int concurrency;

                    /** Isolation. */
                    int isolation;

                    /** Timeout in milliseconds. */
                    int64_t timeout;

                    /** Transaction size. */
                    int32_t txSize;

                    /** Transaction state. */
                    TransactionState::Type state;

                    /** Closed flag. */
                    bool closed;

                    IGNITE_NO_COPY_ASSIGNMENT(TransactionImpl)
                };
            
                class TransactionsImpl
                {
                    typedef ignite::common::concurrent::SharedPointer<TransactionImpl> SP_TransactionImpl;
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

                    SP_TransactionImpl TxStart();

                    int32_t TxCommit(int32_t);

                    int32_t TxRollback(int32_t);

                    template<typename ReqT, typename RspT>
                    void SyncMessage(const ReqT& req, RspT& rsp);

                    SP_TransactionImpl GetTx();

                    /** Data router. */
                    SP_DataRouter router;
                private:
                };

                typedef common::concurrent::SharedPointer<TransactionsImpl> SP_TransactionsImpl;
            }
        }
    }
}

#endif // TRANSACTIONS_IMPL_H
