#ifndef TRANSACTIONS_H
#define TRANSACTIONS_H

#include <string>

#include <ignite/common/concurrent.h>

#include <ignite/thin/transactions/transaction_consts.h>
#include <ignite/impl/thin/transactions/transactions_proxy.h>

using namespace ignite::impl::thin::transactions;

namespace ignite
{
    namespace thin
    {
        namespace transactions
        {
            class ClientTransaction {

            public:                
                ClientTransaction(common::concurrent::SharedPointer<void> impl) :
                    proxy(impl)
                {

                }

                ClientTransaction() {}

                ~ClientTransaction() {}

                void commit()
                {
                    proxy.commit();
                }

                void rollback();

                void close();
            private:
                /** Implementation. */
                impl::thin::transactions::TransactionProxy proxy;
            };

            class ClientTransactions {

            public:
                /**
                 * Constructor.
                 *
                 * @param impl Implementation.
                 */
                ClientTransactions(common::concurrent::SharedPointer<void> impl) :
                    proxy(impl)
                {
                    // No-op.
                }

                ClientTransactions() {}

                ~ClientTransactions() {}

                ClientTransaction* txStart()
                {
                    return new ClientTransaction(proxy.txStart());
                }

                ClientTransaction* txStart(TransactionConcurrency, TransactionIsolation);

                ClientTransaction* txStart(TransactionConcurrency, TransactionIsolation, long);

                ClientTransactions withLabel(std::string&);

            private:
                /** Implementation. */
                impl::thin::transactions::TransactionsProxy proxy;
            };
        }
    }
}

#endif // TRANSACTIONS_H
