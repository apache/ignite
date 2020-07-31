#ifndef TRANSACTIONS_PROXY_H
#define TRANSACTIONS_PROXY_H

#include <ignite/common/concurrent.h>

namespace ignite
{
    namespace impl
    {
        namespace thin
        {
            namespace transactions
            {
            /**
             * Ignite transaction class proxy.
             */
                class IGNITE_IMPORT_EXPORT TransactionProxy {

                public:
                    /**
                     * Default constructor.
                     */
                    TransactionProxy()
                    {
                        // No-op.
                    }

                    TransactionProxy(const common::concurrent::SharedPointer<void>& impl) :
                        impl(impl)
                    {

                    }

                    ~TransactionProxy() {};

                    void commit();

                    void rollback();

                    void close();

                private:
                    /** Implementation. */
                    common::concurrent::SharedPointer<void> impl;
                };

                /**
                 * Ignite transactions class proxy.
                 */
                class IGNITE_IMPORT_EXPORT TransactionsProxy
                {
                public:
                    /**
                     * Default constructor.
                     */
                    TransactionsProxy()
                    {
                        // No-op.
                    }

                    /**
                     * Constructor.
                     */
                    TransactionsProxy(const common::concurrent::SharedPointer<void>& impl) :
                        impl(impl)
                    {
                        // No-op.
                    }

                    /**
                     * Destructor.
                     */
                    ~TransactionsProxy()
                    {
                        // No-op.
                    }

                    /** */
                    TransactionProxy* txStart();

                private:
                    /** Implementation. */
                    common::concurrent::SharedPointer<void> impl;
                };
            }
        }
    }
}

#endif // TRANSACTIONS_PROXY_H
