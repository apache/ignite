#include <ignite/impl/thin/transactions/transactions_proxy.h>

#include <impl/transactions/transactions_impl.h>

#include <string>

using namespace ignite::impl::thin;
using namespace transactions;

namespace
{
    using namespace ignite::common::concurrent;

    TransactionsImpl& GetTxsImpl(SharedPointer<void>& ptr)
    {
        return *reinterpret_cast<TransactionsImpl*>(ptr.Get());
    }

    TransactionProxy& GetTxssImpl(SharedPointer<TransactionImpl>& ptr)
    {
        return *reinterpret_cast<TransactionProxy*>(ptr.Get());
    }
}

namespace ignite
{
    namespace impl
    {
        namespace thin
        {
            namespace transactions
            {
                TransactionProxy TransactionsProxy::txStart()
                {
                    common::concurrent::SharedPointer<TransactionImpl> tx = GetTxsImpl(impl).TxStart();

                    return GetTxssImpl(tx);
                }

                void TransactionProxy::commit()
                {
                    GetTxsImpl(impl).GetTx().Get()->commit();
                }
            }
        }
    }
}
