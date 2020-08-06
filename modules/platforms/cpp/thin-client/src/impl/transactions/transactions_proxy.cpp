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
                    return TransactionProxy(GetTxsImpl(impl).TxStart());
                }

                void TransactionProxy::commit()
                {
                    GetTxsImpl(impl).GetCurrent().Get()->Commit();
                }

                void TransactionProxy::rollback()
                {
                    GetTxsImpl(impl).GetCurrent().Get()->Rollback();
                }
            }
        }
    }
}
