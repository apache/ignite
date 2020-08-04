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

    TransactionImpl& GetTxssImpl(SharedPointer<void>& ptr)
    {
        return *reinterpret_cast<TransactionImpl*>(ptr.Get());
    }

    TransactionProxy* GetTxImpl(void* ptr)
    {
        return reinterpret_cast<TransactionProxy*>(ptr);
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
                TransactionProxy* TransactionsProxy::txStart()
                {
                    return new TransactionProxy(GetTxsImpl(impl).TxStart());
                    //return GetTxImpl((void*)GetTxsImpl(impl).txStart());
                }

                void TransactionProxy::commit()
                {
                    GetTxssImpl(impl).commit();
                }
            }
        }
    }
}
