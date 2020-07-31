
#include "impl/response_status.h"
#include "impl/message.h"
#include "impl/transactions/transactions_impl.h"

#include "string"

namespace ignite
{
    namespace impl
    {
        namespace thin
        {
            namespace transactions
            {
                TransactionsImpl::TransactionsImpl(const SP_DataRouter& router) : router(router)
                {
                    // No-op.
                }

                TransactionsImpl::~TransactionsImpl()
                {
                    // No-op.
                }

                TransactionImpl* TransactionsImpl::txStart()
                {
                    std::cout << "txStart" << std::endl;

                    return new TransactionImpl();
                }

                TransactionImpl::TransactionImpl() {
                    std::cout << "TransactionImpl" << std::endl;
                }

                void TransactionImpl::commit() {
                    std::cout << "commit!!!" << std::endl;
                }
            }
        }
    }
}
