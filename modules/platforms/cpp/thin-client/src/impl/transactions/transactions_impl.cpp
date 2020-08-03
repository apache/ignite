
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
                    TxStartRequest<RequestType::OP_TX_START> req(ignite::thin::transactions::TransactionConcurrency::PESSIMISTIC,
                                                                 ignite::thin::transactions::TransactionIsolation::READ_COMMITTED,
                                                                 0, 0);

                    Int32Response rsp;

                    //Response rsp;

                    SyncMessage(req, rsp);

                    std::cout << "!!! " << rsp.GetValue() << std::endl;

                    return new TransactionImpl(this, rsp.GetValue());
                }

                template<typename ReqT, typename RspT>
                void TransactionsImpl::SyncMessage(const ReqT& req, RspT& rsp)
                {
                    router.Get()->SyncMessage(req, rsp);

                    if (rsp.GetStatus() != ResponseStatus::SUCCESS)
                        throw IgniteError(IgniteError::IGNITE_ERR_CACHE, rsp.GetError().c_str());
                }

                void TransactionImpl::commit() {
                    TxEndRequest<RequestType::OP_TX_END> req(txId, true);

                    Response rsp;

                    reinterpret_cast<TransactionsImpl *>(impl)->SyncMessage(req, rsp);
                }
            }
        }
    }
}
