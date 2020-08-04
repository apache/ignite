
#include "impl/response_status.h"
#include "impl/message.h"
#include "impl/transactions/transactions_impl.h"

#include "string"

using namespace ignite::common::concurrent;

namespace ignite
{
    namespace impl
    {
        namespace thin
        {
            namespace transactions
            {
                TransactionImpl::TL_SP_TransactionsImpl TransactionImpl::threadTx;

                TransactionsImpl::TransactionsImpl(const SP_DataRouter& router) :
                    router(router)
                {
                    // No-op.
                }

                TransactionsImpl::~TransactionsImpl()
                {
                    // No-op.
                }

                SharedPointer<TransactionImpl> TransactionsImpl::TxStart()
                {
                    TransactionConcurrency::Type concurrency = TransactionConcurrency::PESSIMISTIC;

                    TransactionIsolation::Type isolation = TransactionIsolation::REPEATABLE_READ;

                    int64_t timeout = 0;

                    int32_t txSize = 0;

                    //return new TransactionImpl(new DataRouter(cfg), rsp.GetValue());

                    //IgniteError err = IgniteError();

                    SharedPointer<TransactionImpl> tx = TransactionImpl::Create(this,
                        concurrency, isolation, timeout, txSize);

                    return tx;
                }

                TransactionImpl::SP_TransactionImpl TransactionImpl::GetCurrent()
                {
                    SP_TransactionImpl tx = threadTx.Get();
                    TransactionImpl* ptr = tx.Get();

                    /*if (ptr && ptr->IsClosed())
                    {
                        tx = SP_TransactionImpl();

                        threadTx.Remove();
                    }*/

                    std::cout << " txId requested: " << tx.Get() << std::endl;

                    return tx;
                }

                ignite::common::concurrent::SharedPointer<TransactionImpl> TransactionsImpl::GetTx()
                {
                    return TransactionImpl::GetCurrent();
                }

                TransactionImpl::SP_TransactionImpl TransactionImpl::Create(
                    SP_TransactionsImpl txs, TransactionConcurrency::Type concurrency, TransactionIsolation::Type isolation, int64_t timeout, int32_t txSize)
                {
                    TxStartRequest<RequestType::OP_TX_START> req(concurrency, isolation, timeout, txSize);

                    Int32Response rsp;

                    txs.Get()->SyncMessage(req, rsp);

                    int32_t txId = rsp.GetValue();

                    SP_TransactionImpl tx;

                    if (rsp.GetError().empty())
                    {
                        tx = SP_TransactionImpl(new TransactionImpl(txs, txId, concurrency, isolation, timeout, txSize));

                        std::cout << txId << " txId  create: " << tx.Get() << std::endl;

                        threadTx.Set(tx);
                    }

                    return tx;
                }

                template<typename ReqT, typename RspT>
                void TransactionsImpl::SyncMessage(const ReqT& req, RspT& rsp)
                {
                    router.Get()->SyncMessage(req, rsp);

                    if (rsp.GetStatus() != ResponseStatus::SUCCESS)
                        throw IgniteError(IgniteError::IGNITE_ERR_CACHE, rsp.GetError().c_str());
                }

                void TransactionsImpl::TxCommit(int32_t txId)
                {
                    TxEndRequest<RequestType::OP_TX_END> req(txId, true);

                    Response rsp;

                    SyncMessage(req, rsp);
                }

                void TransactionsImpl::TxRollback(int32_t txId)
                {
                    TxEndRequest<RequestType::OP_TX_END> req(txId, false);

                    Response rsp;

                    std::cout << txId << " txId  rollback:" << std::endl;

                    SyncMessage(req, rsp);

                    std::cout << txId << " rollback: " << rsp.GetStatus() << std::endl;
                }

                void TransactionImpl::commit() {
                    txs.Get()->TxCommit(txId);
                }

                void TransactionImpl::rollback() {
                    txs.Get()->TxRollback(txId);
                }
            }
        }
    }
}
