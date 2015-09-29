/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Client.Benchmark.Interop
{
    using System.Collections.Generic;
    using GridGain.Cache;
    using GridGain.Transactions;

    /// <summary>
    ///
    /// </summary>
    internal class GridClientTxInteropBenchmark : GridClientAbstractInteropBenchmark
    {
        /** Cache name. */
        private const string CACHE_NAME = "cache_tx";

        /** Native cache wrapper. */
        private ICache<object, object> cache;

        /** <inheritDoc /> */
        protected override void OnStarted()
        {
            base.OnStarted();

            cache = node.Cache<object, object>(CACHE_NAME);
        }

        /** <inheritDoc /> */
        protected override void Descriptors(ICollection<GridClientBenchmarkOperationDescriptor> descs)
        {
            descs.Add(GridClientBenchmarkOperationDescriptor.Create("PutTx", PutTx, 1));
        }

        /// <summary>
        /// Cache put.
        /// </summary>
        private void PutTx(GridClientBenchmarkState state)
        {
            int idx = GridClientBenchmarkUtils.RandomInt(Dataset);

            using (var tx = node.Transactions.TxStart(TransactionConcurrency.PESSIMISTIC,
                TransactionIsolation.REPEATABLE_READ))
            {
                cache.Put(idx, emps[idx]);

                tx.Commit();
            }
        }
    }
}
