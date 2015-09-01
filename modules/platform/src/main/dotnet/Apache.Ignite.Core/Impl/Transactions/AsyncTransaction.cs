﻿﻿﻿/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace Apache.Ignite.Core.Impl.Transactions
{
    using System;
    using System.Threading;
    using Apache.Ignite.Core.Common;

    /// <summary>
    /// Grid async transaction facade.
    /// </summary>
    internal class AsyncTransaction : Transaction
    {
        /** */
        private readonly ThreadLocal<IFuture> curFut = new ThreadLocal<IFuture>();

        /// <summary>
        /// Initializes a new instance of the <see cref="AsyncTransaction"/> class.
        /// </summary>
        /// <param name="tx">The tx to wrap.</param>
        public AsyncTransaction(TransactionImpl tx) : base(tx)
        {
            // No-op.
        }

        /** <inheritDoc /> */
        public override bool IsAsync
        {
            get { return true; }
        }

        /** <inheritDoc /> */
        public override IFuture<TResult> GetFuture<TResult>()
        {
            return GetFuture() as IFuture<TResult>;
        }

        /** <inheritDoc /> */
        public override IFuture GetFuture()
        {
            var fut = curFut.Value;

            if (fut == null)
                throw new InvalidOperationException("Asynchronous operation not started.");

            curFut.Value = null;

            return fut;
        }

        /** <inheritDoc /> */
        public override void Commit()
        {
            curFut.Value = tx.GetFutureOrError(() => tx.CommitAsync());
        }

        /** <inheritDoc /> */
        public override void Rollback()
        {
            curFut.Value = tx.GetFutureOrError(() => tx.RollbackAsync());
        }
    }
}