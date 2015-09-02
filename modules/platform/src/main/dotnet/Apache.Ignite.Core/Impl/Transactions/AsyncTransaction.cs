/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
        private readonly ThreadLocal<IFuture> _curFut = new ThreadLocal<IFuture>();

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
            var fut = _curFut.Value;

            if (fut == null)
                throw new InvalidOperationException("Asynchronous operation not started.");

            _curFut.Value = null;

            return fut;
        }

        /** <inheritDoc /> */
        public override void Commit()
        {
            _curFut.Value = Tx.GetFutureOrError(() => Tx.CommitAsync());
        }

        /** <inheritDoc /> */
        public override void Rollback()
        {
            _curFut.Value = Tx.GetFutureOrError(() => Tx.RollbackAsync());
        }
    }
}