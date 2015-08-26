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

namespace Apache.Ignite.Core.Impl.Common
{
    using System;
    using System.Diagnostics.CodeAnalysis;
    using System.Threading;
    using Apache.Ignite.Core.Common;

    /// <summary>
    /// Adapts IGridFuture to the IAsyncResult.
    /// </summary>
    [SuppressMessage("Microsoft.Design", "CA1001:TypesThatOwnDisposableFieldsShouldBeDisposable",
        Justification = "Implementing IDisposable has no point since we return this class as IAsyncResult " +
                        "to the client, and IAsyncResult is not IDisposable.")]
    public class AsyncResult : IAsyncResult
    {
        /** */
        private readonly ManualResetEvent _waitHandle;

        /// <summary>
        /// Initializes a new instance of the <see cref="AsyncResult"/> class.
        /// </summary>
        /// <param name="fut">The future to wrap.</param>
        public AsyncResult(IFuture fut)
        {
            _waitHandle = new ManualResetEvent(false);

            fut.Listen(() => _waitHandle.Set());
        }

        /** <inheritdoc /> */
        public bool IsCompleted
        {
            get { return _waitHandle.WaitOne(0); }
        }

        /** <inheritdoc /> */
        public WaitHandle AsyncWaitHandle
        {
            get { return _waitHandle; }
        }

        /** <inheritdoc /> */
        public object AsyncState
        {
            get { return null; }
        }

        /** <inheritdoc /> */
        public bool CompletedSynchronously
        {
            get { return false; }
        }
    }
}