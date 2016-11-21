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
    using System.Diagnostics;
    using System.Diagnostics.CodeAnalysis;
    using System.Threading;
    using System.Threading.Tasks;
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Impl.Binary.IO;

    /// <summary>
    /// Grid future implementation.
    /// </summary>
    [SuppressMessage("ReSharper", "ParameterHidesMember")]
    [CLSCompliant(false)]
    public sealed class Future<T> : IFutureInternal
    {
        /** Converter. */
        private readonly IFutureConverter<T> _converter;

        /** Task completion source. */
        private readonly TaskCompletionSource<T> _taskCompletionSource = new TaskCompletionSource<T>();

        /** */
        private volatile Listenable _listenable;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="converter">Future result marshaller and converter.</param>
        public Future(IFutureConverter<T> converter = null)
        {
            _converter = converter;
        }

        /// <summary>
        /// Gets the result.
        /// </summary>
        public T Get()
        {
            try
            {
                return Task.Result;
            }
            catch (AggregateException ex)
            {
                if (ex.InnerException != null)
                    throw ex.InnerException;

                throw;
            }
        }

        /// <summary>
        /// Gets the task.
        /// </summary>
        [SuppressMessage("Microsoft.Naming", "CA1721:PropertyNamesShouldNotMatchGetMethods")]
        public Task<T> Task
        {
            get { return _taskCompletionSource.Task; }
        }

        /// <summary>
        /// Gets the task with cancellation.
        /// </summary>
        /// <param name="cancellationToken">The cancellation token.</param>
        public Task<T> GetTask(CancellationToken cancellationToken)
        {
            Debug.Assert(_listenable != null);

            // OnTokenCancel will fire even if cancellationToken is already cancelled.
            cancellationToken.Register(OnTokenCancel);

            return Task;
        }

        /// <summary>
        /// Set result from stream.
        /// </summary>
        /// <param name="stream">Stream.</param>
        [SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes")]
        public void OnResult(IBinaryStream stream)
        {
            try
            {
                OnResult(_converter.Convert(stream));
            }
            catch (Exception ex)
            {
                OnError(ex);
            }
        }

        /// <summary>
        /// Set error result.
        /// </summary>
        /// <param name="err">Exception.</param>
        public void OnError(Exception err)
        {
            if (err is IgniteFutureCancelledException)
                _taskCompletionSource.TrySetCanceled();
            else
                _taskCompletionSource.TrySetException(err);
        }

        /// <summary>
        /// Set null result.
        /// </summary>
        [SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes")]
        public void OnNullResult()
        {
            if (_converter == null)
            {
                OnResult(default(T));

                return;
            }

            try
            {
                OnResult(_converter.Convert(null));
            }
            catch (Exception ex)
            {
                OnError(ex);
            }
        }

        /// <summary>
        /// Set result.
        /// </summary>
        /// <param name="res">Result.</param>
        internal void OnResult(T res)
        {
            _taskCompletionSource.TrySetResult(res);
        }

        /// <summary>
        /// Set future to Done state.
        /// </summary>
        /// <param name="res">Result.</param>
        /// <param name="err">Error.</param>
        public void OnDone(T res, Exception err)
        {
            if (err != null)
                OnError(err);
            else
                OnResult(res);
        }

        /// <summary>
        /// Sets unmanaged future target for cancellation.
        /// </summary>
        internal void SetTarget(Listenable target)
        {
            Debug.Assert(target != null);

            _listenable = target;
        }

        /// <summary>
        /// Called when token cancellation occurs.
        /// </summary>
        private void OnTokenCancel()
        {
            if (_listenable != null)
                _listenable.Cancel();
        }
    }
}
