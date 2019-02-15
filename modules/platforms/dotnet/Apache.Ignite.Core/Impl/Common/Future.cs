/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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
    internal sealed class Future<T> : IFutureInternal
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
        /// <exception cref="AggregateException" />
        public T Get()
        {
            return Task.Result;
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
