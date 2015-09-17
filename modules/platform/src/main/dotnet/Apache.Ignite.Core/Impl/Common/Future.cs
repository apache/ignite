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
    using System.Collections.Generic;
    using System.Diagnostics.CodeAnalysis;
    using System.Threading;
    using System.Threading.Tasks;
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Impl.Portable.IO;

    /// <summary>
    /// Grid future implementation.
    /// </summary>
    [SuppressMessage("ReSharper", "ParameterHidesMember")]
    [CLSCompliant(false)]
    public sealed class Future<T> : IFutureInternal, IFuture<T>
    {
        /** Converter. */
        private readonly IFutureConverter<T> _converter;

        /** Result. */
        private T _res;

        /** Caught cxception. */
        private Exception _err;

        /** Done flag. */
        private volatile bool _done;

        /** Listener(s). Either Action or List{Action}. */
        private object _callbacks;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="converter">Future result marshaller and converter.</param>
        public Future(IFutureConverter<T> converter = null)
        {
            _converter = converter;
        }

        /** <inheritdoc/> */
        public bool IsDone
        {
            get { return _done; }
        }

        /** <inheritdoc/> */
        public T Get()
        {
            if (!_done)
            {
                lock (this)
                {
                    while (!_done)
                        Monitor.Wait(this);
                }
            }

            return Get0();
        }

        /** <inheritdoc/> */
        public T Get(TimeSpan timeout)
        {
            long ticks = timeout.Ticks;

            if (ticks < 0)
                throw new ArgumentException("Timeout cannot be negative.");

            if (ticks == 0)
                return Get();

            if (!_done)
            {
                // Fallback to locked mode.
                lock (this)
                {
                    long endTime = DateTime.Now.Ticks + ticks;

                    if (!_done)
                    {
                        while (true)
                        {
                            Monitor.Wait(this, timeout);

                            if (_done)
                                break;

                            ticks = endTime - DateTime.Now.Ticks;

                            if (ticks <= 0)
                                throw new TimeoutException("Timeout waiting for future completion.");

                            timeout = TimeSpan.FromTicks(ticks);
                        }
                    }
                }
            }

            return Get0();
        }

        /** <inheritdoc/> */
        public void Listen(Action callback)
        {
            Listen((Action<IFuture<T>>) (fut => callback()));
        }

        /** <inheritdoc/> */
        public void Listen(Action<IFuture> callback)
        {
            Listen((Action<IFuture<T>>)callback);
        }

        /** <inheritdoc/> */
        public void Listen(Action<IFuture<T>> callback)
        {
            IgniteArgumentCheck.NotNull(callback, "callback");

            if (!_done)
            {
                lock (this)
                {
                    if (!_done)
                    {
                        AddCallback(callback);

                        return;
                    }
                }
            }

            callback(this);
        }

        /// <summary>
        /// Get result or throw an error.
        /// </summary>
        private T Get0()
        {
            if (_err != null)
                throw _err;

            return _res;
        }

        /** <inheritdoc/> */
        public IAsyncResult ToAsyncResult()
        {
            return _done ? CompletedAsyncResult.Instance : new AsyncResult(this);
        }

        /** <inheritdoc/> */
        Task<object> IFuture.ToTask()
        {
            return Task.Factory.FromAsync(ToAsyncResult(), x => (object) Get());
        }

        /** <inheritdoc/> */
        public Task<T> ToTask()
        {
            return Task.Factory.FromAsync(ToAsyncResult(), x => Get());
        }

        /** <inheritdoc/> */
        object IFuture.Get(TimeSpan timeout)
        {
            return Get(timeout);
        }

        /** <inheritdoc/> */
        object IFuture.Get()
        {
            return Get();
        }

        /** <inheritdoc /> */
        public void OnResult(IPortableStream stream)
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

        /** <inheritdoc /> */
        public void OnError(Exception err)
        {
            OnDone(default(T), err);
        }

        /** <inheritdoc /> */
        public void OnNullResult()
        {
            OnResult(default(T));
        }

        /// <summary>
        /// Set result.
        /// </summary>
        /// <param name="res">Result.</param>
        internal void OnResult(T res)
        {
            OnDone(res, null);
        }

        /// <summary>
        /// Set future to Done state.
        /// </summary>
        /// <param name="res">Result.</param>
        /// <param name="err">Error.</param>
        public void OnDone(T res, Exception err)
        {
            object callbacks0 = null;

            lock (this)
            {
                if (!_done)
                {
                    _res = res;
                    _err = err;

                    _done = true;

                    Monitor.PulseAll(this);

                    // Notify listeners outside the lock
                    callbacks0 = _callbacks;
                    _callbacks = null;
                }
            }

            if (callbacks0 != null)
            {
                var list = callbacks0 as List<Action<IFuture<T>>>;

                if (list != null)
                    list.ForEach(x => x(this));
                else
                    ((Action<IFuture<T>>) callbacks0)(this);
            }
        }

        /// <summary>
        /// Adds a callback.
        /// </summary>
        private void AddCallback(Action<IFuture<T>> callback)
        {
            if (_callbacks == null)
            {
                _callbacks = callback;

                return;
            }

            var list = _callbacks as List<Action<IFuture<T>>> ??
                new List<Action<IFuture<T>>> {(Action<IFuture<T>>) _callbacks};

            list.Add(callback);

            _callbacks = list;
        }
    }
}
