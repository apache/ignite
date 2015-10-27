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
    using Apache.Ignite.Core.Impl.Portable.IO;

    /// <summary>
    /// Grid future implementation.
    /// </summary>
    [SuppressMessage("ReSharper", "ParameterHidesMember")]
    [CLSCompliant(false)]
    public sealed class Future<T> : IFutureInternal
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

        /** Task completion source. */
        private readonly TaskCompletionSource<T> _taskCompletionSource = new TaskCompletionSource<T>();

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
            Listen(fut => callback());
        }

        /** <inheritdoc/> */
        public void Listen(Action<Future<T>> callback)
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
        public Task<T> Task
        {
            get { return _taskCompletionSource.Task; }
        }

        /** <inheritdoc /> */
        [SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes")]
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

            if (err != null)
                _taskCompletionSource.SetException(err);
            else
                _taskCompletionSource.SetResult(res);

            if (callbacks0 != null)
            {
                var list = callbacks0 as List<Action<Future<T>>>;

                if (list != null)
                    list.ForEach(x => x(this));
                else
                    ((Action<Future<T>>) callbacks0)(this);
            }
        }

        /// <summary>
        /// Adds a callback.
        /// </summary>
        private void AddCallback(Action<Future<T>> callback)
        {
            if (_callbacks == null)
            {
                _callbacks = callback;

                return;
            }

            var list = _callbacks as List<Action<Future<T>>> ??
                new List<Action<Future<T>>> {(Action<Future<T>>) _callbacks};

            list.Add(callback);

            _callbacks = list;
        }
    }
}
