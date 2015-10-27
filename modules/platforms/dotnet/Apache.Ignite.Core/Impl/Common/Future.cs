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
        public T Get()
        {
            try
            {
                return Task.Result;
            }
            catch (AggregateException ex)
            {
                throw ex.InnerException;
            }
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
            _taskCompletionSource.TrySetException(err);
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
    }
}
