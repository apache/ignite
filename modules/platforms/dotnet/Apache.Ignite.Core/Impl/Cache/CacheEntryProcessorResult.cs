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

namespace Apache.Ignite.Core.Impl.Cache
{
    using System;
    using Apache.Ignite.Core.Cache;

    /// <summary>
    /// Represents a result of <see cref="ICacheEntryProcessor{TK,TV,TA,TR}"/> invocation.
    /// </summary>
    /// <typeparam name="T">Result type.</typeparam>
    internal class CacheEntryProcessorResult<T> : ICacheEntryProcessorResult<T>
    {
        // Result
        private readonly T _res;

        // Error
        private readonly Exception _err;

        /// <summary>
        /// Initializes a new instance of the <see cref="CacheEntryProcessorResult{T}"/> class.
        /// </summary>
        /// <param name="result">The result.</param>
        public CacheEntryProcessorResult(T result)
        {
            _res = result;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="CacheEntryProcessorResult{T}"/> class.
        /// </summary>
        /// <param name="error">The error.</param>
        public CacheEntryProcessorResult(Exception error)
        {
            _err = error;
        }

        /** <inheritdoc /> */
        public T Result
        {
            get
            {
                if (_err != null)
                    throw _err as CacheEntryProcessorException ?? new CacheEntryProcessorException(_err);

                return _res;
            }
        }
    }
}