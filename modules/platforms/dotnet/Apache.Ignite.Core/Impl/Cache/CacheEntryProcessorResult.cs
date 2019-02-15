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

namespace Apache.Ignite.Core.Impl.Cache
{
    using System;
    using Apache.Ignite.Core.Cache;

    /// <summary>
    /// Represents a result of <see cref="ICacheEntryProcessor{TK,TV,TA,TR}" /> invocation.
    /// </summary>
    /// <typeparam name="TK">Key type.</typeparam>
    /// <typeparam name="T">Result type.</typeparam>
    /// <seealso cref="Apache.Ignite.Core.Cache.ICacheEntryProcessorResult{TK, T}" />
    internal class CacheEntryProcessorResult<TK, T> : ICacheEntryProcessorResult<TK, T>
    {
        // Key
        private readonly TK _key;

        // Result
        private readonly T _res;

        // Error
        private readonly Exception _err;

        /// <summary>
        /// Initializes a new instance of the <see cref="CacheEntryProcessorResult{TK, T}" /> class.
        /// </summary>
        /// <param name="key">The key.</param>
        /// <param name="result">The result.</param>
        public CacheEntryProcessorResult(TK key, T result)
        {
            _key = key;
            _res = result;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="CacheEntryProcessorResult{TK, T}" /> class.
        /// </summary>
        /// <param name="key">The key.</param>
        /// <param name="error">The error.</param>
        public CacheEntryProcessorResult(TK key, Exception error)
        {
            _key = key;
            _err = error;
        }

        /** <inheritdoc /> */
        public TK Key
        {
            get { return _key; }
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