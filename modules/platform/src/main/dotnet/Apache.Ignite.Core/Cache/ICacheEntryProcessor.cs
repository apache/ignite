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

namespace Apache.Ignite.Core.Cache
{
    /// <summary>
    /// An invocable function that allows applications to perform compound operations
    /// on a cache entry atomically, according the defined consistency of a cache.
    /// <para />
    /// Any cache entry mutations will not take effect until after
    /// the <see cref="Process" /> method has completedS execution.
    /// <para />
    /// If an exception is thrown by an entry processor, a Caching Implementation
    /// must wrap any exception thrown wrapped in an <see cref="CacheEntryProcessorException" />
    /// If this occurs no mutations will be made to the cache entry.
    /// </summary>
    /// <typeparam name="TK">Key type.</typeparam>
    /// <typeparam name="TV">Value type.</typeparam>
    /// <typeparam name="TA">The type of the processor argument.</typeparam>
    /// <typeparam name="TR">The type of the processor result.</typeparam>
    public interface ICacheEntryProcessor<in TK, TV, in TA, out TR>
    {
        /// <summary>
        /// Process an entry.
        /// </summary>
        /// <param name="entry">The entry to process.</param>
        /// <param name="arg">The argument.</param>
        /// <returns>Processing result.</returns>
        TR Process(IMutableCacheEntry<TK, TV> entry, TA arg);
    }
}