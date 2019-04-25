/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
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
    /// <typeparam name="TArg">The type of the processor argument.</typeparam>
    /// <typeparam name="TRes">The type of the processor result.</typeparam>
    public interface ICacheEntryProcessor<in TK, TV, in TArg, out TRes>
    {
        /// <summary>
        /// Process an entry.
        /// </summary>
        /// <param name="entry">The entry to process.</param>
        /// <param name="arg">The argument.</param>
        /// <returns>Processing result.</returns>
        TRes Process(IMutableCacheEntry<TK, TV> entry, TArg arg);
    }
}