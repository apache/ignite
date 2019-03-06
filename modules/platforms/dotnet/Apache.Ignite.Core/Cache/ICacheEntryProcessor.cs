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