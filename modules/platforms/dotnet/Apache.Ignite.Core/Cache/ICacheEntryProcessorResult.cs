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
    /// Represents a result of processing <see cref="ICacheEntry{K, V}" />
    /// by <see cref="ICacheEntryProcessor{K, V, A, R}" />.
    /// </summary>
    /// <typeparam name="TK">Key type.</typeparam>
    /// <typeparam name="T">Processor result type.</typeparam>
    public interface ICacheEntryProcessorResult<out TK, out T>
    {
        /// <summary>
        /// Gets the cache key.
        /// </summary>
        TK Key { get; }

        /// <summary>
        /// Gets the result of processing an entry.
        /// <para />
        /// If an exception was thrown during the processing of an entry, 
        /// either by the <see cref="ICacheEntryProcessor{K, V, A, R}"/> itself 
        /// or by the Caching implementation, the exceptions will be wrapped and re-thrown as a 
        /// <see cref="CacheEntryProcessorException"/> when calling this property.
        /// </summary>
        /// <value>
        /// The result.
        /// </value>
        T Result { get; }
    }
}