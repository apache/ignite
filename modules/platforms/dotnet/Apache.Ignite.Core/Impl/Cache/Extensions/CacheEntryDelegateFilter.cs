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

namespace Apache.Ignite.Core.Impl.Cache.Extensions
{
    using System;
    using System.Runtime.Serialization;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Impl.Common;

    /// <summary>
    /// Cache entry filter from delegate.
    /// </summary>
    [Serializable]
    internal class CacheEntryDelegateFilter<K, V> : SerializableWrapper<Func<ICacheEntry<K, V>, bool>>, 
        ICacheEntryFilter<K, V>
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="CacheEntryDelegateFilter{K, V}"/> class.
        /// </summary>
        /// <param name="obj">The object to wrap.</param>
        public CacheEntryDelegateFilter(Func<ICacheEntry<K, V>, bool> obj) : base(obj)
        {
            // No-op.
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="CacheEntryDelegateFilter{K, V}"/> class.
        /// </summary>
        public CacheEntryDelegateFilter(SerializationInfo info, StreamingContext context) : base(info, context)
        {
            // No-op.
        }

        /** <inheritdoc /> */
        public bool Invoke(ICacheEntry<K, V> entry)
        {
            return WrappedObject(entry);
        }
    }
}