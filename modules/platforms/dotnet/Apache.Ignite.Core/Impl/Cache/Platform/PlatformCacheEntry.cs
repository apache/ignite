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

namespace Apache.Ignite.Core.Impl.Cache.Platform
{
    using System.Diagnostics;
    using System.Threading;

    /// <summary>
    /// Platform cache entry.
    /// </summary>
    /// <typeparam name="T">Value type.</typeparam>
    internal class PlatformCacheEntry<T>
    {
        /** Value. */
        private readonly T _value;

        /** Partition. */
        private readonly int _partition;
        
        /** Version. Boxed <see cref="Apache.Ignite.Core.Cache.Affinity.AffinityTopologyVersion"/>.
         * Stored as object for atomic updates.
         * Saves memory as well, because sizeof(AffinityTopologyVersion) > sizeof(void*) */
        private object _version;

        /// <summary>
        /// Initializes a new instance of <see cref="PlatformCacheEntry{T}"/> class.
        /// </summary>
        public PlatformCacheEntry(T value, object version, int partition)
        {
            Debug.Assert(version != null);
            
            _value = value;
            _version = version;
            _partition = partition;
        }

        /// <summary>
        /// Gets the value.
        /// </summary>
        public T Value
        {
            get { return _value; }
        }

        /// <summary>
        /// Gets the version.
        /// Returns boxed <see cref="Apache.Ignite.Core.Cache.Affinity.AffinityTopologyVersion"/>.
        /// </summary>
        public object Version
        {
            get { return Interlocked.CompareExchange(ref _version, null, null); }
        }

        /// <summary>
        /// Gets the partition where this cache entry is assigned to.
        /// </summary>
        public int Partition
        {
            get { return _partition; }
        }

        /// <summary>
        /// Sets new version using <see cref="Interlocked.CompareExchange(ref object,object,object)"/>.
        /// </summary>
        public void CompareExchangeVersion(object newVal, object oldVal)
        {
            Interlocked.CompareExchange(ref _version, newVal, oldVal);
        }
    }
}