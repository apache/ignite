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

namespace Apache.Ignite.Core.Impl.Cache.Near
{
    using System.Diagnostics;
    using System.Threading;
    using Apache.Ignite.Core.Cache.Affinity;

    /// <summary>
    /// Near cache entry.
    /// </summary>
    /// <typeparam name="T">Value type.</typeparam>
    internal class NearCacheEntry<T>
    {
        /** Value. */
        private readonly T _value;

        /** Version. Boxed <see cref="AffinityTopologyVersion"/>. Stored as object for atomic updates.
         * Saves memory as well, because sizeof(AffinityTopologyVersion) > sizeof(void*) */
        private object _version;

        /** Partition. */
        private volatile int _partition;

        public NearCacheEntry(T value, object version, int partition)
        {
            Debug.Assert(version != null);
            
            _value = value;
            _version = version;
            _partition = partition;
        }

        public T Value
        {
            get { return _value; }
        }

        public object Version
        {
            get { return Interlocked.CompareExchange(ref _version, null, null); }
        }

        public int Partition
        {
            get { return _partition; }
            set { _partition = value; }
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