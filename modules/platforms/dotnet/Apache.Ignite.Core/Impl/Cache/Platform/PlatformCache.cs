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
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Diagnostics;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Cache.Affinity;
    using Apache.Ignite.Core.Impl.Binary;
    using Apache.Ignite.Core.Impl.Binary.IO;

    /// <summary>
    /// Holds platform cache data for a given cache, serves one or more <see cref="CacheImpl{TK,TV}"/> instances.
    /// </summary>
    internal sealed class PlatformCache<TK, TV> : IPlatformCache
    {
        /** Affinity. */
        private readonly CacheAffinityManager _affinity;

        /** Keep binary flag. */
        private readonly bool _keepBinary;

        /** Topology version func. Returns boxed <see cref="AffinityTopologyVersion"/>.
         * Boxed copy is passed directly to <see cref="PlatformCacheEntry{T}"/>, avoiding extra allocations.
         * This way for every unique <see cref="AffinityTopologyVersion"/> we only have one boxed copy,
         * and we can update <see cref="PlatformCacheEntry{T}.Version"/> atomically without locks. */
        private readonly Func<object> _affinityTopologyVersionFunc;

        /** Underlying map. */
        private readonly ConcurrentDictionary<TK, PlatformCacheEntry<TV>> _map =
            new ConcurrentDictionary<TK, PlatformCacheEntry<TV>>();

        /** Stopped flag. */
        private volatile bool _stopped;

        /// <summary>
        /// Initializes a new instance of the <see cref="PlatformCache{TK,TV}"/> class.
        /// Called via reflection from <see cref="PlatformCacheManager.CreatePlatformCache"/>.
        /// </summary>
        public PlatformCache(Func<object> affinityTopologyVersionFunc, CacheAffinityManager affinity, bool keepBinary)
        {
            _affinityTopologyVersionFunc = affinityTopologyVersionFunc;
            _affinity = affinity;
            _keepBinary = keepBinary;
        }

        /** <inheritdoc /> */
        public bool IsStopped
        {
            get { return _stopped; }
        }

        /** <inheritdoc /> */
        public bool TryGetValue<TKey, TVal>(TKey key, out TVal val)
        {
            if (_stopped)
            {
                val = default(TVal);
                return false;
            }

            PlatformCacheEntry<TV> entry;
            var key0 = (TK) (object) key;

            if (_map.TryGetValue(key0, out entry))
            {
                if (IsValid(entry))
                {
                    val = (TVal) (object) entry.Value;
                    return true;
                }

                // Remove invalid entry to free up memory.
                // NOTE: We may end up removing a good entry that was inserted concurrently,
                // but this does not violate correctness, only causes a potential platform cache miss.
                _map.TryRemove(key0, out entry);
            }

            val = default(TVal);
            return false;
        }

        /** <inheritdoc /> */
        public int GetSize(int? partition)
        {
            if (_stopped)
            {
                return 0;
            }

            var count = 0;

            foreach (var e in _map)
            {
                if (!IsValid(e.Value))
                {
                    continue;
                }

                if (partition != null && e.Value.Partition != partition)
                {
                    continue;
                }

                count++;
            }

            return count;
        }

        /** <inheritdoc /> */
        public void Update(IBinaryStream stream, Marshaller marshaller)
        {
            Debug.Assert(stream != null);
            Debug.Assert(marshaller != null);

            if (_stopped)
            {
                return;
            }

            var mode = _keepBinary ? BinaryMode.ForceBinary : BinaryMode.Deserialize;
            var reader = marshaller.StartUnmarshal(stream, mode);

            var key = reader.ReadObject<TK>();
            var hasVal = stream.ReadBool();

            if (hasVal)
            {
                var val = reader.ReadObject<TV>();
                var part = stream.ReadInt();
                var ver = new AffinityTopologyVersion(stream.ReadLong(), stream.ReadInt());

                _map[key] = new PlatformCacheEntry<TV>(val, GetBoxedAffinityTopologyVersion(ver), part);
            }
            else
            {
                PlatformCacheEntry<TV> unused;
                _map.TryRemove(key, out unused);
            }
        }

        /** <inheritdoc /> */
        public void UpdateFromThreadLocal(int partition, AffinityTopologyVersion affinityTopologyVersion)
        {
            if (_stopped)
            {
                return;
            }

            var pair = (KeyValuePair<TK, TV>) PlatformCacheManager.ThreadLocalPair.Value;

            _map[pair.Key] = new PlatformCacheEntry<TV>(
                pair.Value,
                GetBoxedAffinityTopologyVersion(affinityTopologyVersion),
                partition);
        }

        /** <inheritdoc /> */
        public void Stop()
        {
            _stopped = true;
            Clear();
        }

        /** <inheritdoc /> */
        public void Clear()
        {
            _map.Clear();
        }

        /** <inheritdoc /> */
        public void SetThreadLocalPair<TKey, TVal>(TKey key, TVal val)
        {
            PlatformCacheManager.ThreadLocalPair.Value = new KeyValuePair<TK, TV>((TK) (object) key, (TV) (object) val);
        }

        /** <inheritdoc /> */
        public void ResetThreadLocalPair()
        {
            PlatformCacheManager.ThreadLocalPair.Value = null;
        }

        /** <inheritdoc /> */
        public IEnumerable<ICacheEntry<TKey, TVal>> GetEntries<TKey, TVal>(int? partition)
        {
            if (_stopped)
            {
                yield break;
            }

            foreach (var e in _map)
            {
                if (partition != null && e.Value.Partition != partition)
                {
                    continue;
                }

                if (!IsValid(e.Value))
                {
                    continue;
                }

                yield return new CacheEntry<TKey, TVal>((TKey) (object) e.Key, (TVal) (object) e.Value.Value);
            }
        }

        /// <summary>
        /// Checks whether specified cache entry is still valid, based on Affinity Topology Version.
        /// When primary node changes for a key, GridNearCacheEntry stops receiving updates for that key,
        /// because reader ("subscription") on new primary is not yet established.
        /// <para />
        /// This method is similar to GridNearCacheEntry.valid().
        /// </summary>
        /// <param name="entry">Entry to validate.</param>
        /// <typeparam name="TVal">Value type.</typeparam>
        /// <returns>True if entry is valid and can be returned to the user; false otherwise.</returns>
        private bool IsValid<TVal>(PlatformCacheEntry<TVal> entry)
        {
            // See comments on _affinityTopologyVersionFunc about boxed copy approach.
            var currentVerBoxed = _affinityTopologyVersionFunc();
            var entryVerBoxed = entry.Version;

            Debug.Assert(currentVerBoxed != null);

            if (ReferenceEquals(currentVerBoxed, entryVerBoxed))
            {
                // Happy path: true on stable topology.
                return true;
            }

            if (entryVerBoxed == null)
            {
                return false;
            }

            var entryVer = (AffinityTopologyVersion) entryVerBoxed;
            var currentVer = (AffinityTopologyVersion) currentVerBoxed;

            if (entryVer >= currentVer)
            {
                return true;
            }

            var part = entry.Partition;
            var valid = _affinity.IsAssignmentValid(entryVer, part);

            // Update version or mark as invalid (null).
            entry.CompareExchangeVersion(valid ? currentVerBoxed : null, entryVerBoxed);

            return valid;
        }

        /// <summary>
        /// Gets boxed affinity version. Reuses existing boxing copy to reduce allocations.
        /// </summary>
        private object GetBoxedAffinityTopologyVersion(AffinityTopologyVersion ver)
        {
            var currentVerBoxed = _affinityTopologyVersionFunc();
            return (AffinityTopologyVersion) currentVerBoxed == ver ? currentVerBoxed : ver;
        }
    }
}
