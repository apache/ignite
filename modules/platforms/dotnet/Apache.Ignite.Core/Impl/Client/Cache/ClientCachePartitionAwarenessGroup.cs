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

namespace Apache.Ignite.Core.Impl.Client.Cache
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using Apache.Ignite.Core.Impl.Binary;
    using Apache.Ignite.Core.Impl.Binary.IO;

    /// <summary>
    /// Partition mapping associated with the group of caches.
    /// Mirrors corresponding Java class.
    /// </summary>
    internal class ClientCachePartitionAwarenessGroup
    {
        /** */
        private readonly List<KeyValuePair<Guid, List<int>>> _partitionMap;

        /** */
        private readonly List<KeyValuePair<int, Dictionary<int, int>>> _caches;

        public ClientCachePartitionAwarenessGroup(IBinaryStream stream)
        {
            // Whether this group is eligible for client-side partition awareness.
            var applicable = stream.ReadBool();

            var cachesCount = stream.ReadInt();
            _caches = new List<KeyValuePair<int, Dictionary<int, int>>>(cachesCount);

            for (var i = 0; i < cachesCount; i++)
            {
                var cacheId = stream.ReadInt();
                if (!applicable)
                {
                    _caches.Add(new KeyValuePair<int, Dictionary<int, int>>(cacheId, null));
                    continue;
                }

                var keyCfgCount = stream.ReadInt();
                Dictionary<int, int> keyCfgs = null;
                if (keyCfgCount > 0)
                {
                    keyCfgs = new Dictionary<int, int>(keyCfgCount);
                    for (var j = 0; j < keyCfgCount; j++)
                    {
                        keyCfgs[stream.ReadInt()] = stream.ReadInt();
                    }
                }

                _caches.Add(new KeyValuePair<int, Dictionary<int, int>>(cacheId, keyCfgs));
            }

            if (!applicable)
                return;

            var partMapSize = stream.ReadInt();
            _partitionMap = new List<KeyValuePair<Guid, List<int>>>(partMapSize);

            var reader = BinaryUtils.Marshaller.StartUnmarshal(stream);

            for (var i = 0; i < partMapSize; i++)
            {
                var nodeId = reader.ReadGuid();
                Debug.Assert(nodeId != null);

                var partCount = stream.ReadInt();
                var parts = new List<int>(partCount);

                for (int j = 0; j < partCount; j++)
                {
                    parts.Add(stream.ReadInt());
                }

                _partitionMap.Add(new KeyValuePair<Guid, List<int>>(nodeId.Value, parts));
            }
        }

        /// <summary>
        /// Gets the caches.
        /// </summary>
        public ICollection<KeyValuePair<int, Dictionary<int, int>>> Caches
        {
            get { return _caches; }
        }

        /// <summary>
        /// Gets the partition map: node id -> partitions.
        /// </summary>
        public ICollection<KeyValuePair<Guid, List<int>>> PartitionMap
        {
            get { return _partitionMap; }
        }
    }
}
