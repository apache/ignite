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

namespace Apache.Ignite.Core.Impl.Cache
{
    using System;
    using System.Collections.Generic;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Cluster;
    using Apache.Ignite.Core.Impl.Binary;
    using Apache.Ignite.Core.Impl.Binary.IO;
    using Apache.Ignite.Core.Impl.Common;

    /// <summary>
    /// Cache affinity implementation.
    /// </summary>
    internal class CacheAffinityImpl : PlatformTargetAdapter, ICacheAffinity
    {
        /** */
        private const int OpAffinityKey = 1;

        /** */
        private const int OpAllPartitions = 2;

        /** */
        private const int OpBackupPartitions = 3;

        /** */
        private const int OpIsBackup = 4;

        /** */
        private const int OpIsPrimary = 5;

        /** */
        private const int OpIsPrimaryOrBackup = 6;

        /** */
        private const int OpMapKeyToNode = 7;

        /** */
        private const int OpMapKeyToPrimaryAndBackups = 8;

        /** */
        private const int OpMapKeysToNodes = 9;

        /** */
        private const int OpMapPartitionToNode = 10;

        /** */
        private const int OpMapPartitionToPrimaryAndBackups = 11;

        /** */
        private const int OpMapPartitionsToNodes = 12;

        /** */
        private const int OpPartition = 13;

        /** */
        private const int OpPrimaryPartitions = 14;

        /** */
        private const int OpPartitions = 15;

        /** */
        private readonly bool _keepBinary;

        /** Grid. */
        private readonly IIgniteInternal _ignite;

        /// <summary>
        /// Initializes a new instance of the <see cref="CacheAffinityImpl" /> class.
        /// </summary>
        /// <param name="target">Target.</param>
        /// <param name="keepBinary">Keep binary flag.</param>
        public CacheAffinityImpl(IPlatformTargetInternal target, bool keepBinary) : base(target)
        {
            _keepBinary = keepBinary;

            _ignite = target.Marshaller.Ignite;
        }

        /** <inheritDoc /> */
        public int Partitions
        {
            get { return (int) DoOutInOp(OpPartitions); }
        }

        /** <inheritDoc /> */
        public int GetPartition<TK>(TK key)
        {
            IgniteArgumentCheck.NotNull(key, "key");

            return (int)DoOutOp(OpPartition, key);
        }

        /** <inheritDoc /> */
        public bool IsPrimary<TK>(IClusterNode n, TK key)
        {
            IgniteArgumentCheck.NotNull(n, "n");

            IgniteArgumentCheck.NotNull(key, "key");

            return DoOutOp(OpIsPrimary, n.Id, key) == True;
        }

        /** <inheritDoc /> */
        public bool IsBackup<TK>(IClusterNode n, TK key)
        {
            IgniteArgumentCheck.NotNull(n, "n");

            IgniteArgumentCheck.NotNull(key, "key");

            return DoOutOp(OpIsBackup, n.Id, key) == True;
        }

        /** <inheritDoc /> */
        public bool IsPrimaryOrBackup<TK>(IClusterNode n, TK key)
        {
            IgniteArgumentCheck.NotNull(n, "n");

            IgniteArgumentCheck.NotNull(key, "key");

            return DoOutOp(OpIsPrimaryOrBackup, n.Id, key) == True;
        }

        /** <inheritDoc /> */
        public int[] GetPrimaryPartitions(IClusterNode n)
        {
            IgniteArgumentCheck.NotNull(n, "n");

            return DoOutInOp<Guid, int[]>(OpPrimaryPartitions, n.Id);
        }

        /** <inheritDoc /> */
        public int[] GetBackupPartitions(IClusterNode n)
        {
            IgniteArgumentCheck.NotNull(n, "n");

            return DoOutInOp<Guid, int[]>(OpBackupPartitions, n.Id);
        }

        /** <inheritDoc /> */
        public int[] GetAllPartitions(IClusterNode n)
        {
            IgniteArgumentCheck.NotNull(n, "n");

            return DoOutInOp<Guid, int[]>(OpAllPartitions, n.Id);
        }

        /** <inheritDoc /> */
        public TR GetAffinityKey<TK, TR>(TK key)
        {
            IgniteArgumentCheck.NotNull(key, "key");

            return DoOutInOp<TK, TR>(OpAffinityKey, key);
        }

        /** <inheritDoc /> */
        public IDictionary<IClusterNode, IList<TK>> MapKeysToNodes<TK>(IEnumerable<TK> keys)
        {
            IgniteArgumentCheck.NotNull(keys, "keys");

            return DoOutInOp(OpMapKeysToNodes, w => w.WriteEnumerable(keys),
                reader => ReadDictionary(reader, ReadNode, r => (IList<TK>) r.ReadCollectionAsList<TK>()));
        }

        /** <inheritDoc /> */
        public IClusterNode MapKeyToNode<TK>(TK key)
        {
            IgniteArgumentCheck.NotNull(key, "key");

            return GetNode(DoOutInOp<TK, Guid?>(OpMapKeyToNode, key));
        }

        /** <inheritDoc /> */
        public IList<IClusterNode> MapKeyToPrimaryAndBackups<TK>(TK key)
        {
            IgniteArgumentCheck.NotNull(key, "key");

            return DoOutInOp(OpMapKeyToPrimaryAndBackups, w => w.WriteObject(key), r => ReadNodes(r));
        }

        /** <inheritDoc /> */
        public IClusterNode MapPartitionToNode(int part)
        {
            return GetNode(DoOutInOp<int, Guid?>(OpMapPartitionToNode, part));
        }

        /** <inheritDoc /> */
        public IDictionary<int, IClusterNode> MapPartitionsToNodes(IEnumerable<int> parts)
        {
            IgniteArgumentCheck.NotNull(parts, "parts");

            return DoOutInOp(OpMapPartitionsToNodes,
                w => w.WriteEnumerable(parts),
                reader => ReadDictionary(reader, r => r.ReadInt(), ReadNode));
        }

        /** <inheritDoc /> */
        public IList<IClusterNode> MapPartitionToPrimaryAndBackups(int part)
        {
            return DoOutInOp(OpMapPartitionToPrimaryAndBackups, w => w.WriteObject(part), r => ReadNodes(r));
        }

        /** <inheritDoc /> */
        protected override T Unmarshal<T>(IBinaryStream stream)
        {
            return Marshaller.Unmarshal<T>(stream, _keepBinary);
        }

        /// <summary>
        /// Gets the node by id.
        /// </summary>
        /// <param name="id">The id.</param>
        /// <returns>Node.</returns>
        private IClusterNode GetNode(Guid? id)
        {
            return _ignite.GetNode(id);
        }

        /// <summary>
        /// Reads a node from stream.
        /// </summary>
        private IClusterNode ReadNode(BinaryReader r)
        {
            return GetNode(r.ReadGuid());
        }

        /// <summary>
        /// Reads nodes from stream.
        /// </summary>
        private IList<IClusterNode> ReadNodes(IBinaryStream reader)
        {
            return IgniteUtils.ReadNodes(Marshaller.StartUnmarshal(reader, _keepBinary));
        }

        /// <summary>
        /// Reads a dictionary from stream.
        /// </summary>
        private Dictionary<TK, TV> ReadDictionary<TK, TV>(IBinaryStream reader, Func<BinaryReader, TK> readKey,
            Func<BinaryReader, TV> readVal)
        {
            var r = Marshaller.StartUnmarshal(reader, _keepBinary);

            var cnt = r.ReadInt();

            var dict = new Dictionary<TK, TV>(cnt);

            for (var i = 0; i < cnt; i++)
                dict[readKey(r)] = readVal(r);

            return dict;
        }
    }
}
