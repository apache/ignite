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
    using System.Diagnostics;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Cluster;
    using Apache.Ignite.Core.Impl.Common;
    using Apache.Ignite.Core.Impl.Portable;
    using Apache.Ignite.Core.Impl.Portable.IO;
    using Apache.Ignite.Core.Impl.Unmanaged;
    using UU = Apache.Ignite.Core.Impl.Unmanaged.UnmanagedUtils;

    /// <summary>
    /// Cache affinity implementation.
    /// </summary>
    internal class CacheAffinityImpl : PlatformTarget, ICacheAffinity
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
        private readonly bool _keepPortable;
        
        /** Grid. */
        private readonly Ignite _ignite;

        /// <summary>
        /// Initializes a new instance of the <see cref="CacheAffinityImpl" /> class.
        /// </summary>
        /// <param name="target">Target.</param>
        /// <param name="marsh">Marshaller.</param>
        /// <param name="keepPortable">Keep portable flag.</param>
        /// <param name="ignite">Grid.</param>
        public CacheAffinityImpl(IUnmanagedTarget target, PortableMarshaller marsh, bool keepPortable, 
            Ignite ignite) : base(target, marsh)
        {
            _keepPortable = keepPortable;

            Debug.Assert(ignite != null);
            
            _ignite = ignite;
        }

        /** <inheritDoc /> */
        public int Partitions
        {
            get { return UU.AffinityPartitions(Target); }
        }

        /** <inheritDoc /> */
        public int Partition<TK>(TK key)
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
        public int[] PrimaryPartitions(IClusterNode n)
        {
            IgniteArgumentCheck.NotNull(n, "n");

            return DoOutInOp<Guid, int[]>(OpPrimaryPartitions, n.Id);
        }

        /** <inheritDoc /> */
        public int[] BackupPartitions(IClusterNode n)
        {
            IgniteArgumentCheck.NotNull(n, "n");

            return DoOutInOp<Guid, int[]>(OpBackupPartitions, n.Id);
        }

        /** <inheritDoc /> */
        public int[] AllPartitions(IClusterNode n)
        {
            IgniteArgumentCheck.NotNull(n, "n");

            return DoOutInOp<Guid, int[]>(OpAllPartitions, n.Id);
        }

        /** <inheritDoc /> */
        public TR AffinityKey<TK, TR>(TK key)
        {
            IgniteArgumentCheck.NotNull(key, "key");

            return DoOutInOp<TK, TR>(OpAffinityKey, key);
        }

        /** <inheritDoc /> */
        public IDictionary<IClusterNode, IList<TK>> MapKeysToNodes<TK>(IList<TK> keys)
        {
            IgniteArgumentCheck.NotNull(keys, "keys");

            return DoOutInOp(OpMapKeysToNodes, w => w.WriteObject(keys),
                reader => ReadDictionary(reader, ReadNode, r => r.ReadObject<IList<TK>>()));
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
        public IDictionary<int, IClusterNode> MapPartitionsToNodes(IList<int> parts)
        {
            IgniteArgumentCheck.NotNull(parts, "parts");

            return DoOutInOp(OpMapPartitionsToNodes,
                w => w.WriteObject(parts),
                reader => ReadDictionary(reader, r => r.ReadInt(), ReadNode));
        }

        /** <inheritDoc /> */
        public IList<IClusterNode> MapPartitionToPrimaryAndBackups(int part)
        {
            return DoOutInOp(OpMapPartitionToPrimaryAndBackups, w => w.WriteObject(part), r => ReadNodes(r));
        }

        /** <inheritDoc /> */
        protected override T Unmarshal<T>(IPortableStream stream)
        {
            return Marshaller.Unmarshal<T>(stream, _keepPortable);
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
        private IClusterNode ReadNode(PortableReaderImpl r)
        {
            return GetNode(r.ReadGuid());
        }

        /// <summary>
        /// Reads nodes from stream.
        /// </summary>
        private IList<IClusterNode> ReadNodes(IPortableStream reader)
        {
            return IgniteUtils.ReadNodes(Marshaller.StartUnmarshal(reader, _keepPortable));
        }

        /// <summary>
        /// Reads a dictionary from stream.
        /// </summary>
        private Dictionary<TK, TV> ReadDictionary<TK, TV>(IPortableStream reader, Func<PortableReaderImpl, TK> readKey,
            Func<PortableReaderImpl, TV> readVal)
        {
            var r = Marshaller.StartUnmarshal(reader, _keepPortable);

            var cnt = r.ReadInt();

            var dict = new Dictionary<TK, TV>(cnt);

            for (var i = 0; i < cnt; i++)
                dict[readKey(r)] = readVal(r);

            return dict;
        }
    }
}