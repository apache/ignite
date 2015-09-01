/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Impl.Cache
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using Apache.Ignite.Core.Impl.Portable.IO;
    using GridGain.Cache;
    using GridGain.Cluster;
    using GridGain.Impl.Portable;
    using GridGain.Impl.Unmanaged;

    using A = Apache.Ignite.Core.Impl.Common.GridArgumentCheck;
    using UU = GridGain.Impl.Unmanaged.UnmanagedUtils;
    using U = GridGain.Impl.GridUtils;

    /// <summary>
    /// Cache affinity implementation.
    /// </summary>
    internal class CacheAffinityImpl : GridTarget, ICacheAffinity
    {
        /** */
        private const int OP_AFFINITY_KEY = 1;

        /** */
        private const int OP_ALL_PARTITIONS = 2;

        /** */
        private const int OP_BACKUP_PARTITIONS = 3;

        /** */
        private const int OP_IS_BACKUP = 4;

        /** */
        private const int OP_IS_PRIMARY = 5;

        /** */
        private const int OP_IS_PRIMARY_OR_BACKUP = 6;

        /** */
        private const int OP_MAP_KEY_TO_NODE = 7;

        /** */
        private const int OP_MAP_KEY_TO_PRIMARY_AND_BACKUPS = 8;

        /** */
        private const int OP_MAP_KEYS_TO_NODES = 9;

        /** */
        private const int OP_MAP_PARTITION_TO_NODE = 10;

        /** */
        private const int OP_MAP_PARTITION_TO_PRIMARY_AND_BACKUPS = 11;

        /** */
        private const int OP_MAP_PARTITIONS_TO_NODES = 12;

        /** */
        private const int OP_PARTITION = 13;

        /** */
        private const int OP_PRIMARY_PARTITIONS = 14;

        /** */
        private readonly bool keepPortable;
        
        /** Grid. */
        private readonly GridImpl grid;

        /// <summary>
        /// Initializes a new instance of the <see cref="CacheAffinityImpl" /> class.
        /// </summary>
        /// <param name="target">Target.</param>
        /// <param name="marsh">Marshaller.</param>
        /// <param name="keepPortable">Keep portable flag.</param>
        /// <param name="grid">Grid.</param>
        public CacheAffinityImpl(IUnmanagedTarget target, PortableMarshaller marsh, bool keepPortable, 
            GridImpl grid)
            : base(target, marsh)
        {
            this.keepPortable = keepPortable;

            Debug.Assert(grid != null);
            
            this.grid = grid;
        }

        /** <inheritDoc /> */
        public int Partitions
        {
            get { return UU.AffinityPartitions(target); }
        }

        /** <inheritDoc /> */
        public int Partition<K>(K key)
        {
            A.NotNull(key, "key");

            return (int)DoOutOp(OP_PARTITION, key);
        }

        /** <inheritDoc /> */
        public bool IsPrimary<K>(IClusterNode n, K key)
        {
            A.NotNull(n, "n");
            
            A.NotNull(key, "key");

            return DoOutOp(OP_IS_PRIMARY, n.Id, key) == TRUE;
        }

        /** <inheritDoc /> */
        public bool IsBackup<K>(IClusterNode n, K key)
        {
            A.NotNull(n, "n");

            A.NotNull(key, "key");

            return DoOutOp(OP_IS_BACKUP, n.Id, key) == TRUE;
        }

        /** <inheritDoc /> */
        public bool IsPrimaryOrBackup<K>(IClusterNode n, K key)
        {
            A.NotNull(n, "n");

            A.NotNull(key, "key");

            return DoOutOp(OP_IS_PRIMARY_OR_BACKUP, n.Id, key) == TRUE;
        }

        /** <inheritDoc /> */
        public int[] PrimaryPartitions(IClusterNode n)
        {
            A.NotNull(n, "n");

            return DoOutInOp<Guid, int[]>(OP_PRIMARY_PARTITIONS, n.Id);
        }

        /** <inheritDoc /> */
        public int[] BackupPartitions(IClusterNode n)
        {
            A.NotNull(n, "n");

            return DoOutInOp<Guid, int[]>(OP_BACKUP_PARTITIONS, n.Id);
        }

        /** <inheritDoc /> */
        public int[] AllPartitions(IClusterNode n)
        {
            A.NotNull(n, "n");

            return DoOutInOp<Guid, int[]>(OP_ALL_PARTITIONS, n.Id);
        }

        /** <inheritDoc /> */
        public R AffinityKey<K, R>(K key)
        {
            A.NotNull(key, "key");

            return DoOutInOp<K, R>(OP_AFFINITY_KEY, key);
        }

        /** <inheritDoc /> */
        public IDictionary<IClusterNode, IList<K>> MapKeysToNodes<K>(IList<K> keys)
        {
            A.NotNull(keys, "keys");

            return DoOutInOp(OP_MAP_KEYS_TO_NODES, w => w.WriteObject(keys),
                reader => ReadDictionary(reader, ReadNode, r => r.ReadObject<IList<K>>()));
        }

        /** <inheritDoc /> */
        public IClusterNode MapKeyToNode<K>(K key)
        {
            A.NotNull(key, "key");

            return GetNode(DoOutInOp<K, Guid?>(OP_MAP_KEY_TO_NODE, key));
        }

        /** <inheritDoc /> */
        public IList<IClusterNode> MapKeyToPrimaryAndBackups<K>(K key)
        {
            A.NotNull(key, "key");

            return DoOutInOp(OP_MAP_KEY_TO_PRIMARY_AND_BACKUPS, w => w.WriteObject(key), r => ReadNodes(r));
        }

        /** <inheritDoc /> */
        public IClusterNode MapPartitionToNode(int part)
        {
            return GetNode(DoOutInOp<int, Guid?>(OP_MAP_PARTITION_TO_NODE, part));
        }

        /** <inheritDoc /> */
        public IDictionary<int, IClusterNode> MapPartitionsToNodes(IList<int> parts)
        {
            A.NotNull(parts, "parts");

            return DoOutInOp(OP_MAP_PARTITIONS_TO_NODES,
                w => w.WriteObject(parts),
                reader => ReadDictionary(reader, r => r.ReadInt(), ReadNode));
        }

        /** <inheritDoc /> */
        public IList<IClusterNode> MapPartitionToPrimaryAndBackups(int part)
        {
            return DoOutInOp(OP_MAP_PARTITION_TO_PRIMARY_AND_BACKUPS, w => w.WriteObject(part), r => ReadNodes(r));
        }

        /** <inheritDoc /> */
        protected override T Unmarshal<T>(IPortableStream stream)
        {
            return Marshaller.Unmarshal<T>(stream, keepPortable);
        }


        /// <summary>
        /// Gets the node by id.
        /// </summary>
        /// <param name="id">The id.</param>
        /// <returns>Node.</returns>
        private IClusterNode GetNode(Guid? id)
        {
            return grid.GetNode(id);
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
            return U.ReadNodes(Marshaller.StartUnmarshal(reader, keepPortable));
        }

        /// <summary>
        /// Reads a dictionary from stream.
        /// </summary>
        private Dictionary<K, V> ReadDictionary<K, V>(IPortableStream reader, Func<PortableReaderImpl, K> readKey,
            Func<PortableReaderImpl, V> readVal)
        {
            var r = Marshaller.StartUnmarshal(reader, keepPortable);

            var cnt = r.ReadInt();

            var dict = new Dictionary<K, V>(cnt);

            for (var i = 0; i < cnt; i++)
                dict[readKey(r)] = readVal(r);

            return dict;
        }
    }
}