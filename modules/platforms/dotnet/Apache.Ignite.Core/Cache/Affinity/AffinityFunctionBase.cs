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

namespace Apache.Ignite.Core.Cache.Affinity
{
    using System;
    using System.Collections.Generic;
    using System.ComponentModel;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Cache.Affinity.Fair;
    using Apache.Ignite.Core.Cache.Affinity.Rendezvous;
    using Apache.Ignite.Core.Cluster;
    using Apache.Ignite.Core.Common;

    /// <summary>
    /// Base class for predefined affinity functions.
    /// </summary>
    public abstract class AffinityFunctionBase : IAffinityFunction
    {
        /** */
        private const byte TypeCodeNull = 0;

        /** */
        private const byte TypeCodeFair = 1;

        /** */
        private const byte TypeCodeRendezvous = 2;

        /** */
        private const byte TypeCodeUser = 3;

        /// <summary> The default value for <see cref="PartitionCount"/> property. </summary>
        public const int DefaultPartitionCount = 1024;

        /// <summary>
        /// Gets or sets the total number of partitions.
        /// </summary>
        [DefaultValue(DefaultPartitionCount)]
        public int PartitionCount { get; set; }

        /// <summary>
        /// Gets partition number for a given key starting from 0. Partitioned caches
        /// should make sure that keys are about evenly distributed across all partitions
        /// from 0 to <see cref="PartitionCount" /> for best performance.
        /// <para />
        /// Note that for fully replicated caches it is possible to segment key sets among different
        /// grid node groups. In that case each node group should return a unique partition
        /// number. However, unlike partitioned cache, mappings of keys to nodes in
        /// replicated caches are constant and a node cannot migrate from one partition
        /// to another.
        /// </summary>
        /// <param name="key">Key to get partition for.</param>
        /// <returns>
        /// Partition number for a given key.
        /// </returns>
        public int GetPartition(object key)
        {
            throw GetDirectUsageError();
        }

        /// <summary>
        /// Removes node from affinity. This method is called when it is safe to remove
        /// disconnected node from affinity mapping.
        /// </summary>
        /// <param name="nodeId">The node identifier.</param>
        public void RemoveNode(Guid nodeId)
        {
            throw GetDirectUsageError();
        }

        /// <summary>
        /// Gets affinity nodes for a partition. In case of replicated cache, all returned
        /// nodes are updated in the same manner. In case of partitioned cache, the returned
        /// list should contain only the primary and back up nodes with primary node being
        /// always first.
        /// <pare />
        /// Note that partitioned affinity must obey the following contract: given that node
        /// <code>N</code> is primary for some key <code>K</code>, if any other node(s) leave
        /// grid and no node joins grid, node <code>N</code> will remain primary for key <code>K</code>.
        /// </summary>
        /// <param name="context">The affinity function context.</param>
        /// <returns>
        /// A collection of partitions, where each partition is a collection of nodes,
        /// where first node is a primary node, and other nodes are backup nodes.
        /// </returns>
        public IEnumerable<IEnumerable<IClusterNode>> AssignPartitions(IAffinityFunctionContext context)
        {
            throw GetDirectUsageError();
        }

        /// <summary>
        /// Resets cache affinity to its initial state. This method will be called by the system any time
        /// the affinity has been sent to remote node where it has to be reinitialized.
        /// If your implementation of affinity function has no initialization logic, leave this method empty.
        /// </summary>
        public void Reset()
        {
            throw GetDirectUsageError();
        }

        /// <summary>
        /// Gets or sets a value indicating whether to exclude same-host-neighbors from being backups of each other.
        /// </summary>
        public bool ExcludeNeighbors { get; set; }

        /// <summary>
        /// Initializes a new instance of the <see cref="AffinityFunctionBase"/> class.
        /// </summary>
        internal AffinityFunctionBase()
        {
            PartitionCount = DefaultPartitionCount;
        }

        /// <summary>
        /// Reads the instance.
        /// </summary>
        internal static IAffinityFunction Read(IBinaryRawReader reader)
        {
            AffinityFunctionBase fun;

            var typeCode = reader.ReadByte();
            switch (typeCode)
            {
                case TypeCodeNull:
                    return null;
                case TypeCodeFair:
                    fun = new FairAffinityFunction();
                    break;
                case TypeCodeRendezvous:
                    fun = new RendezvousAffinityFunction();
                    break;
                case TypeCodeUser:
                    // TODO: Deserialize
                    return null;
                default:
                    throw new InvalidOperationException("Invalid AffinityFunction type code: " + typeCode);
            }

            fun.PartitionCount = reader.ReadInt();
            fun.ExcludeNeighbors = reader.ReadBoolean();

            return fun;
        }

        /// <summary>
        /// Writes the instance.
        /// </summary>
        internal static void Write(IBinaryRawWriter writer, IAffinityFunction fun)
        {
            if (fun == null)
            {
                writer.WriteByte(TypeCodeNull);
                return;
            }

            var p = fun as AffinityFunctionBase;

            if (p != null)
            {
                writer.WriteByte(p is FairAffinityFunction ? TypeCodeFair : TypeCodeRendezvous);
                writer.WriteInt(p.PartitionCount);
                writer.WriteBoolean(p.ExcludeNeighbors);
            }
            else
            {
                writer.WriteByte(TypeCodeUser);

                // TODO: This won't work on startup?
                writer.WriteObject(fun);
            }
        }

        /// <summary>
        /// Gets the direct usage error.
        /// </summary>
        private Exception GetDirectUsageError()
        {
            return new IgniteException(GetType() + " can not be used directly.");
        }
    }
}
