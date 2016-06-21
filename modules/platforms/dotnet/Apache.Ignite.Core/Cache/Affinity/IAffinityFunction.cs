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
    using Apache.Ignite.Core.Cache.Affinity.Fair;
    using Apache.Ignite.Core.Cache.Affinity.Rendezvous;

    /// <summary>
    /// Represents a function that maps cache keys to cluster nodes.
    /// <para />
    /// Predefined implementations: 
    /// <see cref="RendezvousAffinityFunction"/>, <see cref="FairAffinityFunction"/>.
    /// </summary>
    public interface IAffinityFunction
    {
        /// <summary>
        ///  Resets cache affinity to its initial state. This method will be called by the system any time 
        /// the affinity has been sent to remote node where it has to be reinitialized.
        /// If your implementation of affinity function has no initialization logic, leave this method empty.
        /// </summary>
        void Reset();

        /// <summary>
        /// Gets the total number of partitions.
        /// <para />
        /// All caches should always provide correct partition count which should be the same on all 
        /// participating nodes. Note that partitions should always be numbered from 0 inclusively 
        /// to N exclusively without any gaps.
        /// </summary>
        int PartitionCount { get; }

        /// <summary>
        /// Gets partition number for a given key starting from 0. Partitioned caches
        /// should make sure that keys are about evenly distributed across all partitions
        /// from 0 to <see cref="PartitionCount"/> for best performance.
        /// <para />
        /// Note that for fully replicated caches it is possible to segment key sets among different
        /// grid node groups. In that case each node group should return a unique partition
        /// number. However, unlike partitioned cache, mappings of keys to nodes in
        /// replicated caches are constant and a node cannot migrate from one partition
        /// to another.
        /// </summary>
        /// <param name="key">Key to get partition for.</param>
        /// <returns>Partition number for a given key.</returns>
        int GetPartition(object key);

        /// <summary>
        /// Removes node from affinity. This method is called when it is safe to remove 
        /// disconnected node from affinity mapping.
        /// </summary>
        /// <param name="nodeId">The node identifier.</param>
        void RemoveNode(Guid nodeId);
    }
}