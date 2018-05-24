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

namespace Apache.Ignite.Core.Cache.Configuration
{
    using System.Collections.Generic;

    /// <summary>
    /// Partition loss policy. Defines how cache will behave in a case when one or more partitions are
    /// lost because of a node(s) failure.
    /// <para />
    /// All *Safe policies prevent a user from interaction with partial data in lost partitions until 
    /// <see cref="IIgnite.ResetLostPartitions(IEnumerable{string})"/> method is called.
    /// <para />
    /// *All policies allow working with partial data in lost partitions.
    /// <para />
    /// ReadOnly* and ReadWrite* policies do not automatically change partition state and thus do not change
    /// rebalancing assignments for such partitions.
    /// </summary>
    public enum PartitionLossPolicy
    {
        /// <summary>
        /// All writes to the cache will be failed with an exception, reads will only be allowed for keys in
        /// non-lost partitions. Reads from lost partitions will be failed with an exception.
        /// </summary>
        ReadOnlySafe,

        /// <summary>
        /// All writes to the cache will be failed with an exception. All reads will proceed as if all partitions
        /// were in a consistent state. The result of reading from a lost partition is undefined and may be different
        /// on different nodes in the cluster.
        /// </summary>
        ReadOnlyAll,

        /// <summary>
        /// All reads and writes will be allowed for keys in valid partitions. All reads and writes for keys
        /// in lost partitions will be failed with an exception.
        /// </summary>
        ReadWriteSafe,

        /// <summary>
        /// All reads and writes will proceed as if all partitions were in a consistent state. The result of
        /// reading from a lost partition is undefined and may be different on different nodes in the cluster.
        /// </summary>
        ReadWriteAll,

        /// <summary>
        /// If partition is lost, reset it's state and do not clear intermediate data. The result of reading from
        /// a previously lost and not cleared partition is undefined and may be different on different nodes in the
        /// cluster.
        /// </summary>
        Ignore
    }
}
