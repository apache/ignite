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
    using System.ComponentModel;
    using Apache.Ignite.Core.Binary;

    /// <summary>
    /// Base class for predefined affinity functions.
    /// </summary>
    public abstract class AffinityFunctionBase : IAffinityFunction
    {
        /// <summary> The default value for <see cref="PartitionCount"/> property. </summary>
        public const int DefaultPartitionCount = 1024;

        /// <summary>
        /// Gets or sets the total number of partitions.
        /// </summary>
        [DefaultValue(DefaultPartitionCount)]
        public int PartitionCount { get; set; }

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
            // TODO
            return null;
        }

        /// <summary>
        /// Writes the instance.
        /// </summary>
        internal static void Write(IBinaryRawWriter writer, IAffinityFunction affinityFunction)
        {
            // TODO
        }
    }
}
