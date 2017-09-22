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

namespace Apache.Ignite.Core.Impl.Cache.Affinity
{
    using System;
    using System.Collections.Generic;
    using Apache.Ignite.Core.Cache.Affinity;
    using Apache.Ignite.Core.Cluster;
    using Apache.Ignite.Core.Impl.Binary;
    using Apache.Ignite.Core.Impl.Unmanaged;

    /// <summary>
    /// Affinity function that delegates to Java.
    /// </summary>
    internal class PlatformAffinityFunction : PlatformTarget, IAffinityFunction
    {
        /** Opcodes. */
        private enum  Op
        {
            Partition = 1,
            RemoveNode = 2,
            AssignPartitions = 3
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="PlatformAffinityFunction"/> class.
        /// </summary>
        /// <param name="target">Target.</param>
        /// <param name="marsh">Marshaller.</param>
        public PlatformAffinityFunction(IUnmanagedTarget target, Marshaller marsh) : base(target, marsh)
        {
            // No-op.
        }

        /** <inheritdoc /> */
        public int Partitions
        {
            get { throw new NotSupportedException("PlatformAffinityFunction.Partitions is not supported."); }
        }

        /** <inheritdoc /> */
        public int GetPartition(object key)
        {
            return (int) DoOutOp((int) Op.Partition, w => w.WriteObject(key));
        }

        /** <inheritdoc /> */
        public void RemoveNode(Guid nodeId)
        {
            DoOutOp((int) Op.RemoveNode, w => w.WriteGuid(nodeId));
        }

        /** <inheritdoc /> */
        public IEnumerable<IEnumerable<IClusterNode>> AssignPartitions(AffinityFunctionContext context)
        {
            return DoInOp((int) Op.AssignPartitions, s => AffinityFunctionSerializer.ReadPartitions(s, Marshaller));
        }
    }
}
