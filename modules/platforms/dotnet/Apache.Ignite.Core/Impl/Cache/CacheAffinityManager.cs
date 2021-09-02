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
    using Apache.Ignite.Core.Cache.Affinity;
    using Apache.Ignite.Core.Impl.Binary.IO;

    /// <summary>
    /// Affinity manager.
    /// </summary>
    internal class CacheAffinityManager : PlatformTargetAdapter
    {
        /** */
        private const int OpIsAssignmentValid = 1;

        /// <summary>
        /// Initializes a new instance of <see cref="CacheAffinityManager"/> class.
        /// </summary>
        /// <param name="target">Target.</param>
        internal CacheAffinityManager(IPlatformTargetInternal target) : base(target)
        {
            // No-op.
        }

        /// <summary>
        /// Checks whether given partition is still assigned to the same node as in specified version.
        /// </summary>
        internal bool IsAssignmentValid(AffinityTopologyVersion version, int partition)
        {
            return DoOutOp(OpIsAssignmentValid, (IBinaryStream s) =>
            {
                s.WriteLong(version.Version);
                s.WriteInt(version.MinorVersion);
                s.WriteInt(partition);
            }) != 0;
        }
    }
}
