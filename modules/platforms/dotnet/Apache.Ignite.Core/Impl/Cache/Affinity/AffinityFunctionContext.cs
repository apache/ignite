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
    using System.Collections.Generic;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Cache.Affinity;
    using Apache.Ignite.Core.Cluster;
    using Apache.Ignite.Core.Events;

    /// <summary>
    /// Affinity function context.
    /// </summary>
    internal class AffinityFunctionContext : IAffinityFunctionContext
    {
        /** */
        private readonly List<List<IClusterNode>> _previousAssignment;

        /** */
        private readonly int _backups;

        /** */
        private readonly ICollection<IClusterNode> _currentTopologySnapshot;
        
        /** */
        private readonly AffinityTopologyVersion _currentTopologyVersion;
        
        /** */
        private readonly DiscoveryEvent _discoveryEvent;

        /// <summary>
        /// Initializes a new instance of the <see cref="AffinityFunctionContext"/> class.
        /// </summary>
        /// <param name="reader">The reader.</param>
        public AffinityFunctionContext(IBinaryRawReader reader)
        {
            var cnt = reader.ReadInt();

            if (cnt > 0)
            {
                _previousAssignment = new List<List<IClusterNode>>(cnt);

                for (var i = 0; i < cnt; i++)
                    _previousAssignment.Add(IgniteUtils.ReadNodes(reader));
            }

            _backups = reader.ReadInt();
            _currentTopologySnapshot = IgniteUtils.ReadNodes(reader);
            _currentTopologyVersion = new AffinityTopologyVersion(reader.ReadLong(), reader.ReadInt());
            _discoveryEvent = new DiscoveryEvent(reader);
        }

        /** <inheritdoc /> */
        public ICollection<IClusterNode> GetPreviousAssignment(int partition)
        {
            return _previousAssignment == null ? null : _previousAssignment[partition];
        }

        /** <inheritdoc /> */
        public int Backups
        {
            get { return _backups; }
        }

        /** <inheritdoc /> */
        public ICollection<IClusterNode> CurrentTopologySnapshot
        {
            get { return _currentTopologySnapshot; }
        }

        /** <inheritdoc /> */
        public AffinityTopologyVersion CurrentTopologyVersion
        {
            get { return _currentTopologyVersion; }
        }

        /** <inheritdoc /> */
        public DiscoveryEvent DiscoveryEvent
        {
            get { return _discoveryEvent; }
        }
    }
}
