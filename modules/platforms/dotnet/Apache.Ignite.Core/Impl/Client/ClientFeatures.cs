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

namespace Apache.Ignite.Core.Impl.Client
{
    using System.Collections;
    using System.Collections.Generic;
    using System.Diagnostics;

    /// <summary>
    /// Handles client features based on protocol version and feature flags.
    /// </summary>
    internal class ClientFeatures
    {
        /** */
        private static readonly Dictionary<ClientOp, ClientProtocolVersion> OpVersion =
            new Dictionary<ClientOp, ClientProtocolVersion>
            {
                {ClientOp.CachePartitions, new ClientProtocolVersion(1, 4, 0)},
                {ClientOp.ClusterIsActive, new ClientProtocolVersion(1, 5, 0)},
                {ClientOp.ClusterChangeState, new ClientProtocolVersion(1, 5, 0)},
                {ClientOp.ClusterChangeWalState, new ClientProtocolVersion(1, 5, 0)},
                {ClientOp.ClusterGetWalState, new ClientProtocolVersion(1, 5, 0)},
                {ClientOp.ClusterGroupGetNodeIds, new ClientProtocolVersion(1, 5, 0)},
                {ClientOp.ClusterGroupGetNodesInfo, new ClientProtocolVersion(1, 5, 0)},
            };
        
        /** */
        private static readonly Dictionary<ClientOp, ClientBitmaskFeature> OpFeature = 
            new Dictionary<ClientOp, ClientBitmaskFeature>
        {
            {ClientOp.ClusterGroupGetNodesEndpoints, ClientBitmaskFeature.ClusterGroupGetNodesEndpoints}
        };
        
        /** */
        private readonly ClientProtocolVersion _protocolVersion;

        /** */
        private readonly BitArray _features;

        /// <summary>
        /// Initializes a new instance of <see cref="ClientFeatures"/>. 
        /// </summary>
        public ClientFeatures(ClientProtocolVersion protocolVersion, BitArray features)
        {
            // TODO: Move all logic to this class, including mapping from version to op, from feature to op.
            // TODO: Move ValidateOp methods here.
            Debug.Assert(features != null);
            
            _protocolVersion = protocolVersion;
            _features = features;
        }

        public bool HasFeature(ClientBitmaskFeature feature)
        {
            var index = (int) feature;

            return index >= 0 && index < _features.Count && _features.Get(index);
        }

        public bool HasOp(ClientOp op)
        {
            // TODO
            return false;
        }
    }
}