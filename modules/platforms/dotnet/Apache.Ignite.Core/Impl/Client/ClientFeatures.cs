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
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.Linq;
    using Apache.Ignite.Core.Cache.Configuration;
    using Apache.Ignite.Core.Client;

    /// <summary>
    /// Handles client features based on protocol version and feature flags.
    /// </summary>
    internal class ClientFeatures
    {
        /** Bit mask of all features. */
        public static readonly byte[] AllFeatures = GetAllFeatures();

        /** Current features. */
        public static readonly ClientFeatures CurrentFeatures =
            new ClientFeatures(ClientSocket.CurrentProtocolVersion, new BitArray(AllFeatures));

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
            _protocolVersion = protocolVersion;
            _features = features;
        }

        public bool HasFeature(ClientBitmaskFeature feature)
        {
            var index = (int) feature;

            return _features != null && index >= 0 && index < _features.Count && _features.Get(index);
        }

        public bool HasOp(ClientOp op)
        {
            // TODO
            return false;
        }

        /// <summary>
        /// Returns a value indicating whether WithExpiryPolicy request flag is supported.
        /// </summary>
        public bool HasWithExpiryPolicyFlag()
        {
            return _protocolVersion >= ClientSocket.Ver150;
        }

        /// <summary>
        /// Returns a value indicating whether <see cref="QueryField.Precision"/> and <see cref="QueryField.Scale"/>
        /// are supported.
        /// </summary>
        public bool HasQueryFieldPrecisionAndScale()
        {
            return _protocolVersion >= ClientSocket.Ver120;
        }
        
        /// <summary>
        /// Returns a value indicating whether <see cref="CacheConfiguration.ExpiryPolicyFactory"/> is supported.
        /// </summary>
        public bool HasCacheConfigurationExpiryPolicyFactory()
        {
            return _protocolVersion >= ClientSocket.Ver160;
        }
        
        /// <summary>
        /// Gets minimum protocol version that is required to perform specified operation.
        /// </summary>
        /// <param name="op">Operation.</param>
        /// <returns>Minimum protocol version.</returns>
        public static ClientProtocolVersion GetMinVersion(ClientOp op)
        {
            ClientProtocolVersion minVersion;
            
            return OpVersion.TryGetValue(op, out minVersion) 
                ? minVersion 
                : ClientSocket.Ver100;
        }

        /// <summary>
        /// Gets <see cref="ClientBitmaskFeature"/> that is required to perform specified operation.
        /// </summary>
        /// <param name="op">Operation.</param>
        /// <returns>Required feature flag, or null.</returns>
        public static ClientBitmaskFeature? GetFeature(ClientOp op)
        {
            ClientBitmaskFeature feature;

            return OpFeature.TryGetValue(op, out feature)
                ? feature
                : (ClientBitmaskFeature?) null;
        }

        
        /// <summary>
        /// Validates op code against current protocol version.
        /// </summary>
        /// <param name="operation">Operation.</param>
        public void ValidateOp(ClientOp operation)
        {
            ValidateOp(operation, _protocolVersion , GetMinVersion(operation), _features, GetFeature(operation));
        }
        
        /// <summary>
        /// Validates op code against current protocol version.
        /// </summary>
        private static void ValidateOp<T>(T operation, ClientProtocolVersion protocolVersion, 
            ClientProtocolVersion requiredProtocolVersion, BitArray features, ClientBitmaskFeature? requiredFeature)
        {
            if (protocolVersion < requiredProtocolVersion)
            {
                var message = string.Format("Operation {0} is not supported by protocol version {1}. " +
                                            "Minimum protocol version required is {2}.", 
                    operation, protocolVersion, requiredProtocolVersion);
                
                throw new IgniteClientException(message);
            }

            if (features != null && requiredFeature != null && !features.Get((int) requiredFeature.Value))
            {
                throw new IgniteClientException(string.Format(
                    "Operation {0} is not supported by the server. Feature {1} is missing.",
                    operation, requiredFeature.Value));
            }
        }

        /// <summary>
        /// Gets a bit array with all supported features.
        /// </summary>
        private static byte[] GetAllFeatures()
        {
            var vals = Enum.GetValues(typeof(ClientBitmaskFeature))
                .Cast<int>()
                .ToArray();

            var bits = new BitArray(vals.Max() + 1);

            foreach (var feature in vals)
            {
                bits.Set(feature, true);
            }
            
            var bytes = new byte[1 + vals.Length / 8];
            bits.CopyTo(bytes, 0);

            return bytes;
        }
    }
}