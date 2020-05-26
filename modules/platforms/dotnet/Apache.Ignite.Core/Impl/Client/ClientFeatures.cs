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
                {ClientOp.ClusterGetWalState, new ClientProtocolVersion(1, 5, 0)}
            };

        /** */
        private static readonly Dictionary<ClientOp, ClientBitmaskFeature> OpFeature =
            new Dictionary<ClientOp, ClientBitmaskFeature>
            {
                {ClientOp.ClusterGroupGetNodesEndpoints, ClientBitmaskFeature.ClusterGroupGetNodesEndpoints},
                {ClientOp.ComputeTaskExecute, ClientBitmaskFeature.ExecuteTaskByName},
                {ClientOp.ClusterGroupGetNodeIds, ClientBitmaskFeature.ClusterGroups},
                {ClientOp.ClusterGroupGetNodesInfo, ClientBitmaskFeature.ClusterGroups}
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

        /// <summary>
        /// Returns a value indicating whether specified feature is supported.
        /// </summary>
        public bool HasFeature(ClientBitmaskFeature feature)
        {
            var index = (int) feature;

            return _features != null && index >= 0 && index < _features.Count && _features.Get(index);
        }

        /// <summary>
        /// Returns a value indicating whether specified operation is supported.
        /// </summary>
        public bool HasOp(ClientOp op)
        {
            return ValidateOp(op, false);
        }

        /// <summary>
        /// Checks whether WithExpiryPolicy request flag is supported. Throws an exception when not supported. 
        /// </summary>
        public void ValidateWithExpiryPolicyFlag()
        {
            var requiredVersion = ClientSocket.Ver150;

            if (_protocolVersion < requiredVersion)
            {
                ThrowMinimumVersionException("WithExpiryPolicy", requiredVersion);
            }
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
        /// Validates specified op code against current protocol version and features.
        /// </summary>
        /// <param name="operation">Operation.</param>
        public void ValidateOp(ClientOp operation)
        {
            ValidateOp(operation, true);
        }
        
        /// <summary>
        /// Validates specified op code against current protocol version and features.
        /// </summary>
        private bool ValidateOp(ClientOp operation, bool shouldThrow)
        {
            var requiredProtocolVersion = GetMinVersion(operation);
            
            if (_protocolVersion < requiredProtocolVersion)
            {
                if (shouldThrow)
                {
                    ThrowMinimumVersionException(operation, requiredProtocolVersion);
                }

                return false;
            }

            var requiredFeature = GetFeature(operation);

            if (requiredFeature != null && (_features == null || !_features.Get((int) requiredFeature.Value)))
            {
                if (shouldThrow)
                {
                    throw new IgniteClientException(string.Format(
                        "Operation {0} is not supported by the server. Feature {1} is missing.",
                        operation, requiredFeature.Value));
                }

                return false;
            }

            return true;
        }

        /// <summary>
        /// Throws minimum version exception.
        /// </summary>
        private void ThrowMinimumVersionException(object operation, ClientProtocolVersion requiredProtocolVersion)
        {
            var message = string.Format("Operation {0} is not supported by protocol version {1}. " +
                                        "Minimum protocol version required is {2}.",
                operation, _protocolVersion, requiredProtocolVersion);

            throw new IgniteClientException(message);
        }

        /// <summary>
        /// Gets <see cref="ClientBitmaskFeature"/> that is required to perform specified operation.
        /// </summary>
        /// <param name="op">Operation.</param>
        /// <returns>Required feature flag, or null.</returns>
        private static ClientBitmaskFeature? GetFeature(ClientOp op)
        {
            ClientBitmaskFeature feature;

            return OpFeature.TryGetValue(op, out feature)
                ? feature
                : (ClientBitmaskFeature?) null;
        }

        /// <summary>
        /// Gets a bit array with all supported features.
        /// </summary>
        private static byte[] GetAllFeatures()
        {
            var values = Enum.GetValues(typeof(ClientBitmaskFeature))
                .Cast<int>()
                .ToArray();

            var bits = new BitArray(values.Max() + 1);

            foreach (var feature in values)
            {
                bits.Set(feature, true);
            }
            
            var bytes = new byte[1 + values.Length / 8];
            bits.CopyTo(bytes, 0);

            return bytes;
        }
    }
}