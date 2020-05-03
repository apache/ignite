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
    using Apache.Ignite.Core.Client;

    /// <summary>
    /// Client utils.
    /// </summary>
    internal static class ClientUtils
    {
        /** Bit mask of all features. */
        public static readonly byte[] AllFeatures = GetAllFeatures();
        
        /// <summary>
        /// Validates op code against current protocol version.
        /// </summary>
        /// <param name="operation">Operation.</param>
        /// <param name="protocolVersion">Protocol version.</param>
        public static void ValidateOp(ClientOp operation, ClientProtocolVersion protocolVersion)
        {
            ValidateOp(operation, protocolVersion, operation.GetMinVersion());
        }
        
        /// <summary>
        /// Validates op code against current protocol version.
        /// </summary>
        /// <param name="operation">Operation.</param>
        /// <param name="protocolVersion">Protocol version.</param>
        /// <param name="requiredProtocolVersion">Required protocol version.</param>
        public static void ValidateOp<T>(T operation, ClientProtocolVersion protocolVersion, 
            ClientProtocolVersion requiredProtocolVersion)
        {
            if (protocolVersion >= requiredProtocolVersion)
            {
                return;
            }

            var message = string.Format("Operation {0} is not supported by protocol version {1}. " +
                                        "Minimum protocol version required is {2}.", 
                operation, protocolVersion, requiredProtocolVersion);
                
            throw new IgniteClientException(message);
        }

        /// <summary>
        /// Gets a bit array with all supported features.
        /// </summary>
        private static byte[] GetAllFeatures()
        {
            var vals = Enum.GetValues(typeof(ClientBitmaskFeature));
            var bits = new BitArray(vals.Length);

            foreach (ClientBitmaskFeature feature in vals)
            {
                bits.Set((int)feature, true);
            }
            
            var bytes = new byte[1 + vals.Length / 8];
            bits.CopyTo(bytes, 0);

            return bytes;
        }
    }
}