﻿/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

namespace Apache.Ignite.Core.Impl.Client
{
    using Apache.Ignite.Core.Client;

    /// <summary>
    /// Client utils.
    /// </summary>
    internal static class ClientUtils
    {
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
    }
}