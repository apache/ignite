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
    using System.Collections.Generic;
    using System.Linq;

    /// <summary>
    /// Extension methods for <see cref="ClientOp"/>.
    /// </summary>
    internal static class ClientOpExtensions
    {
        /** */
        private static readonly Dictionary<ClientOp, ClientProtocolVersion> VersionMap = GetVersionMap();
        
        /** */
        private static readonly HashSet<ClientOp> TransactionalOpCodes = GetTransactionalOpCodes();

        /// <summary>
        /// Gets minimum protocol version that is required to perform specified operation.
        /// </summary>
        /// <param name="op">Operation.</param>
        /// <returns>Minimum protocol version.</returns>
        public static ClientProtocolVersion GetMinVersion(this ClientOp op)
        {
            ClientProtocolVersion minVersion;
            
            return VersionMap.TryGetValue(op, out minVersion) 
                ? minVersion 
                : ClientSocket.Ver100;
        }

        public static bool IsTransactional(this ClientOp op)
        {
            return TransactionalOpCodes.Contains(op);
        }

        /// <summary>
        /// Gets the version map.
        /// </summary>
        private static Dictionary<ClientOp, ClientProtocolVersion> GetVersionMap()
        {
            var res = new Dictionary<ClientOp, ClientProtocolVersion>();
            
            foreach (var memberInfo in typeof(ClientOp).GetMembers())
            {
                var attr = memberInfo.GetCustomAttributes(false)
                    .OfType<MinVersionAttribute>()
                    .SingleOrDefault();

                if (attr == null)
                {
                    continue;
                }

                var clientOp = (ClientOp) Enum.Parse(typeof(ClientOp), memberInfo.Name);
                res[clientOp] = attr.Version;
            }

            return res;
        }

        // TODO: combine with GetVersionMap
        /// <summary>
        /// Gets transactional operations codes
        /// </summary>
        private static HashSet<ClientOp> GetTransactionalOpCodes()
        {
            var res = new HashSet<ClientOp>();
            
            foreach (var memberInfo in typeof(ClientOp).GetMembers())
            {
                var attr = memberInfo.GetCustomAttributes(false)
                   .OfType<MinVersionAttribute>()
                   .SingleOrDefault();

                if (attr == null)
                {
                    continue;
                }

                var clientOp = (ClientOp) Enum.Parse(typeof(ClientOp), memberInfo.Name);
                res.Add(clientOp);
            }

            return res;
        }
    }
}