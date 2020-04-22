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

namespace Apache.Ignite.Core.Impl.Client.Cluster
{
    using System;
    using System.Collections.Generic;
    using Apache.Ignite.Core.Client;
    using Apache.Ignite.Core.Impl.Binary;
    using Apache.Ignite.Core.Impl.Common;

    /// <summary>
    /// Ignite client cluster implementation.
    /// </summary>
    internal class ClientCluster : ClientClusterGroup, IClientCluster
    {
        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="ignite">Ignite.</param>
        /// <param name="marsh">Marshaller.</param>
        public ClientCluster(IgniteClient ignite, Marshaller marsh)
            : base(ignite, marsh)
        {
        }

        /** <inheritdoc /> */
        public void SetActive(bool isActive)
        {
            DoOutInOp<object>(ClientOp.ClusterChangeState, ctx => ctx.Stream.WriteBool(isActive), null);
        }

        /** <inheritdoc /> */
        public bool IsActive()
        {
            return DoOutInOp(ClientOp.ClusterIsActive, null, ctx => ctx.Stream.ReadBool());
        }

        /** <inheritdoc /> */
        public bool DisableWal(string cacheName)
        {
            IgniteArgumentCheck.NotNullOrEmpty(cacheName, "cacheName");

            Action<ClientRequestContext> action = ctx =>
            {
                ctx.Writer.WriteString(cacheName);
                ctx.Writer.WriteBoolean(false);
            };
            
            return DoOutInOp(ClientOp.ClusterChangeWalState, action, ctx => ctx.Stream.ReadBool());
        }

        /** <inheritdoc /> */
        public bool EnableWal(string cacheName)
        {
            IgniteArgumentCheck.NotNullOrEmpty(cacheName, "cacheName");

            Action<ClientRequestContext> action = ctx =>
            {
                ctx.Writer.WriteString(cacheName);
                ctx.Writer.WriteBoolean(true);
            };
            
            return DoOutInOp(ClientOp.ClusterChangeWalState, action, ctx => ctx.Stream.ReadBool());
        }

        /** <inheritdoc /> */
        public bool IsWalEnabled(string cacheName)
        {
            IgniteArgumentCheck.NotNullOrEmpty(cacheName, "cacheName");

            return DoOutInOp(ClientOp.ClusterGetWalState, ctx => ctx.Writer.WriteString(cacheName), ctx => ctx.Stream.ReadBool());
        }

        /// <summary>
        /// Gets endpoints for all known servers.
        /// </summary>
        internal IList<string> GetServerEndpoints()
        {
            // TODO: Optionally pass node ids.
            return DoOutInOp(ClientOp.ClusterGroupGetNodesEndpoints, 
                ctx => ctx.Writer.WriteGuidArray(new Guid?[0]),
                ctx => ctx.Reader.ReadStringArray());
        }
    }
}
