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
    using Apache.Ignite.Core.Client;
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
        public ClientCluster(IgniteClient ignite)
            : base(ignite)
        {
            // No-op.
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
    }
}
