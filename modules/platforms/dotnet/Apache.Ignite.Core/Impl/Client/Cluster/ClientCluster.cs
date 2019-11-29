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
    using System.Diagnostics;
    using Apache.Ignite.Core.Client;
    using Apache.Ignite.Core.Impl.Binary;
    using Apache.Ignite.Core.Impl.Binary.IO;
    using Apache.Ignite.Core.Impl.Common;

    /// <summary>
    /// Ignite client cluster implementation.
    /// </summary>
    internal class ClientCluster : IClientCluster
    {
        /** Ignite. */
        private readonly IgniteClient _ignite;

        /** Marshaller. */
        private readonly Marshaller _marsh;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="ignite">Ignite.</param>
        /// <param name="marsh">Marshaller.</param>
        public ClientCluster(IgniteClient ignite, Marshaller marsh)
        {
            Debug.Assert(ignite != null);
            Debug.Assert(marsh != null);

            _ignite = ignite;
            _marsh = marsh;
        }

        /** <inheritdoc /> */
        public void SetActive(bool isActive)
        {
            DoOutInOp<object>(ClientOp.ClusterChangeState, w => w.WriteBoolean(isActive), null);
        }

        /** <inheritdoc /> */
        public bool IsActive()
        {
            return DoOutInOp(ClientOp.ClusterIsActive, null, r => r.ReadBool());
        }

        /** <inheritdoc /> */
        public bool DisableWal(string cacheName)
        {
            IgniteArgumentCheck.NotNullOrEmpty(cacheName, "cacheName");

            Action<BinaryWriter> action = w =>
            {
                w.WriteString(cacheName);
                w.WriteBoolean(false);
            };
            return DoOutInOp(ClientOp.ClusterChangeWalState, action, r => r.ReadBool());
        }

        /** <inheritdoc /> */
        public bool EnableWal(string cacheName)
        {
            IgniteArgumentCheck.NotNullOrEmpty(cacheName, "cacheName");

            Action<BinaryWriter> action = w =>
            {
                w.WriteString(cacheName);
                w.WriteBoolean(true);
            };
            return DoOutInOp(ClientOp.ClusterChangeWalState, action, r => r.ReadBool());
        }

        /** <inheritdoc /> */
        public bool IsWalEnabled(string cacheName)
        {
            IgniteArgumentCheck.NotNullOrEmpty(cacheName, "cacheName");

            return DoOutInOp(ClientOp.ClusterGetWalState, w => w.WriteString(cacheName), r => r.ReadBool());
        }

        /// <summary>
        /// Does the out in op.
        /// </summary>
        private T DoOutInOp<T>(ClientOp opId, Action<BinaryWriter> writeAction, Func<IBinaryStream, T> readFunc)
        {
            return _ignite.Socket.DoOutInOp(opId, stream => WriteRequest(writeAction, stream),
                readFunc, HandleError<T>);
        }

        /// <summary>
        /// Writes the request.
        /// </summary>
        private void WriteRequest(Action<BinaryWriter> writeAction, IBinaryStream stream)
        {
            if (writeAction != null)
            {
                var writer = _marsh.StartMarshal(stream);

                writeAction(writer);

                _marsh.FinishMarshal(writer);
            }
        }

        /// <summary>
        /// Handles the error.
        /// </summary>
        private static T HandleError<T>(ClientStatusCode status, string msg)
        {
            throw new IgniteClientException(msg, null, status);
        }
    }
}
