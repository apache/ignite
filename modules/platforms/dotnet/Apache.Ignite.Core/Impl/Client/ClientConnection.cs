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
    using System.Diagnostics;
    using System.Net;
    using Apache.Ignite.Core.Client;

    /// <summary>
    /// Represents Ignite client connection.
    /// </summary>
    internal class ClientConnection : IClientConnection
    {
        /** */
        private readonly EndPoint _localEndPoint;
        
        /** */
        private readonly EndPoint _remoteEndPoint;

        /** */
        private readonly Guid _nodeId;

        /// <summary>
        /// Initializes a new instance of <see cref="ClientConnection"/>.
        /// </summary>
        public ClientConnection(EndPoint localEndPoint, EndPoint remoteEndPoint, Guid nodeId)
        {
            Debug.Assert(localEndPoint != null);
            Debug.Assert(remoteEndPoint != null);
            Debug.Assert(nodeId != Guid.Empty);
            
            _localEndPoint = localEndPoint;
            _remoteEndPoint = remoteEndPoint;
            _nodeId = nodeId;
        }

        /** <inheritdoc /> */
        public EndPoint RemoteEndPoint
        {
            get { return _remoteEndPoint; }
        }

        /** <inheritdoc /> */
        public EndPoint LocalEndPoint
        {
            get { return _localEndPoint; }
        }

        /** <inheritdoc /> */
        public Guid NodeId
        {
            get { return _nodeId; }
        }
    }
}