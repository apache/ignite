/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

namespace Apache.Ignite.Internal
{
    using System.Diagnostics;
    using System.Net;

    /// <summary>
    /// Internal representation of client socket endpoint.
    /// </summary>
    internal sealed class SocketEndpoint
    {
        /** */
        private volatile ClientSocket? _socket;

        /// <summary>
        /// Initializes a new instance of the <see cref="SocketEndpoint"/> class.
        /// </summary>
        /// <param name="endPoint">Endpoint.</param>
        /// <param name="host">Host name.</param>
        public SocketEndpoint(IPEndPoint endPoint, string host)
        {
            EndPoint = endPoint;
            Host = host;
        }

        /// <summary>
        /// Gets or sets the socket.
        /// </summary>
        public ClientSocket? Socket
        {
            get => _socket;
            set
            {
                Debug.Assert(value != null, "value != null");
                _socket = value;
            }
        }

        /// <summary>
        /// Gets the IPEndPoint.
        /// </summary>
        public IPEndPoint EndPoint { get; }

        /// <summary>
        /// Gets the host.
        /// </summary>
        public string Host { get; }
    }
}
