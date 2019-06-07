/*
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
    using System.Diagnostics;
    using System.Net;

    /// <summary>
    /// Internal representation of client socket endpoint.
    /// </summary>
    internal class SocketEndpoint
    {
        /** */
        private readonly IPEndPoint _endPoint;

        /** */
        private readonly string _host;

        /** */
        private volatile ClientSocket _socket;

        /// <summary>
        /// Initializes a new instance of the <see cref="SocketEndpoint"/> class.
        /// </summary>
        public SocketEndpoint(IPEndPoint endPoint, string host)
        {
            _endPoint = endPoint;
            _host = host;
        }

        /// <summary>
        /// Gets the socket.
        /// </summary>
        public ClientSocket Socket
        {
            get { return _socket; }
            set
            {
                Debug.Assert(value != null);
                _socket = value;
            }
        }

        /// <summary>
        /// Gets the IPEndPoint.
        /// </summary>
        public IPEndPoint EndPoint
        {
            get { return _endPoint; }
        }

        /// <summary>
        /// Gets the host.
        /// </summary>
        public string Host
        {
            get { return _host; }
        }
    }
}
