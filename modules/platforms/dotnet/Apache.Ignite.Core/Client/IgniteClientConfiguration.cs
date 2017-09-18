﻿/*
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

namespace Apache.Ignite.Core.Client
{
    using System.ComponentModel;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Impl.Binary;

    /// <summary>
    /// Ignite thin client configuration.
    /// <para />
    /// Ignite thin client connects to a specific Ignite node with a socket and does not start JVM in process.
    /// This configuration should correspond to <see cref="IgniteConfiguration.SqlConnectorConfiguration"/>
    /// on a target node.
    /// </summary>
    public class IgniteClientConfiguration
    {
        /// <summary>
        /// Default port.
        /// </summary>
        public const int DefaultPort = 10800;

        /// <summary>
        /// Default socket buffer size.
        /// </summary>
        public const int DefaultSocketBufferSize = 0;

        /// <summary>
        /// Default value of <see cref="TcpNoDelay" /> property.
        /// </summary>
        public const bool DefaultTcpNoDelay = true;

        /// <summary>
        /// Initializes a new instance of the <see cref="IgniteClientConfiguration"/> class.
        /// </summary>
        public IgniteClientConfiguration()
        {
            Port = DefaultPort;
            SocketSendBufferSize = DefaultSocketBufferSize;
            SocketReceiveBufferSize = DefaultSocketBufferSize;
            TcpNoDelay = DefaultTcpNoDelay;
        }

        /// <summary>
        /// Gets or sets the host. Should not be null.
        /// </summary>
        public string Host { get; set; }

        /// <summary>
        /// Gets or sets the port.
        /// </summary>
        [DefaultValue(DefaultPort)]
        public int Port { get; set; }

        /// <summary>
        /// Gets or sets the size of the socket send buffer. When set to 0, operating system default is used.
        /// </summary>
        [DefaultValue(DefaultSocketBufferSize)]
        public int SocketSendBufferSize { get; set; }

        /// <summary>
        /// Gets or sets the size of the socket receive buffer. When set to 0, operating system default is used.
        /// </summary>
        [DefaultValue(DefaultSocketBufferSize)]
        public int SocketReceiveBufferSize { get; set; }

        /// <summary>
        /// Gets or sets the value for <c>TCP_NODELAY</c> socket option. Each
        /// socket will be opened using provided value.
        /// <para />
        /// Setting this option to <c>true</c> disables Nagle's algorithm
        /// for socket decreasing latency and delivery time for small messages.
        /// <para />
        /// For systems that work under heavy network load it is advisable to set this value to <c>false</c>.
        /// </summary>
        [DefaultValue(DefaultTcpNoDelay)]
        public bool TcpNoDelay { get; set; }

        /// <summary>
        /// Gets or sets the binary configuration.
        /// </summary>
        public BinaryConfiguration BinaryConfiguration { get; set; }

        /// <summary>
        /// Gets or sets custom binary processor. Internal property for tests.
        /// </summary>
        internal IBinaryProcessor BinaryProcessor { get; set; }
    }
}
