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

namespace Apache.Ignite.Core.Configuration
{
    using System;
    using System.ComponentModel;
    using System.Diagnostics;
    using Apache.Ignite.Core.Binary;

    /// <summary>
    /// SQL connector configuration (for ODBC and JDBC).
    /// </summary>
    [Obsolete("Use ClientConnectorConfiguration instead.")]
    public class SqlConnectorConfiguration
    {
        /// <summary>
        /// Default port.
        /// </summary>
        public const int DefaultPort = 10800;

        /// <summary>
        /// Default port range.
        /// </summary>
        public const int DefaultPortRange = 100;

        /// <summary>
        /// Default socket buffer size.
        /// </summary>
        public const int DefaultSocketBufferSize = 0;

        /// <summary>
        /// Default value of <see cref="TcpNoDelay" /> property.
        /// </summary>
        public const bool DefaultTcpNoDelay = true;

        /// <summary>
        /// Default maximum number of open cursors per connection.
        /// </summary>
        public const int DefaultMaxOpenCursorsPerConnection = 128;

        /// <summary>
        /// Default SQL connector thread pool size.
        /// </summary>
        public static readonly int DefaultThreadPoolSize = IgniteConfiguration.DefaultThreadPoolSize;

        /// <summary>
        /// Initializes a new instance of the <see cref="SqlConnectorConfiguration"/> class.
        /// </summary>
        public SqlConnectorConfiguration()
        {
            Port = DefaultPort;
            PortRange = DefaultPortRange;
            SocketSendBufferSize = DefaultSocketBufferSize;
            SocketReceiveBufferSize = DefaultSocketBufferSize;
            TcpNoDelay = DefaultTcpNoDelay;
            MaxOpenCursorsPerConnection = DefaultMaxOpenCursorsPerConnection;
            ThreadPoolSize = DefaultThreadPoolSize;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="SqlConnectorConfiguration"/> class.
        /// </summary>
        internal SqlConnectorConfiguration(IBinaryRawReader reader)
        {
            Debug.Assert(reader != null);

            Host = reader.ReadString();
            Port = reader.ReadInt();
            PortRange = reader.ReadInt();
            SocketSendBufferSize = reader.ReadInt();
            SocketReceiveBufferSize = reader.ReadInt();
            TcpNoDelay = reader.ReadBoolean();
            MaxOpenCursorsPerConnection = reader.ReadInt();
            ThreadPoolSize = reader.ReadInt();
        }

        /// <summary>
        /// Writes to the specified writer.
        /// </summary>
        internal void Write(IBinaryRawWriter writer)
        {
            Debug.Assert(writer != null);
            
            writer.WriteString(Host);
            writer.WriteInt(Port);
            writer.WriteInt(PortRange);
            writer.WriteInt(SocketSendBufferSize);
            writer.WriteInt(SocketReceiveBufferSize);
            writer.WriteBoolean(TcpNoDelay);
            writer.WriteInt(MaxOpenCursorsPerConnection);
            writer.WriteInt(ThreadPoolSize);
        }

        /// <summary>
        /// Gets or sets the host.
        /// </summary>
        public string Host { get; set; }

        /// <summary>
        /// Gets or sets the port.
        /// </summary>
        [DefaultValue(DefaultPort)]
        public int Port { get; set; }

        /// <summary>
        /// Gets or sets the port range.
        /// </summary>
        [DefaultValue(DefaultPortRange)]
        public int PortRange { get; set; }

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
        /// Gets or sets the maximum open cursors per connection.
        /// </summary>
        [DefaultValue(DefaultMaxOpenCursorsPerConnection)]
        public int MaxOpenCursorsPerConnection { get; set; }

        /// <summary>
        /// Gets or sets the size of the thread pool.
        /// </summary>
        public int ThreadPoolSize { get; set; }
    }
}
