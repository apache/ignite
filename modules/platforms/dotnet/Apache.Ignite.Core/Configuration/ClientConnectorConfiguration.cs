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
    using Apache.Ignite.Core.Impl.Binary;

    /// <summary>
    /// Client connector configuration (ODBC, JDBC, Thin Client).
    /// </summary>
    public class ClientConnectorConfiguration
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
        /// Default idle timeout.
        /// </summary>
        public static readonly TimeSpan DefaultIdleTimeout = TimeSpan.Zero;

        /// <summary>
        /// Default handshake timeout.
        /// </summary>
        public static readonly TimeSpan DefaultHandshakeTimeout = TimeSpan.FromSeconds(10);

        /// <summary>
        /// Default value for <see cref="ThinClientEnabled"/> property.
        /// </summary>
        public const bool DefaultThinClientEnabled = true;

        /// <summary>
        /// Default value for <see cref="JdbcEnabled"/> property.
        /// </summary>
        public const bool DefaultJdbcEnabled = true;

        /// <summary>
        /// Default value for <see cref="OdbcEnabled"/> property.
        /// </summary>
        public const bool DefaultOdbcEnabled = true;

        /// <summary>
        /// Initializes a new instance of the <see cref="ClientConnectorConfiguration"/> class.
        /// </summary>
        public ClientConnectorConfiguration()
        {
            Port = DefaultPort;
            PortRange = DefaultPortRange;
            SocketSendBufferSize = DefaultSocketBufferSize;
            SocketReceiveBufferSize = DefaultSocketBufferSize;
            TcpNoDelay = DefaultTcpNoDelay;
            MaxOpenCursorsPerConnection = DefaultMaxOpenCursorsPerConnection;
            ThreadPoolSize = DefaultThreadPoolSize;
            IdleTimeout = DefaultIdleTimeout;
            HandshakeTimeout = DefaultHandshakeTimeout;

            ThinClientEnabled = DefaultThinClientEnabled;
            OdbcEnabled = DefaultOdbcEnabled;
            JdbcEnabled = DefaultJdbcEnabled;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ClientConnectorConfiguration"/> class.
        /// </summary>
        internal ClientConnectorConfiguration(IBinaryRawReader reader)
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
            IdleTimeout = reader.ReadLongAsTimespan();

            ThinClientEnabled = reader.ReadBoolean();
            OdbcEnabled = reader.ReadBoolean();
            JdbcEnabled = reader.ReadBoolean();

            HandshakeTimeout = reader.ReadLongAsTimespan();

            // Thin client configuration.
            if (reader.ReadBoolean())
            {
                ThinClientConfiguration = new ThinClientConfiguration
                {
                    MaxActiveTxPerConnection = reader.ReadInt(),
                    MaxActiveComputeTasksPerConnection = reader.ReadInt()
                };
            }
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
            writer.WriteTimeSpanAsLong(IdleTimeout);

            writer.WriteBoolean(ThinClientEnabled);
            writer.WriteBoolean(OdbcEnabled);
            writer.WriteBoolean(JdbcEnabled);

            writer.WriteTimeSpanAsLong(HandshakeTimeout);

            // Thin client configuration.
            if (ThinClientConfiguration != null)
            {
                writer.WriteBoolean(true);
                writer.WriteInt(ThinClientConfiguration.MaxActiveTxPerConnection);
                writer.WriteInt(ThinClientConfiguration.MaxActiveComputeTasksPerConnection);
            }
            else
            {
                writer.WriteBoolean(false);
            }
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
        
        /// <summary>
        /// Gets or sets idle timeout for client connections on the server side.
        /// If no packets come within idle timeout, the connection is closed by the server.
        /// Zero or negative means no timeout.
        /// </summary>
        public TimeSpan IdleTimeout { get; set; }

        /// <summary>
        /// Gets or sets handshake timeout for client connections on the server side.
        /// If no successful handshake is performed within this timeout upon successful establishment of TCP connection
        /// the connection is closed.
        /// </summary>
        public TimeSpan HandshakeTimeout { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether thin client connector is enabled.
        /// </summary>
        [DefaultValue(DefaultThinClientEnabled)]
        public bool ThinClientEnabled { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether JDBC connector is enabled.
        /// </summary>
        [DefaultValue(DefaultJdbcEnabled)]
        public bool JdbcEnabled { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether ODBC connector is enabled.
        /// </summary>
        [DefaultValue(DefaultOdbcEnabled)]
        public bool OdbcEnabled { get; set; }

        /// <summary>
        /// Gets or sets thin client specific configuration.
        /// </summary>
        public ThinClientConfiguration ThinClientConfiguration { get; set; }
    }
}
