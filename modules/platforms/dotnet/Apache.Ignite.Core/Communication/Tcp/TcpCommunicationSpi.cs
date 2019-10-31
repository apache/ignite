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

namespace Apache.Ignite.Core.Communication.Tcp
{
    using System;
    using System.ComponentModel;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Impl.Binary;

    /// <summary>
    /// <see cref="TcpCommunicationSpi"/> is default communication SPI which uses
    /// TCP/IP protocol and Java NIO to communicate with other nodes.
    /// <para />
    /// At startup, this SPI tries to start listening to local port specified by
    /// <see cref="LocalPort"/> property. If local port is occupied, then SPI will
    /// automatically increment the port number until it can successfully bind for
    /// listening. <see cref="LocalPortRange"/> configuration parameter controls
    /// maximum number of ports that SPI will try before it fails. Port range comes
    /// very handy when starting multiple grid nodes on the same machine or even
    /// in the same VM. In this case all nodes can be brought up without a single
    /// change in configuration.
    /// </summary>
    public class TcpCommunicationSpi : ICommunicationSpi
    {
        /// <summary> Default value of <see cref="AckSendThreshold"/> property. </summary>
        public const int DefaultAckSendThreshold = 16;

        /// <summary> Default value of <see cref="ConnectionsPerNode"/> property. </summary>
        public const int DefaultConnectionsPerNode = 1;

        /// <summary> Default value of <see cref="ConnectTimeout"/> property. </summary>
        public static readonly TimeSpan DefaultConnectTimeout = TimeSpan.FromSeconds(5);

        /// <summary> Default value of <see cref="DirectBuffer"/> property. </summary>
        public const bool DefaultDirectBuffer = true;

        /// <summary> Default value of <see cref="DirectSendBuffer"/> property. </summary>
        public const bool DefaultDirectSendBuffer = false;

        /// <summary> Default value of <see cref="FilterReachableAddresses"/> property. </summary>
        public const bool DefaultFilterReachableAddresses = false;

        /// <summary> Default value of <see cref="IdleConnectionTimeout"/> property. </summary>
        public static readonly TimeSpan DefaultIdleConnectionTimeout = TimeSpan.FromSeconds(30);

        /// <summary> Default value of <see cref="LocalPort"/> property. </summary>
        public const int DefaultLocalPort = 47100;

        /// <summary> Default value of <see cref="LocalPortRange"/> property. </summary>
        public const int DefaultLocalPortRange = 100;

        /// <summary> Default value of <see cref="MaxConnectTimeout"/> property. </summary>
        public static readonly TimeSpan DefaultMaxConnectTimeout = TimeSpan.FromMinutes(10);

        /// <summary> Default value of <see cref="MessageQueueLimit"/> property. </summary>
        public const int DefaultMessageQueueLimit = 1024;

        /// <summary> Default value of <see cref="ReconnectCount"/> property. </summary>
        public const int DefaultReconnectCount = 10;

        /// <summary> Default value of <see cref="SelectorsCount"/> property. </summary>
        public static readonly int DefaultSelectorsCount = Math.Min(4, Environment.ProcessorCount);

        /// <summary> Default value of <see cref="SelectorSpins"/> property. </summary>
        public const long DefaultSelectorSpins = 0;

        /// <summary> Default value of <see cref="SharedMemoryPort"/> property. </summary>
        public const int DefaultSharedMemoryPort = -1;

        /// <summary> Default socket buffer size. </summary>
        public const int DefaultSocketBufferSize = 32 * 1024;

        /// <summary> Default value of <see cref="SocketWriteTimeout"/> property. </summary>
        public const long DefaultSocketWriteTimeout = 2000;

        /// <summary> Default value of <see cref="TcpNoDelay"/> property. </summary>
        public const bool DefaultTcpNoDelay = true;

        /// <summary> Default value of <see cref="UsePairedConnections"/> property. </summary>
        public const bool DefaultUsePairedConnections = false;

        /// <summary>
        /// Initializes a new instance of the <see cref="TcpCommunicationSpi"/> class.
        /// </summary>
        public TcpCommunicationSpi()
        {
            AckSendThreshold = DefaultAckSendThreshold;
            ConnectionsPerNode = DefaultConnectionsPerNode;
            ConnectTimeout = DefaultConnectTimeout;
            DirectBuffer = DefaultDirectBuffer;
            DirectSendBuffer = DefaultDirectSendBuffer;
            FilterReachableAddresses = DefaultFilterReachableAddresses;
            IdleConnectionTimeout = DefaultIdleConnectionTimeout;
            LocalPort = DefaultLocalPort;
            LocalPortRange = DefaultLocalPortRange;
            MaxConnectTimeout = DefaultMaxConnectTimeout;
            MessageQueueLimit = DefaultMessageQueueLimit;
            ReconnectCount = DefaultReconnectCount;
            SharedMemoryPort = DefaultSharedMemoryPort;
            SelectorsCount = DefaultSelectorsCount;
            SelectorSpins = DefaultSelectorSpins;
            SocketReceiveBufferSize = DefaultSocketBufferSize;
            SocketSendBufferSize = DefaultSocketBufferSize;
            SocketWriteTimeout = DefaultSocketWriteTimeout;
            TcpNoDelay = DefaultTcpNoDelay;
            UsePairedConnections = DefaultUsePairedConnections;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="TcpCommunicationSpi"/> class.
        /// </summary>
        /// <param name="reader">The reader.</param>
        internal TcpCommunicationSpi(IBinaryRawReader reader)
        {
            AckSendThreshold = reader.ReadInt();
            ConnectionsPerNode = reader.ReadInt();
            ConnectTimeout = reader.ReadLongAsTimespan();
            DirectBuffer = reader.ReadBoolean();
            DirectSendBuffer = reader.ReadBoolean();
            FilterReachableAddresses = reader.ReadBoolean();
            IdleConnectionTimeout = reader.ReadLongAsTimespan();
            LocalAddress = reader.ReadString();
            LocalPort = reader.ReadInt();
            LocalPortRange = reader.ReadInt();
            MaxConnectTimeout = reader.ReadLongAsTimespan();
            MessageQueueLimit = reader.ReadInt();
            ReconnectCount = reader.ReadInt();
            SelectorsCount = reader.ReadInt();
            SelectorSpins = reader.ReadLong();
            SharedMemoryPort = reader.ReadInt();
            SlowClientQueueLimit = reader.ReadInt();
            SocketReceiveBufferSize = reader.ReadInt();
            SocketSendBufferSize = reader.ReadInt();
            SocketWriteTimeout = reader.ReadLong();
            TcpNoDelay = reader.ReadBoolean();
            UnacknowledgedMessagesBufferSize = reader.ReadInt();
            UsePairedConnections = reader.ReadBoolean();
        }

        /// <summary>
        /// Gets or sets the number of received messages per connection to node
        /// after which acknowledgment message is sent.
        /// </summary>
        [DefaultValue(DefaultAckSendThreshold)]
        public int AckSendThreshold { get; set; }

        /// <summary>
        /// Gets or sets the connect timeout used when establishing connection with remote nodes.
        /// </summary>
        [DefaultValue(typeof(TimeSpan), "00:00:05")]
        public TimeSpan ConnectTimeout { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether to allocate direct (ByteBuffer.allocateDirect)
        /// or heap (ByteBuffer.allocate) buffer.
        /// </summary>
        [DefaultValue(DefaultDirectBuffer)]
        public bool DirectBuffer { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether to allocate direct (ByteBuffer.allocateDirect)
        /// or heap (ByteBuffer.allocate) send buffer.
        /// </summary>
        [DefaultValue(DefaultDirectSendBuffer)]
        public bool DirectSendBuffer { get; set; }

        /// <summary>
        /// Sets maximum idle connection timeout upon which a connection to client will be closed.
        /// </summary>
        [DefaultValue(typeof(TimeSpan), "00:00:30")]
        public TimeSpan IdleConnectionTimeout { get; set; }

        /// <summary>
        /// Gets or sets the local host address for socket binding. Note that one node could have
        /// additional addresses beside the loopback one. This configuration parameter is optional.
        /// </summary>
        public string LocalAddress { get; set; }

        /// <summary>
        /// Gets or sets the local port for socket binding.
        /// </summary>
        [DefaultValue(DefaultLocalPort)]
        public int LocalPort { get; set; }

        /// <summary>
        /// Gets or sets local port range for local host ports (value must greater than or equal to <tt>0</tt>).
        /// If provided local port <see cref="LocalPort"/> is occupied,
        /// implementation will try to increment the port number for as long as it is less than
        /// initial value plus this range.
        /// <para />
        /// If port range value is <c>0</c>, then implementation will try bind only to the port provided by
        /// <see cref="LocalPort"/> method and fail if binding to this port did not succeed.
        /// </summary>
        [DefaultValue(DefaultLocalPortRange)]
        public int LocalPortRange { get; set; }

        /// <summary>
        /// Gets or sets maximum connect timeout. If handshake is not established within connect timeout,
        /// then SPI tries to repeat handshake procedure with increased connect timeout.
        /// Connect timeout can grow till maximum timeout value,
        /// if maximum timeout value is reached then the handshake is considered as failed.
        /// <para />
        /// <c>0</c> is interpreted as infinite timeout.
        /// </summary>
        [DefaultValue(typeof(TimeSpan), "00:10:00")]
        public TimeSpan MaxConnectTimeout { get; set; }

        /// <summary>
        /// Gets or sets the message queue limit for incoming and outgoing messages.
        /// <para />
        /// When set to positive number send queue is limited to the configured value.
        /// <c>0</c> disables the limitation.
        /// </summary>
        [DefaultValue(DefaultMessageQueueLimit)]
        public int MessageQueueLimit { get; set; }

        /// <summary>
        /// Gets or sets the maximum number of reconnect attempts used when establishing connection with remote nodes.
        /// </summary>
        [DefaultValue(DefaultReconnectCount)]
        public int ReconnectCount { get; set; }

        /// <summary>
        /// Gets or sets the count of selectors te be used in TCP server.
        /// <para />
        /// Default value is <see cref="DefaultSelectorsCount"/>, which is calculated as
        /// <c>Math.Min(4, Environment.ProcessorCount)</c>
        /// </summary>
        public int SelectorsCount { get; set; }

        /// <summary>
        /// Gets or sets slow client queue limit.
        /// <para/>
        /// When set to a positive number, communication SPI will monitor clients outbound message queue sizes
        /// and will drop those clients whose queue exceeded this limit.
        /// <para/>
        /// Usually this value should be set to the same value as <see cref="MessageQueueLimit"/> which controls
        /// message back-pressure for server nodes. The default value for this parameter is <c>0</c>
        /// which means unlimited.
        /// </summary>
        public int SlowClientQueueLimit { get; set; }

        /// <summary>
        /// Gets or sets the size of the socket receive buffer.
        /// </summary>
        [DefaultValue(DefaultSocketBufferSize)]
        public int SocketReceiveBufferSize { get; set; }

        /// <summary>
        /// Gets or sets the size of the socket send buffer.
        /// </summary>
        [DefaultValue(DefaultSocketBufferSize)]
        public int SocketSendBufferSize { get; set; }

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
        /// Gets or sets the maximum number of stored unacknowledged messages per connection to node.
        /// If number of unacknowledged messages exceeds this number
        /// then connection to node is closed and reconnect is attempted.
        /// </summary>
        public int UnacknowledgedMessagesBufferSize { get; set; }

        /// <summary>
        /// Gets or sets the number of connections per node.
        /// </summary>
        [DefaultValue(DefaultConnectionsPerNode)]
        public int ConnectionsPerNode { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether separate connections should be used for incoming and outgoing data.
        /// Set this to <c>true</c> if <see cref="ConnectionsPerNode"/> should maintain connection for outgoing
        /// and incoming messages separately. In this case total number of connections between local and each remote
        /// node is equals to <see cref="ConnectionsPerNode"/> * 2.
        /// </summary>
        public bool UsePairedConnections { get; set; }

        /// <summary>
        /// Gets or sets a local port to accept shared memory connections.
        /// </summary>
        [DefaultValue(DefaultSharedMemoryPort)]
        public int SharedMemoryPort { get; set; }

        /// <summary>
        /// Gets or sets socket write timeout for TCP connection. If message can not be written to
        /// socket within this time then connection is closed and reconnect is attempted.
        /// <para />
        /// Default value is <see cref="DefaultSocketWriteTimeout"/>.
        /// </summary>
        [DefaultValue(DefaultSocketWriteTimeout)]
        public long SocketWriteTimeout { get; set; }

        /// <summary>
        /// Gets or sets a values that defines how many non-blocking selectors should be made.
        /// Can be set to <see cref="Int64.MaxValue"/> so selector threads will never block.
        /// <para />
        /// Default value is <see cref="DefaultSelectorSpins"/>.
        /// </summary>
        public long SelectorSpins { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether filter for reachable addresses
        /// should be enabled on creating tcp client.
        /// </summary>
        public bool FilterReachableAddresses { get; set; }

        /// <summary>
        /// Writes this instance to the specified writer.
        /// </summary>
        internal void Write(IBinaryRawWriter writer)
        {
            writer.WriteInt(AckSendThreshold);
            writer.WriteInt(ConnectionsPerNode);
            writer.WriteLong((long) ConnectTimeout.TotalMilliseconds);
            writer.WriteBoolean(DirectBuffer);
            writer.WriteBoolean(DirectSendBuffer);
            writer.WriteBoolean(FilterReachableAddresses);
            writer.WriteLong((long) IdleConnectionTimeout.TotalMilliseconds);
            writer.WriteString(LocalAddress);
            writer.WriteInt(LocalPort);
            writer.WriteInt(LocalPortRange);
            writer.WriteLong((long) MaxConnectTimeout.TotalMilliseconds);
            writer.WriteInt(MessageQueueLimit);
            writer.WriteInt(ReconnectCount);
            writer.WriteInt(SelectorsCount);
            writer.WriteLong(SelectorSpins);
            writer.WriteInt(SharedMemoryPort);
            writer.WriteInt(SlowClientQueueLimit);
            writer.WriteInt(SocketReceiveBufferSize);
            writer.WriteInt(SocketSendBufferSize);
            writer.WriteLong(SocketWriteTimeout);
            writer.WriteBoolean(TcpNoDelay);
            writer.WriteInt(UnacknowledgedMessagesBufferSize);
            writer.WriteBoolean(UsePairedConnections);
        }
    }
}
