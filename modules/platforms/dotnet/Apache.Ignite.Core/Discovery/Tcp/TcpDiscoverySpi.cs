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

namespace Apache.Ignite.Core.Discovery.Tcp
{
    using System;
    using System.ComponentModel;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Impl.Binary;

    /// <summary>
    /// TCP discover service provider.
    /// </summary>
    public class TcpDiscoverySpi : IDiscoverySpi
    {
        /// <summary>
        /// Default socket timeout.
        /// </summary>
        public static readonly TimeSpan DefaultSocketTimeout = TimeSpan.FromMilliseconds(5000);

        /// <summary>
        /// Default acknowledgement timeout.
        /// </summary>
        public static readonly TimeSpan DefaultAckTimeout = TimeSpan.FromMilliseconds(5000);

        /// <summary>
        /// Default maximum acknowledgement timeout.
        /// </summary>
        public static readonly TimeSpan DefaultMaxAckTimeout = TimeSpan.FromMinutes(10);

        /// <summary>
        /// Default network timeout.
        /// </summary>
        public static readonly TimeSpan DefaultNetworkTimeout = TimeSpan.FromMilliseconds(5000);

        /// <summary>
        /// Default join timeout.
        /// </summary>
        public static readonly TimeSpan DefaultJoinTimeout = TimeSpan.Zero;

        /// <summary>
        /// Default value for the <see cref="ReconnectCount"/> property.
        /// </summary>
        public const int DefaultReconnectCount = 10;

        /// <summary>
        /// Default value for the <see cref="LocalPort"/> property.
        /// </summary>
        public const int DefaultLocalPort = 47500;

        /// <summary>
        /// Default value for the <see cref="LocalPortRange"/> property.
        /// </summary>
        public const int DefaultLocalPortRange = 100;

        /// <summary>
        /// Default value for the <see cref="IpFinderCleanFrequency"/> property.
        /// </summary>
        public static readonly TimeSpan DefaultIpFinderCleanFrequency = TimeSpan.FromSeconds(60);

        /// <summary>
        /// Default value for the <see cref="ThreadPriority"/> property.
        /// </summary>
        public const int DefaultThreadPriority = 10;

        /// <summary>
        /// Default value for the <see cref="TopologyHistorySize"/> property.
        /// </summary>
        public const int DefaultTopologyHistorySize = 1000;

        /// <summary>
        /// Initializes a new instance of the <see cref="TcpDiscoverySpi"/> class.
        /// </summary>
        public TcpDiscoverySpi()
        {
            SocketTimeout = DefaultSocketTimeout;
            AckTimeout = DefaultAckTimeout;
            MaxAckTimeout = DefaultMaxAckTimeout;
            NetworkTimeout = DefaultNetworkTimeout;
            JoinTimeout = DefaultJoinTimeout;
            ReconnectCount = DefaultReconnectCount;
            LocalPort = DefaultLocalPort;
            LocalPortRange = DefaultLocalPortRange;
            IpFinderCleanFrequency = DefaultIpFinderCleanFrequency;
            ThreadPriority = DefaultThreadPriority;
            TopologyHistorySize = DefaultTopologyHistorySize;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="TcpDiscoverySpi"/> class.
        /// </summary>
        /// <param name="reader">The reader.</param>
        internal TcpDiscoverySpi(IBinaryRawReader reader)
        {
            IpFinder = reader.ReadBoolean() ? TcpDiscoveryIpFinderBase.ReadInstance(reader) : null;

            SocketTimeout = reader.ReadLongAsTimespan();
            AckTimeout = reader.ReadLongAsTimespan();
            MaxAckTimeout = reader.ReadLongAsTimespan();
            NetworkTimeout = reader.ReadLongAsTimespan();
            JoinTimeout = reader.ReadLongAsTimespan();

            ForceServerMode = reader.ReadBoolean();
            ClientReconnectDisabled = reader.ReadBoolean();
            LocalAddress = reader.ReadString();
            ReconnectCount = reader.ReadInt();
            LocalPort = reader.ReadInt();
            LocalPortRange = reader.ReadInt();
            StatisticsPrintFrequency = reader.ReadLongAsTimespan();
            IpFinderCleanFrequency = reader.ReadLongAsTimespan();
            ThreadPriority = reader.ReadInt();
            TopologyHistorySize = reader.ReadInt();
        }

        /// <summary>
        /// Gets or sets the IP finder which defines how nodes will find each other on the network.
        /// </summary>
        public ITcpDiscoveryIpFinder IpFinder { get; set; }

        /// <summary>
        /// Gets or sets the socket timeout.
        /// </summary>
        [DefaultValue(typeof(TimeSpan), "00:00:05")]
        public TimeSpan SocketTimeout { get; set; }

        /// <summary>
        /// Gets or sets the timeout for receiving acknowledgement for sent message.
        /// </summary>
        [DefaultValue(typeof(TimeSpan), "00:00:05")]
        public TimeSpan AckTimeout { get; set; }

        /// <summary>
        /// Gets or sets the maximum timeout for receiving acknowledgement for sent message.
        /// </summary>
        [DefaultValue(typeof(TimeSpan), "00:10:00")]
        public TimeSpan MaxAckTimeout { get; set; }

        /// <summary>
        /// Gets or sets the network timeout.
        /// </summary>
        [DefaultValue(typeof(TimeSpan), "00:00:05")]
        public TimeSpan NetworkTimeout { get; set; }
        
        /// <summary>
        /// Gets or sets the join timeout.
        /// </summary>
        public TimeSpan JoinTimeout { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether TcpDiscoverySpi is started in server mode 
        /// regardless of <see cref="IgniteConfiguration.ClientMode"/> setting.
        /// </summary>
        public bool ForceServerMode { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether client does not try to reconnect after
        /// server detected client node failure.
        /// </summary>
        public bool ClientReconnectDisabled { get; set; }

        /// <summary>
        /// Gets or sets the local host IP address that discovery SPI uses.
        /// </summary>
        public string LocalAddress { get; set; }

        /// <summary>
        /// Gets or sets the number of times node tries to (re)establish connection to another node.
        /// </summary>
        [DefaultValue(DefaultReconnectCount)]
        public int ReconnectCount { get; set; }

        /// <summary>
        /// Gets or sets the local port to listen to.
        /// </summary>
        [DefaultValue(DefaultLocalPort)]
        public int LocalPort { get; set; }

        /// <summary>
        /// Gets or sets the range for local ports. Local node will try to bind on first available port starting from
        /// <see cref="LocalPort"/> up until (<see cref="LocalPort"/> + <see cref="LocalPortRange"/>).
        /// </summary>
        [DefaultValue(DefaultLocalPortRange)]
        public int LocalPortRange { get; set; }

        /// <summary>
        /// Gets or sets the statistics print frequency.
        /// <see cref="TimeSpan.Zero"/> for no statistics.
        /// </summary>
        public TimeSpan StatisticsPrintFrequency { get; set; }

        /// <summary>
        /// Gets or sets the IP finder clean frequency.
        /// </summary>
        [DefaultValue(typeof(TimeSpan), "0:1:0")]
        public TimeSpan IpFinderCleanFrequency { get; set; }

        /// <summary>
        /// Sets thread priority, 1 (lowest) to 10 (highest). All threads within SPI will be started with it.
        /// </summary>
        [DefaultValue(DefaultThreadPriority)]
        public int ThreadPriority { get; set; }

        /// <summary>
        /// Gets or sets the size of topology snapshots history.
        /// </summary>
        [DefaultValue(DefaultTopologyHistorySize)]
        public int TopologyHistorySize { get; set; }

        /// <summary>
        /// Writes this instance to the specified writer.
        /// </summary>
        internal void Write(IBinaryRawWriter writer)
        {
            var ipFinder = IpFinder;

            if (ipFinder != null)
            {
                writer.WriteBoolean(true);

                var finder = ipFinder as TcpDiscoveryIpFinderBase;

                if (finder == null)
                    throw new InvalidOperationException("Unsupported IP finder: " + ipFinder.GetType());

                finder.Write(writer);
            }
            else
                writer.WriteBoolean(false);

            writer.WriteLong((long) SocketTimeout.TotalMilliseconds);
            writer.WriteLong((long) AckTimeout.TotalMilliseconds);
            writer.WriteLong((long) MaxAckTimeout.TotalMilliseconds);
            writer.WriteLong((long) NetworkTimeout.TotalMilliseconds);
            writer.WriteLong((long) JoinTimeout.TotalMilliseconds);

            writer.WriteBoolean(ForceServerMode);
            writer.WriteBoolean(ClientReconnectDisabled);
            writer.WriteString(LocalAddress);
            writer.WriteInt(ReconnectCount);
            writer.WriteInt(LocalPort);
            writer.WriteInt(LocalPortRange);
            writer.WriteLong((long) StatisticsPrintFrequency.TotalMilliseconds);
            writer.WriteLong((long) IpFinderCleanFrequency.TotalMilliseconds);
            writer.WriteInt(ThreadPriority);
            writer.WriteInt(TopologyHistorySize);
        }
    }
}
