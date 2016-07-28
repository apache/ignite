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

namespace Apache.Ignite.Core.Discovery.Tcp.Multicast
{
    using System;
    using System.ComponentModel;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Discovery.Tcp.Static;

    /// <summary>
    /// Multicast-based IP finder.
    /// <para />
    /// When TCP discovery starts this finder sends multicast request and waits
    /// for some time when others nodes reply to this request with messages containing their addresses
    /// </summary>
    public class TcpDiscoveryMulticastIpFinder : TcpDiscoveryStaticIpFinder
    {
        /// <summary>
        /// Default multicast port.
        /// </summary>
        public const int DefaultMulticastPort = 47400;

        /// <summary>
        /// Default address request attempts.
        /// </summary>
        public const int DefaultAddressRequestAttempts = 2;

        /// <summary>
        /// Default multicast group.
        /// </summary>
        public const string DefaultMulticastGroup = "228.1.2.4";

        /// <summary>
        /// Default response timeout.
        /// </summary>
        public static readonly TimeSpan DefaultResponseTimeout = TimeSpan.FromMilliseconds(500);

        /// <summary>
        /// Initializes a new instance of the <see cref="TcpDiscoveryMulticastIpFinder"/> class.
        /// </summary>
        public TcpDiscoveryMulticastIpFinder()
        {
            MulticastPort = DefaultMulticastPort;
            AddressRequestAttempts = DefaultAddressRequestAttempts;
            ResponseTimeout = DefaultResponseTimeout;
            MulticastGroup = DefaultMulticastGroup;
        }

        /// <summary>
        /// Gets or sets the local address.
        /// If provided address is non-loopback then multicast socket is bound to this interface. 
        /// If local address is not set or is any local address then IP finder
        /// creates multicast sockets for all found non-loopback addresses.
        /// </summary>
        public string LocalAddress { get; set; }

        /// <summary>
        /// Gets or sets the IP address of the multicast group.
        /// </summary>
        [DefaultValue(DefaultMulticastGroup)]
        public string MulticastGroup { get; set; }

        /// <summary>
        /// Gets or sets the port number which multicast messages are sent to.
        /// </summary>
        [DefaultValue(DefaultMulticastPort)]
        public int MulticastPort { get; set; }

        /// <summary>
        /// Gets or sets the number of attempts to send multicast address request. IP finder re-sends
        /// request only in case if no reply for previous request is received.
        /// </summary>
        [DefaultValue(DefaultAddressRequestAttempts)]
        public int AddressRequestAttempts { get; set; }

        /// <summary>
        /// Gets or sets the response timeout.
        /// </summary>
        [DefaultValue(typeof(TimeSpan), "00:00:00.5")]
        public TimeSpan ResponseTimeout { get; set; }

        /// <summary>
        /// Gets or sets the time to live for multicast packets sent out on this
        /// IP finder in order to control the scope of the multicast.
        /// </summary>
        public byte? TimeToLive { get; set; } 

        /// <summary>
        /// Initializes a new instance of the <see cref="TcpDiscoveryMulticastIpFinder"/> class.
        /// </summary>
        /// <param name="reader">The reader.</param>
        internal TcpDiscoveryMulticastIpFinder(IBinaryRawReader reader) : base(reader)
        {
            LocalAddress = reader.ReadString();
            MulticastGroup = reader.ReadString();
            MulticastPort = reader.ReadInt();
            AddressRequestAttempts = reader.ReadInt();
            ResponseTimeout = TimeSpan.FromMilliseconds(reader.ReadInt());
            TimeToLive = reader.ReadBoolean() ? (byte?) reader.ReadInt() : null;
        }

        /** <inheritdoc /> */
        internal override void Write(IBinaryRawWriter writer)
        {
            base.Write(writer);

            writer.WriteString(LocalAddress);
            writer.WriteString(MulticastGroup);
            writer.WriteInt(MulticastPort);
            writer.WriteInt(AddressRequestAttempts);
            writer.WriteInt((int) ResponseTimeout.TotalMilliseconds);

            writer.WriteBoolean(TimeToLive.HasValue);

            if (TimeToLive.HasValue)
                writer.WriteInt(TimeToLive.Value);
        }

        /// <summary>
        /// Gets the type code to be used in Java to determine ip finder type.
        /// </summary>
        protected override byte TypeCode
        {
            get { return TypeCodeMulticastIpFinder; }
        }
    }
}