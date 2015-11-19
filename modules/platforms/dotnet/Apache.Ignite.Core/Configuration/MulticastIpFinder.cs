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
    using Apache.Ignite.Core.Binary;

    /// <summary>
    /// Multicast-based IP finder.
    /// <para />
    /// When TCP discovery starts this finder sends multicast request and waits
    /// for some time when others nodes reply to this request with messages containing their addresses
    /// </summary>
    public class MulticastIpFinder : StaticIpFinder
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
        /// Default response timeout.
        /// </summary>
        public static readonly TimeSpan DefaultResponseTimeout = TimeSpan.FromMilliseconds(500);

        /// <summary>
        /// Initializes a new instance of the <see cref="MulticastIpFinder"/> class.
        /// </summary>
        public MulticastIpFinder()
        {
            MulticastPort = DefaultMulticastPort;
            AddressRequestAtempts = DefaultAddressRequestAttempts;
            ResponseTimeout = DefaultResponseTimeout;
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
        public string MulticastGroup { get; set; }

        /// <summary>
        /// Gets or sets the port number which multicast messages are sent to.
        /// </summary>
        public int MulticastPort { get; set; }

        /// <summary>
        /// Gets or sets the number of attempts to send multicast address request. IP finder re-sends
        /// request only in case if no reply for previous request is received.
        /// </summary>
        public int AddressRequestAtempts { get; set; }

        /// <summary>
        /// Gets or sets the response timeout.
        /// </summary>
        public TimeSpan ResponseTimeout { get; set; }

        /// <summary>
        /// Gets or sets the time to live for multicast packets sent out on this
        /// IP finder in order to control the scope of the multicast.
        /// </summary>
        public byte? TimeToLive { get; set; }

        /*
        /// <summary>
        /// Initializes a new instance of the <see cref="MulticastIpFinder"/> class.
        /// </summary>
        /// <param name="reader">The reader.</param>
        internal MulticastIpFinder(IBinaryRawReader reader) : base(reader)
        {
            LocalAddress = reader.ReadString();
            MulticastGroup = reader.ReadString();
            MulticastPort = reader.ReadInt();
            AddressRequestAtempts = reader.ReadInt();
            ResponseTimeout = TimeSpan.FromMilliseconds(reader.ReadLong());
        }*/

        /** <inheritdoc /> */
        internal override void Write(IBinaryRawWriter writer)
        {
            base.Write(writer);

            writer.WriteString(LocalAddress);
            writer.WriteString(MulticastGroup);
            writer.WriteInt(MulticastPort);
            writer.WriteInt(AddressRequestAtempts);
            writer.WriteInt((int) ResponseTimeout.TotalMilliseconds);

            writer.WriteBoolean(TimeToLive.HasValue);

            if (TimeToLive.HasValue)
                writer.WriteByte(TimeToLive.Value);
        }

        /** <inheritdoc /> */
        protected override byte TypeCode
        {
            get { return TypeCodeMulticastIpFinder; }
        }
    }
}