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
    /// Node discovery configuration.
    /// </summary>
    public class DiscoveryConfiguration
    {
        /// <summary>
        /// Default socket timeout.
        /// </summary>
        public readonly TimeSpan DefaultSocketTimeout = TimeSpan.FromMilliseconds(5000);

        /// <summary>
        /// Default acknowledgement timeout.
        /// </summary>
        public readonly TimeSpan DefaultAckTimeout = TimeSpan.FromMilliseconds(5000);

        /// <summary>
        /// Default maximum acknowledgement timeout.
        /// </summary>
        public readonly TimeSpan DefaultMaxAckTimeout = TimeSpan.FromMinutes(10);

        /// <summary>
        /// Default network timeout.
        /// </summary>
        public readonly TimeSpan DefaultNetworkTimeout = TimeSpan.FromMilliseconds(5000);

        /// <summary>
        /// Default join timeout.
        /// </summary>
        public readonly TimeSpan DefaultJoinTimeout = TimeSpan.Zero;

        /// <summary>
        /// Initializes a new instance of the <see cref="DiscoveryConfiguration"/> class.
        /// </summary>
        public DiscoveryConfiguration()
        {
            SocketTimeout = DefaultSocketTimeout;
            AckTimeout = DefaultAckTimeout;
            MaxAckTimeout = DefaultMaxAckTimeout;
            NetworkTimeout = DefaultNetworkTimeout;
            JoinTimeout = DefaultJoinTimeout;
        }

        /// <summary>
        /// Gets or sets the IP finder which defines how nodes will find each other on the network.
        /// </summary>
        public IpFinder IpFinder { get; set; }

        /// <summary>
        /// Gets or sets the socket timeout.
        /// </summary>
        public TimeSpan SocketTimeout { get; set; }
        
        /// <summary>
        /// Gets or sets the timeout for receiving acknowledgement for sent message.
        /// </summary>
        public TimeSpan AckTimeout { get; set; }

        /// <summary>
        /// Gets or sets the maximum timeout for receiving acknowledgement for sent message.
        /// </summary>
        public TimeSpan MaxAckTimeout { get; set; }

        /// <summary>
        /// Gets or sets the network timeout.
        /// </summary>
        public TimeSpan NetworkTimeout { get; set; }
        
        /// <summary>
        /// Gets or sets the join timeout.
        /// </summary>
        public TimeSpan JoinTimeout { get; set; }

        /// <summary>
        /// Writes this instance to the specified writer.
        /// </summary>
        internal void Write(IBinaryRawWriter writer)
        {
            var ipFinder = IpFinder;

            if (ipFinder != null)
            {
                writer.WriteBoolean(true);

                IpFinder.Write(writer);
            }
            else
                writer.WriteBoolean(false);

            writer.WriteLong((long) SocketTimeout.TotalMilliseconds);
            writer.WriteLong((long) AckTimeout.TotalMilliseconds);
            writer.WriteLong((long) MaxAckTimeout.TotalMilliseconds);
            writer.WriteLong((long) NetworkTimeout.TotalMilliseconds);
            writer.WriteLong((long) JoinTimeout.TotalMilliseconds);
        }

        // TODO: Validate
    }
}
