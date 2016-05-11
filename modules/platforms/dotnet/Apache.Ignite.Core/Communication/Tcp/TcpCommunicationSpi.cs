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

        /// <summary> Default value of <see cref="ConnectTimeout"/> property. </summary>
        public static readonly TimeSpan DefaultConnectTimeout = TimeSpan.FromMilliseconds(5000);

        /// <summary> Default value of <see cref="DirectBuffer"/> property. </summary>
        public const bool DefaultDirectBuffer = true;

        /// <summary> Default value of <see cref="DirectSendBuffer"/> property. </summary>
        public const bool DefaultDirectSendBuffer = false;

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
        public bool DirectBuffer { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether to allocate direct (ByteBuffer.allocateDirect) 
        /// or heap (ByteBuffer.allocate) send buffer.
        /// </summary>
        public bool DirectSendBuffer { get; set; }


        public int LocalPort { get; set; }
        public int LocalPortRange { get; set; }
    }
}
