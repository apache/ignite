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

namespace Apache.Ignite.Core.Client
{
    using System;
    using System.Collections.Generic;
    using System.ComponentModel;
    using System.Diagnostics.CodeAnalysis;
    using System.Linq;
    using System.Xml;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Impl.Binary;
    using Apache.Ignite.Core.Impl.Client;
    using Apache.Ignite.Core.Impl.Common;
    using Apache.Ignite.Core.Log;

    /// <summary>
    /// Ignite thin client configuration.
    /// <para />
    /// Ignite thin client connects to a specific Ignite node with a socket and does not start JVM in process.
    /// This configuration should correspond to <see cref="IgniteConfiguration.ClientConnectorConfiguration"/>
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
        /// Default socket timeout.
        /// </summary>
        public static readonly TimeSpan DefaultSocketTimeout = TimeSpan.FromMilliseconds(5000);

        /// <summary>
        /// Initializes a new instance of the <see cref="IgniteClientConfiguration"/> class.
        /// </summary>
        public IgniteClientConfiguration()
        {
#pragma warning disable 618
            Port = DefaultPort;
#pragma warning restore 618
            SocketSendBufferSize = DefaultSocketBufferSize;
            SocketReceiveBufferSize = DefaultSocketBufferSize;
            TcpNoDelay = DefaultTcpNoDelay;
            SocketTimeout = DefaultSocketTimeout;
            Logger = new ConsoleLogger();
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="IgniteClientConfiguration" /> class.
        /// </summary>
        /// <param name="host">The host to connect to.</param>
        public IgniteClientConfiguration(string host) : this()
        {
            IgniteArgumentCheck.NotNull(host, "host");

            Endpoints = new List<string> {host};
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="IgniteClientConfiguration"/> class.
        /// </summary>
        /// <param name="cfg">The configuration to copy.</param>
        public IgniteClientConfiguration(IgniteClientConfiguration cfg) : this()
        {
            if (cfg == null)
            {
                return;
            }

#pragma warning disable 618
            Host = cfg.Host;
            Port = cfg.Port;
#pragma warning restore 618
            SocketSendBufferSize = cfg.SocketSendBufferSize;
            SocketReceiveBufferSize = cfg.SocketReceiveBufferSize;
            TcpNoDelay = cfg.TcpNoDelay;
            SocketTimeout = cfg.SocketTimeout;

            if (cfg.BinaryConfiguration != null)
            {
                BinaryConfiguration = new BinaryConfiguration(cfg.BinaryConfiguration);
            }

            BinaryProcessor = cfg.BinaryProcessor;
            SslStreamFactory = cfg.SslStreamFactory;

            UserName = cfg.UserName;
            Password = cfg.Password;
            Endpoints = cfg.Endpoints == null ? null : cfg.Endpoints.ToList();
            ReconnectDisabled = cfg.ReconnectDisabled;
            EnablePartitionAwareness = cfg.EnablePartitionAwareness;
            Logger = cfg.Logger;
            ProtocolVersion = cfg.ProtocolVersion;
        }

        /// <summary>
        /// Gets or sets the host. Should not be null.
        /// </summary>
        [Obsolete("Use Endpoints instead")]
        public string Host { get; set; }

        /// <summary>
        /// Gets or sets the port.
        /// </summary>
        [DefaultValue(DefaultPort)]
        [Obsolete("Use Endpoints instead")]
        public int Port { get; set; }

        /// <summary>
        /// Gets or sets endpoints to connect to.
        /// Examples of supported formats:
        ///  * 192.168.1.25 (default port is used, see <see cref="DefaultPort"/>).
        ///  * 192.168.1.25:780 (custom port)
        ///  * 192.168.1.25:780..787 (custom port range)
        ///  * my-host.com (default port is used, see <see cref="DefaultPort"/>).
        ///  * my-host.com:780 (custom port)
        ///  * my-host.com:780..787 (custom port range)
        /// <para />
        /// When multiple endpoints are specified, failover and load-balancing mechanism is enabled:
        /// * Ignite picks random endpoint and connects to it.
        /// * On disconnect, next endpoint is picked from the list (.
        /// </summary>
        [SuppressMessage("Microsoft.Usage", "CA2227:CollectionPropertiesShouldBeReadOnly")]
        public ICollection<string> Endpoints { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether automatic reconnect is disabled.
        /// </summary>
        public bool ReconnectDisabled { get; set; }

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
        /// Gets or sets the socket operation timeout. Zero or negative means infinite timeout.
        /// </summary>
        [DefaultValue(typeof(TimeSpan), "00:00:05")]
        public TimeSpan SocketTimeout { get; set; }

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
        /// Gets or sets the SSL stream factory.
        /// <para />
        /// When not null, secure socket connection will be established.
        /// </summary>
        public ISslStreamFactory SslStreamFactory { get; set; }

        /// <summary>
        /// Username to be used to connect to secured cluster.
        /// </summary>
        public string UserName { get; set; }

        /// <summary>
        /// Password to be used to connect to secured cluster.
        /// </summary>
        public string Password { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether partition awareness should be enabled.
        /// <para />
        /// Default is false: only one connection is established at a given moment to a random server node.
        /// When true: for cache operations, Ignite client attempts to send the request directly to
        /// the primary node for the given cache key.
        /// To do so, connection is established to every known server node at all times.
        /// </summary>
        public bool EnablePartitionAwareness { get; set; }
        
        /// <summary>
        /// Gets or sets the logger.
        /// Default is <see cref="ConsoleLogger"/>. Set to <c>null</c> to disable logging.
        /// </summary>
        public ILogger Logger { get; set; }

        /// <summary>
        /// Gets or sets custom binary processor. Internal property for tests.
        /// </summary>
        internal IBinaryProcessor BinaryProcessor { get; set; }

        /// <summary>
        /// Gets or sets protocol version. Internal property for tests.
        /// </summary>
        internal ClientProtocolVersion? ProtocolVersion { get; set; }

        /// <summary>
        /// Serializes this instance to the specified XML writer.
        /// </summary>
        /// <param name="writer">The writer.</param>
        /// <param name="rootElementName">Name of the root element.</param>
        public void ToXml(XmlWriter writer, string rootElementName)
        {
            IgniteConfigurationXmlSerializer.Serialize(this, writer, rootElementName);
        }

        /// <summary>
        /// Serializes this instance to an XML string.
        /// </summary>
        public string ToXml()
        {
            return IgniteConfigurationXmlSerializer.Serialize(this, "igniteClientConfiguration");
        }

        /// <summary>
        /// Deserializes IgniteClientConfiguration from the XML reader.
        /// </summary>
        /// <param name="reader">The reader.</param>
        /// <returns>Deserialized instance.</returns>
        public static IgniteClientConfiguration FromXml(XmlReader reader)
        {
            return IgniteConfigurationXmlSerializer.Deserialize<IgniteClientConfiguration>(reader);
        }

        /// <summary>
        /// Deserializes IgniteClientConfiguration from the XML string.
        /// </summary>
        /// <param name="xml">Xml string.</param>
        /// <returns>Deserialized instance.</returns>
        public static IgniteClientConfiguration FromXml(string xml)
        {
            return IgniteConfigurationXmlSerializer.Deserialize<IgniteClientConfiguration>(xml);
        }
    }
}
