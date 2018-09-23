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

namespace Apache.Ignite.Core.Impl.Client
{
    using System.ComponentModel;
    using Apache.Ignite.Core.Client;

    /// <summary>
    /// Defines Ignite client endpoint.
    /// </summary>
    internal class Endpoint
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="Endpoint"/> class.
        /// </summary>
        public Endpoint()
        {
            Port = IgniteClientConfiguration.DefaultPort;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="Endpoint"/> class with specified host and default port
        /// (see <see cref="IgniteClientConfiguration.DefaultPort"/>).
        /// </summary>
        public Endpoint(string host) : this()
        {
            Host = host;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="Endpoint"/> class.
        /// </summary>
        public Endpoint(string host, int port) : this(host)
        {
            Port = port;
        }

        /// <summary>
        /// Gets or sets the host.
        /// </summary>
        public string Host { get; set; }

        /// <summary>
        /// Gets or sets the port.
        /// </summary>
        [DefaultValue(IgniteClientConfiguration.DefaultPort)]
        public int Port { get; set; }
    }
}
