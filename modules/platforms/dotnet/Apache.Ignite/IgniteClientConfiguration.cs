/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

namespace Apache.Ignite
{
    using System;
    using System.Collections.Generic;
    using System.ComponentModel;
    using System.Linq;
    using Internal.Common;
    using Log;

    /// <summary>
    /// Ignite client driver configuration.
    /// </summary>
    public sealed record IgniteClientConfiguration
    {
        /// <summary>
        /// Default port.
        /// </summary>
        public const int DefaultPort = 10800;

        /// <summary>
        /// Default socket timeout.
        /// </summary>
        public static readonly TimeSpan DefaultSocketTimeout = TimeSpan.FromSeconds(5);

        /// <summary>
        /// Initializes a new instance of the <see cref="IgniteClientConfiguration"/> class.
        /// </summary>
        public IgniteClientConfiguration()
        {
            // No-op.
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="IgniteClientConfiguration"/> class.
        /// </summary>
        /// <param name="endpoints">Endpoints.</param>
        public IgniteClientConfiguration(params string[] endpoints)
            : this()
        {
            IgniteArgumentCheck.NotNull(endpoints, nameof(endpoints));

            foreach (var endpoint in endpoints)
            {
                Endpoints.Add(endpoint);
            }
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="IgniteClientConfiguration"/> class.
        /// </summary>
        /// <param name="other">Other configuration.</param>
        public IgniteClientConfiguration(IgniteClientConfiguration other)
        {
            IgniteArgumentCheck.NotNull(other, nameof(other));

            Logger = other.Logger;
            SocketTimeout = other.SocketTimeout;
            Endpoints = other.Endpoints.ToList();
        }

        /// <summary>
        /// Gets or sets the logger.
        /// </summary>
        public IIgniteLogger? Logger { get; set; }

        /// <summary>
        /// Gets or sets the socket operation timeout. Zero or negative means infinite timeout.
        /// </summary>
        [DefaultValue(typeof(TimeSpan), "00:00:05")]
        public TimeSpan SocketTimeout { get; set; } = DefaultSocketTimeout;

        /// <summary>
        /// Gets endpoints to connect to.
        /// Examples of supported formats:
        ///  * 192.168.1.25 (default port is used, see <see cref="DefaultPort"/>).
        ///  * 192.168.1.25:780 (custom port)
        ///  * 192.168.1.25:780..787 (custom port range)
        ///  * my-host.com (default port is used, see <see cref="DefaultPort"/>).
        ///  * my-host.com:780 (custom port)
        ///  * my-host.com:780..787 (custom port range).
        /// </summary>
        public IList<string> Endpoints { get; } = new List<string>();
    }
}
