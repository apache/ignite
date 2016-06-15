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

namespace Apache.Ignite.Core.Discovery.Tcp.Static
{
    using System.Collections.Generic;
    using System.Diagnostics.CodeAnalysis;
    using Apache.Ignite.Core.Binary;

    /// <summary>
    /// IP Finder which works only with pre-configured list of IP addresses.
    /// </summary>
    public class TcpDiscoveryStaticIpFinder : TcpDiscoveryIpFinderBase
    {
        /// <summary>
        /// Gets or sets the end points.
        /// </summary>
        [SuppressMessage("Microsoft.Usage", "CA2227:CollectionPropertiesShouldBeReadOnly")]
        public ICollection<string> Endpoints { get; set; }

        /// <summary>
        /// Initializes a new instance of the <see cref="TcpDiscoveryStaticIpFinder"/> class.
        /// </summary>
        public TcpDiscoveryStaticIpFinder()
        {
            // No-op.
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="TcpDiscoveryStaticIpFinder"/> class.
        /// </summary>
        /// <param name="reader">The reader.</param>
        internal TcpDiscoveryStaticIpFinder(IBinaryRawReader reader)
        {
            var count = reader.ReadInt();

            if (count > 0)
            {
                Endpoints = new List<string>(count);

                for (int i = 0; i < count; i++)
                    Endpoints.Add(reader.ReadString());
            }
        }

        /** <inheritdoc /> */
        internal override void Write(IBinaryRawWriter writer)
        {
            base.Write(writer);

            var eps = Endpoints;

            if (eps != null)
            {
                writer.WriteInt(eps.Count);

                foreach (var ep in eps)
                    writer.WriteString(ep);
            }
            else
                writer.WriteInt(0);
        }

        /// <summary>
        /// Gets the type code to be used in Java to determine ip finder type.
        /// </summary>
        protected override byte TypeCode
        {
            get { return TypeCodeVmIpFinder; }
        }
    }
}