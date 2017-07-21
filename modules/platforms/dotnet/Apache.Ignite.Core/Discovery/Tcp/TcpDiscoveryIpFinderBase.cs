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
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Discovery.Tcp.Multicast;
    using Apache.Ignite.Core.Discovery.Tcp.Static;

    /// <summary>
    /// Base IpFinder class.
    /// </summary>
    public abstract class TcpDiscoveryIpFinderBase : ITcpDiscoveryIpFinder
    {
        /** */
        protected const byte TypeCodeVmIpFinder = 1;

        /** */
        protected const byte TypeCodeMulticastIpFinder = 2;

        /// <summary>
        /// Initializes a new instance of the <see cref="TcpDiscoveryIpFinderBase"/> class.
        /// Prevents user-defined implementations.
        /// </summary>
        protected internal TcpDiscoveryIpFinderBase()
        {
            // No-op.
        }

        /// <summary>
        /// Writes this instance to the specified writer.
        /// </summary>
        /// <param name="writer">The writer.</param>
        internal virtual void Write(IBinaryRawWriter writer)
        {
            writer.WriteByte(TypeCode);
        }

        /// <summary>
        /// Gets the type code to be used in Java to determine ip finder type.
        /// </summary>
        protected abstract byte TypeCode { get; }

        /// <summary>
        /// Reads the instance.
        /// </summary>
        internal static TcpDiscoveryIpFinderBase ReadInstance(IBinaryRawReader reader)
        {
            var code = reader.ReadByte();

            switch (code)
            {
                case TypeCodeVmIpFinder:
                    return new TcpDiscoveryStaticIpFinder(reader);

                case TypeCodeMulticastIpFinder:
                    return new TcpDiscoveryMulticastIpFinder(reader);

                default:
                    return null;
            }
        }
    }
}