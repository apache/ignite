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
    /// <summary>
    /// Server endpoint retrieved with client discovery. 
    /// </summary>
    internal class ClientDiscoveryEndpoint
    {
        /** */
        private readonly string _address;
        
        /** */
        private readonly string _host;

        /** */
        private readonly int _port;

        /// <summary>
        /// Initializes a new instance of <see cref="ClientDiscoveryEndpoint"/>.
        /// </summary>
        public ClientDiscoveryEndpoint(string address, string host, int port)
        {
            _address = address;
            _host = host;
            _port = port;
        }

        /// <summary>
        /// Gets the address.
        /// </summary>
        public string Address
        {
            get { return _address; }
        }

        /// <summary>
        /// Gets the host.
        /// </summary>
        public string Host
        {
            get { return _host; }
        }

        /// <summary>
        /// Gets the port.
        /// </summary>
        public int Port
        {
            get { return _port; }
        }
    }
}