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
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;

    /// <summary>
    /// Represents a discovered node.
    /// </summary>
    internal class ClientDiscoveryNode
    {
        /** */
        private readonly Guid _id;

        /** */
        private readonly int _port;
        
        /** */
        private readonly IList<string> _addresses;

        /// <summary>
        /// Initializes a new instance of <see cref="ClientDiscoveryNode"/>.
        /// </summary>
        public ClientDiscoveryNode(Guid id, int port, IList<string> addresses)
        {
            Debug.Assert(addresses != null);
            Debug.Assert(addresses.Count > 0);
            
            _id = id;
            _port = port;
            _addresses = addresses;
        }

        /// <summary>
        /// Gets the id.
        /// </summary>
        public Guid Id
        {
            get { return _id; }
        }

        /// <summary>
        /// Gets the port.
        /// </summary>
        public int Port
        {
            get { return _port; }
        }

        /// <summary>
        /// Gets the addresses - IPs or host names.
        /// </summary>
        public IList<string> Addresses
        {
            get { return _addresses; }
        }
    }
}