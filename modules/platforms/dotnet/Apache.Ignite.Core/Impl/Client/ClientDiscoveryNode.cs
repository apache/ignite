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
        private readonly IList<string> _endpoints;

        public ClientDiscoveryNode(Guid id, IList<string> endpoints)
        {
            Debug.Assert(endpoints != null);
            Debug.Assert(endpoints.Count > 0);
            
            _id = id;
            _endpoints = endpoints;
        }

        /// <summary>
        /// Gets the id.
        /// </summary>
        public Guid Id
        {
            get { return _id; }
        }

        /// <summary>
        /// Gets the endpoints.
        /// </summary>
        public IList<string> Endpoints
        {
            get { return _endpoints; }
        }
    }
}