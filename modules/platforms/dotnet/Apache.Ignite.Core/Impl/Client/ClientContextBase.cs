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
    using System.Diagnostics;
    using Apache.Ignite.Core.Impl.Binary;
    using Apache.Ignite.Core.Impl.Binary.IO;

    /// <summary>
    /// Base class for client context.
    /// </summary>
    internal abstract class ClientContextBase
    {
        /** */
        private readonly IBinaryStream _stream;

        /** */
        private readonly Marshaller _marshaller;

        /** */
        private readonly ClientProtocolVersion _protocolVersion;
        
        /// <summary>
        /// Initializes a new instance of <see cref="ClientContextBase"/> class.
        /// </summary>
        /// <param name="stream">Stream.</param>
        /// <param name="marshaller">Marshaller.</param>
        /// <param name="protocolVersion">Protocol version to be used for this request.</param>
        protected ClientContextBase(IBinaryStream stream, Marshaller marshaller, ClientProtocolVersion protocolVersion)
        {
            Debug.Assert(stream != null);
            Debug.Assert(marshaller != null);
            
            _stream = stream;
            _marshaller = marshaller;
            _protocolVersion = protocolVersion;
        }

        /// <summary>
        /// Stream.
        /// </summary>
        public IBinaryStream Stream
        {
            get { return _stream; }
        }

        /// <summary>
        /// Gets the marshaller.
        /// </summary>
        public Marshaller Marshaller
        {
            get { return _marshaller; }
        }
        
        /// <summary>
        /// Protocol version to be used for this request.
        /// (Takes partition awareness, failover and reconnect into account).
        /// </summary>
        public ClientProtocolVersion ProtocolVersion
        {
            get { return _protocolVersion; }
        }
    }
}