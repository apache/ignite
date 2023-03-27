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
    using Apache.Ignite.Core.Impl.Binary;
    using Apache.Ignite.Core.Impl.Binary.IO;

    /// <summary>
    /// Response context.
    /// </summary>
    internal sealed class ClientResponseContext : ClientContextBase
    {
        /** */
        private BinaryReader _reader;

        /// <summary>
        /// Initializes a new instance of <see cref="ClientResponseContext"/> class.
        /// </summary>
        /// <param name="stream">Stream.</param>
        /// <param name="socket">Socket.</param>
        public ClientResponseContext(IBinaryStream stream, ClientSocket socket)
            : base(stream, socket)
        {
            // No-op.
        }

        /// <summary>
        /// Reader.
        /// </summary>
        public BinaryReader Reader
        {
            get { return _reader ?? (_reader = Marshaller.StartUnmarshal(Stream)); }
        }
    }
}