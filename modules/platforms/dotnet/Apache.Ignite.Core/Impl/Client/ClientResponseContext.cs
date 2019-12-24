/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
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
        /// <param name="marshaller">Marshaller.</param>
        /// <param name="protocolVersion">Protocol version to be used for this response.</param>
        public ClientResponseContext(IBinaryStream stream, Marshaller marshaller, ClientProtocolVersion protocolVersion)
            : base(stream, marshaller, protocolVersion)
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