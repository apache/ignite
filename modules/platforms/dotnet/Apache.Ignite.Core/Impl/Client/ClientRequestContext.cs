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
    /// Request context.
    /// </summary>
    internal sealed class ClientRequestContext : ClientContextBase
    {
        /** */
        private BinaryWriter _writer;

        /// <summary>
        /// Initializes a new instance of <see cref="ClientRequestContext"/> class.
        /// </summary>
        /// <param name="stream">Stream.</param>
        /// <param name="socket">Socket.</param>
        public ClientRequestContext(IBinaryStream stream, ClientSocket socket)
            : base(stream, socket)

        {
            // No-op.
        }

        /// <summary>
        /// Writer.
        /// </summary>
        public BinaryWriter Writer
        {
            get { return _writer ?? (_writer = Marshaller.StartMarshal(Stream)); }
        }

        /// <summary>
        /// Finishes marshal session for this request (if any).
        /// </summary>
        public void FinishMarshal()
        {
            if (_writer != null)
            {
                Marshaller.FinishMarshal(_writer);
            }
        }
    }
}