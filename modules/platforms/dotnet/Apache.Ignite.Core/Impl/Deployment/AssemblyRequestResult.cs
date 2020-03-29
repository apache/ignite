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

namespace Apache.Ignite.Core.Impl.Deployment
{
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Impl.Binary;

    /// <summary>
    /// Peer assembly request result.
    /// </summary>
    internal class AssemblyRequestResult : IBinaryWriteAware
    {
        /** Assembly bytes. */
        private readonly byte[] _assemblyBytes;

        /** Error message. */
        private readonly string _message;

        /// <summary>
        /// Initializes a new instance of the <see cref="AssemblyRequestResult" /> class.
        /// </summary>
        /// <param name="assemblyBytes">The assembly bytes.</param>
        /// <param name="message">The message.</param>
        public AssemblyRequestResult(byte[] assemblyBytes, string message)
        {
            _assemblyBytes = assemblyBytes;
            _message = message;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="AssemblyRequestResult"/> class.
        /// </summary>
        /// <param name="reader">The reader.</param>
        public AssemblyRequestResult(IBinaryRawReader reader)
        {
            _assemblyBytes = reader.ReadByteArray();
            _message = reader.ReadString();
        }

        /** <inheritdoc /> */
        public void WriteBinary(IBinaryWriter writer)
        {
            var raw = writer.GetRawWriter();

            raw.WriteByteArray(_assemblyBytes);
            raw.WriteString(_message);
        }

        /// <summary>
        /// Gets the assembly bytes.
        /// </summary>
        public byte[] AssemblyBytes
        {
            get { return _assemblyBytes; }
        }

        /// <summary>
        /// Gets the message.
        /// </summary>
        public string Message
        {
            get { return _message; }
        }
    }
}
