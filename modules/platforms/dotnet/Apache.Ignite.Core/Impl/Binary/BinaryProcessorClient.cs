/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

namespace Apache.Ignite.Core.Impl.Binary
{
    using System.Collections.Generic;
    using System.Diagnostics;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Impl.Binary.Metadata;
    using Apache.Ignite.Core.Impl.Client;

    /// <summary>
    /// Thin client binary processor.
    /// </summary>
    internal class BinaryProcessorClient : IBinaryProcessor
    {
        /** Type registry platform id. See org.apache.ignite.internal.MarshallerPlatformIds in Java. */
        private const byte DotNetPlatformId = 1;

        /** Socket. */
        private readonly IClientSocket _socket;

        /** Marshaller. */
        private readonly Marshaller _marsh = BinaryUtils.Marshaller;

        /// <summary>
        /// Initializes a new instance of the <see cref="BinaryProcessorClient"/> class.
        /// </summary>
        /// <param name="socket">The socket.</param>
        public BinaryProcessorClient(IClientSocket socket)
        {
            Debug.Assert(socket != null);

            _socket = socket;
        }

        /** <inheritdoc /> */
        public BinaryType GetBinaryType(int typeId)
        {
            return _socket.DoOutInOp(ClientOp.BinaryTypeGet, s => s.WriteInt(typeId),
                s => s.ReadBool() ? new BinaryType(_marsh.StartUnmarshal(s), true) : null);
        }

        /** <inheritdoc /> */
        public List<IBinaryType> GetBinaryTypes()
        {
            throw IgniteClient.GetClientNotSupportedException();
        }

        /** <inheritdoc /> */
        public int[] GetSchema(int typeId, int schemaId)
        {
            return GetBinaryType(typeId).Schema.Get(schemaId);
        }

        /** <inheritdoc /> */
        public void PutBinaryTypes(ICollection<BinaryType> types)
        {
            Debug.Assert(types != null);

            foreach (var binaryType in types)
            {
                var type = binaryType;  // Access to modified closure.

                _socket.DoOutInOp<object>(ClientOp.BinaryTypePut,
                    s => BinaryProcessor.WriteBinaryType(_marsh.StartMarshal(s), type), null);
            }
        }

        /** <inheritdoc /> */
        public bool RegisterType(int id, string typeName)
        {
            return _socket.DoOutInOp(ClientOp.BinaryTypeNamePut, s =>
            {
                s.WriteByte(DotNetPlatformId);
                s.WriteInt(id);
                _marsh.StartMarshal(s).WriteString(typeName);
            }, s => s.ReadBool());
        }

        /** <inheritdoc /> */
        public BinaryType RegisterEnum(string typeName, IEnumerable<KeyValuePair<string, int>> values)
        {
            throw IgniteClient.GetClientNotSupportedException();
        }

        /** <inheritdoc /> */
        public string GetTypeName(int id)
        {
            return _socket.DoOutInOp(ClientOp.BinaryTypeNameGet, s =>
                {
                    s.WriteByte(DotNetPlatformId);
                    s.WriteInt(id);
                },
                s => _marsh.StartUnmarshal(s).ReadString());
        }
    }
}