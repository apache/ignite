﻿/*
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

namespace Apache.Ignite.Core.Impl.Binary
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Client;
    using Apache.Ignite.Core.Impl.Binary.Metadata;
    using Apache.Ignite.Core.Impl.Client;

    /// <summary>
    /// Thin client binary processor.
    /// </summary>
    internal class BinaryProcessorClient : IBinaryProcessor
    {
        /** Socket. */
        private readonly ClientFailoverSocket _socket;

        /// <summary>
        /// Initializes a new instance of the <see cref="BinaryProcessorClient"/> class.
        /// </summary>
        /// <param name="socket">The socket.</param>
        public BinaryProcessorClient(ClientFailoverSocket socket)
        {
            Debug.Assert(socket != null);

            _socket = socket;
        }

        /** <inheritdoc /> */
        public BinaryType GetBinaryType(int typeId)
        {
            return _socket.DoOutInOp(ClientOp.BinaryTypeGet, ctx => ctx.Stream.WriteInt(typeId),
                ctx => ctx.Stream.ReadBool() ? new BinaryType(ctx.Reader, true) : null);
        }

        /** <inheritdoc /> */
        public List<IBinaryType> GetBinaryTypes()
        {
            throw IgniteClient.GetClientNotSupportedException();
        }

        /** <inheritdoc /> */
        public void PutBinaryTypes(ICollection<BinaryType> types)
        {
            Debug.Assert(types != null);

            foreach (var binaryType in types)
            {
                var type = binaryType;  // Access to modified closure.

                _socket.DoOutInOp<object>(ClientOp.BinaryTypePut,
                    ctx => BinaryProcessor.WriteBinaryType(ctx.Writer, type), null);
            }
        }

        /** <inheritdoc /> */
        public bool RegisterType(int id, string typeName, bool registerSameJavaType)
        {
            var res = _socket.DoOutInOp(ClientOp.BinaryTypeNamePut, ctx =>
            {
                ctx.Stream.WriteByte(BinaryProcessor.DotNetPlatformId);
                ctx.Stream.WriteInt(id);
                ctx.Writer.WriteString(typeName);
            }, ctx => ctx.Stream.ReadBool());

            if (registerSameJavaType && res)
            {
                res = _socket.DoOutInOp(ClientOp.BinaryTypeNamePut, ctx =>
                {
                    ctx.Stream.WriteByte(BinaryProcessor.JavaPlatformId);
                    ctx.Stream.WriteInt(id);
                    ctx.Writer.WriteString(typeName);
                }, ctx => ctx.Stream.ReadBool());
            }

            return res;
        }

        /** <inheritdoc /> */
        public BinaryType RegisterEnum(string typeName, IEnumerable<KeyValuePair<string, int>> values)
        {
            throw IgniteClient.GetClientNotSupportedException();
        }

        /** <inheritdoc /> */
        public string GetTypeName(int id, byte platformId, Func<Exception, string> errorFunc = null)
        {
            return _socket.DoOutInOp(ClientOp.BinaryTypeNameGet, ctx =>
                {
                    ctx.Stream.WriteByte(platformId);
                    ctx.Stream.WriteInt(id);
                },
                ctx => ctx.Reader.ReadString(),
                errorFunc == null
                    ? (Func<ClientStatusCode, string, string>) null
                    : (statusCode, msg) => errorFunc(new BinaryObjectException(msg)));
        }
    }
}
