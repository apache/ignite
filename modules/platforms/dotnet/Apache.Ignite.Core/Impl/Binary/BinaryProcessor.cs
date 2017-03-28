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
    using System.Collections.Generic;
    using System.Diagnostics;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Impl.Binary.Metadata;
    using Apache.Ignite.Core.Impl.Unmanaged;

    /// <summary>
    /// Binary metadata processor.
    /// </summary>
    internal class BinaryProcessor : PlatformTarget
    {
        /// <summary>
        /// Op codes.
        /// </summary>
        private enum Op
        {
            GetMeta = 1,
            GetAllMeta = 2,
            PutMeta = 3,
            GetSchema = 4
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="BinaryProcessor"/> class.
        /// </summary>
        /// <param name="target">Target.</param>
        /// <param name="marsh">Marshaller.</param>
        public BinaryProcessor(IUnmanagedTarget target, Marshaller marsh) : base(target, marsh)
        {
            // No-op.
        }

        /// <summary>
        /// Gets metadata for specified type.
        /// </summary>
        public IBinaryType GetBinaryType(int typeId)
        {
            return DoOutInOp<IBinaryType>((int) Op.GetMeta,
                writer => writer.WriteInt(typeId),
                stream =>
                {
                    var reader = Marshaller.StartUnmarshal(stream, false);

                    return reader.ReadBoolean() ? new BinaryType(reader) : null;
                }
            );
        }

        /// <summary>
        /// Gets metadata for all known types.
        /// </summary>
        public List<IBinaryType> GetBinaryTypes()
        {
            return DoInOp((int) Op.GetAllMeta, s =>
            {
                var reader = Marshaller.StartUnmarshal(s);

                var size = reader.ReadInt();

                var res = new List<IBinaryType>(size);

                for (var i = 0; i < size; i++)
                    res.Add(reader.ReadBoolean() ? new BinaryType(reader) : null);

                return res;
            });
        }

        /// <summary>
        /// Gets the schema.
        /// </summary>
        public int[] GetSchema(int typeId, int schemaId)
        {
            return DoOutInOp<int[]>((int) Op.GetSchema, writer =>
            {
                writer.WriteInt(typeId);
                writer.WriteInt(schemaId);
            });
        }

        /// <summary>
        /// Put binary types to Grid.
        /// </summary>
        /// <param name="types">Binary types.</param>
        internal void PutBinaryTypes(ICollection<BinaryType> types)
        {
            DoOutOp((int) Op.PutMeta, w =>
            {
                w.WriteInt(types.Count);

                foreach (var meta in types)
                {
                    w.WriteInt(meta.TypeId);
                    w.WriteString(meta.TypeName);
                    w.WriteString(meta.AffinityKeyFieldName);

                    var fields = meta.GetFieldsMap();

                    w.WriteInt(fields.Count);

                    foreach (var field in fields)
                    {
                        w.WriteString(field.Key);
                        w.WriteInt(field.Value.TypeId);
                        w.WriteInt(field.Value.FieldId);
                    }

                    w.WriteBoolean(meta.IsEnum);

                    // Send schemas
                    var desc = meta.Descriptor;
                    Debug.Assert(desc != null);

                    var count = 0;
                    var countPos = w.Stream.Position;
                    w.WriteInt(0); // Reserve for count

                    foreach (var schema in desc.Schema.GetAll())
                    {
                        w.WriteInt(schema.Key);

                        var ids = schema.Value;
                        w.WriteInt(ids.Length);

                        foreach (var id in ids)
                            w.WriteInt(id);

                        count++;
                    }

                    w.Stream.WriteInt(countPos, count);
                }
            });

            Marshaller.OnBinaryTypesSent(types);
        }
    }
}
