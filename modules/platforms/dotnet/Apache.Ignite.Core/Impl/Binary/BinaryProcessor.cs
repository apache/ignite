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

namespace Apache.Ignite.Core.Impl.Binary
{
    using System.Collections.Generic;
    using System.Diagnostics;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Impl.Binary.Metadata;

    /// <summary>
    /// Binary metadata processor, delegates to PlatformBinaryProcessor in Java.
    /// </summary>
    internal class BinaryProcessor : PlatformTargetAdapter, IBinaryProcessor
    {
        /// <summary>
        /// Op codes.
        /// </summary>
        private enum Op
        {
            // ReSharper disable UnusedMember.Local
            GetMeta = 1,
            GetAllMeta = 2,
            PutMeta = 3,
            GetSchema = 4,
            RegisterType = 5,
            GetType = 6,
            RegisterEnum = 7,
            GetMetaWithSchemas = 8
            // ReSharper restore UnusedMember.Local
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="BinaryProcessor"/> class.
        /// </summary>
        /// <param name="target">Target.</param>
        public BinaryProcessor(IPlatformTargetInternal target) : base(target)
        {
            // No-op.
        }

        /// <summary>
        /// Gets metadata for specified type.
        /// </summary>
        public BinaryType GetBinaryType(int typeId)
        {
            return DoOutInOp((int) Op.GetMetaWithSchemas,
                writer => writer.WriteInt(typeId),
                stream =>
                {
                    var reader = Marshaller.StartUnmarshal(stream, false);

                    return reader.ReadBoolean() ? new BinaryType(reader, true) : null;
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
        /// Put binary types to Grid.
        /// </summary>
        /// <param name="types">Binary types.</param>
        public void PutBinaryTypes(ICollection<BinaryType> types)
        {
            DoOutOp((int) Op.PutMeta, w => WriteBinaryTypes(w, types));
        }

        /// <summary>
        /// Registers the type.
        /// </summary>
        /// <param name="id">The identifier.</param>
        /// <param name="typeName">The type name.</param>
        /// <returns>True if registration succeeded; otherwise, false.</returns>
        public bool RegisterType(int id, string typeName)
        {
            Debug.Assert(typeName != null);
            Debug.Assert(id != BinaryTypeId.Unregistered);

            return DoOutOp((int) Op.RegisterType, w =>
            {
                w.WriteInt(id);
                w.WriteString(typeName);
            }) == True;
        }

        /// <summary>
        /// Registers the enum.
        /// </summary>
        /// <param name="typeName">Name of the type.</param>
        /// <param name="values">The values.</param>
        /// <returns>Resulting binary type.</returns>
        public BinaryType RegisterEnum(string typeName, IEnumerable<KeyValuePair<string, int>> values)
        {
            Debug.Assert(typeName != null);

            return DoOutInOp((int) Op.RegisterEnum, w =>
            {
                w.WriteString(typeName);

                if (values == null)
                {
                    w.WriteInt(0);
                }
                else
                {
                    var countPos = w.Stream.Position;
                    w.WriteInt(0);
                    var count = 0;

                    foreach (var enumPair in values)
                    {
                        w.WriteString(enumPair.Key);
                        w.WriteInt(enumPair.Value);

                        count++;
                    }

                    w.Stream.WriteInt(countPos, count);
                }
            }, s => s.ReadBool() ? new BinaryType(Marshaller.StartUnmarshal(s)) : null);
        }

        /// <summary>
        /// Gets the type name by id.
        /// </summary>
        /// <param name="id">The identifier.</param>
        /// <returns>Type or null.</returns>
        public string GetTypeName(int id)
        {
            return DoOutInOp((int) Op.GetType, w => w.WriteInt(id), r => Marshaller.StartUnmarshal(r).ReadString());
        }

        /// <summary>
        /// Writes the binary types.
        /// </summary>
        public static void WriteBinaryTypes(BinaryWriter w, ICollection<BinaryType> types)
        {
            Debug.Assert(w != null);
            Debug.Assert(types != null);

            w.WriteInt(types.Count);

            foreach (var meta in types)
            {
                WriteBinaryType(w, meta);
            }
        }

        /// <summary>
        /// Writes the binary type.
        /// </summary>
        public static void WriteBinaryType(BinaryWriter w, BinaryType meta)
        {
            Debug.Assert(w != null);
            Debug.Assert(meta != null);

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

            // Enum data
            w.WriteBoolean(meta.IsEnum);

            if (meta.IsEnum)
            {
                if (meta.EnumValuesMap != null)
                {
                    w.WriteInt(meta.EnumValuesMap.Count);

                    foreach (var pair in meta.EnumValuesMap)
                    {
                        w.WriteString(pair.Key);
                        w.WriteInt(pair.Value);
                    }
                }
                else
                {
                    w.WriteInt(0);
                }
            }

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
    }
}
