/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

namespace Apache.Ignite.Internal.Table.Serialization
{
    using System;
    using System.Reflection;
    using Buffers;
    using MessagePack;
    using Proto;

    /// <summary>
    /// Object serializer handler.
    /// </summary>
    /// <typeparam name="T">Object type.</typeparam>
    internal class ObjectSerializerHandler<T> : IRecordSerializerHandler<T>
        where T : class
    {
        /// <inheritdoc/>
        public T Read(ref MessagePackReader reader, Schema schema, bool keyOnly = false)
        {
            // TODO: Emit code for efficient serialization (IGNITE-16341).
            var columns = schema.Columns;
            var count = keyOnly ? schema.KeyColumnCount : columns.Count;
            var res = Activator.CreateInstance<T>();
            var type = typeof(T);

            for (var index = 0; index < count; index++)
            {
                if (reader.TryReadNoValue())
                {
                    continue;
                }

                var col = columns[index];
                var prop = GetPropertyIgnoreCase(type, col.Name);

                if (prop != null)
                {
                    var value = reader.ReadObject(col.Type);
                    prop.SetValue(res, value);
                }
                else
                {
                    reader.Skip();
                }
            }

            return (T)(object)res;
        }

        /// <inheritdoc/>
        public T ReadValuePart(PooledBuffer buf, Schema schema, T key)
        {
            // TODO: Emit code for efficient serialization (IGNITE-16341).
            // Skip schema version.
            var r = buf.GetReader();
            r.Skip();

            var columns = schema.Columns;
            var res = Activator.CreateInstance<T>();
            var type = typeof(T);

            for (var i = 0; i < columns.Count; i++)
            {
                var col = columns[i];
                var prop = GetPropertyIgnoreCase(type, col.Name);

                if (i < schema.KeyColumnCount)
                {
                    if (prop != null)
                    {
                        prop.SetValue(res, prop.GetValue(key));
                    }
                }
                else
                {
                    if (r.TryReadNoValue())
                    {
                        continue;
                    }

                    if (prop != null)
                    {
                        prop.SetValue(res, r.ReadObject(col.Type));
                    }
                    else
                    {
                        r.Skip();
                    }
                }
            }

            return res;
        }

        /// <inheritdoc/>
        public void Write(ref MessagePackWriter writer, Schema schema, T record, bool keyOnly = false)
        {
            // TODO: Emit code for efficient serialization (IGNITE-16341).
            var columns = schema.Columns;
            var count = keyOnly ? schema.KeyColumnCount : columns.Count;
            var type = record.GetType();

            for (var index = 0; index < count; index++)
            {
                var col = columns[index];
                var prop = GetPropertyIgnoreCase(type, col.Name);

                if (prop == null)
                {
                    writer.WriteNoValue();
                }
                else
                {
                    writer.WriteObject(prop.GetValue(record));
                }
            }
        }

        private static PropertyInfo? GetPropertyIgnoreCase(Type type, string name)
        {
            // TODO: Use fields, not properties (IGNITE-16341).
            foreach (var p in type.GetProperties())
            {
                if (p.Name.Equals(name, StringComparison.OrdinalIgnoreCase))
                {
                    return p;
                }
            }

            return null;
        }
    }
}
