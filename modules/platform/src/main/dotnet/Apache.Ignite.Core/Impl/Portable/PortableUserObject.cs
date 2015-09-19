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

namespace Apache.Ignite.Core.Impl.Portable
{
    using System.Collections;
    using System.Collections.Generic;
    using System.IO;
    using System.Runtime.CompilerServices;
    using System.Text;
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Impl.Portable.IO;
    using Apache.Ignite.Core.Portable;

    /// <summary>
    /// User portable object.
    /// </summary>
    internal class PortableUserObject : IPortableObject
    {
        /** Marshaller. */
        private readonly PortableMarshaller _marsh;

        /** Raw data of this portable object. */
        private readonly byte[] _data;

        /** Offset in data array. */
        private readonly int _offset;

        /** Type ID. */
        private readonly int _typeId;

        /** Hash code. */
        private readonly int _hashCode;

        /** Fields. */
        private volatile IDictionary<int, int> _fields;

        /** Deserialized value. */
        private object _deserialized;

        /// <summary>
        /// Initializes a new instance of the <see cref="PortableUserObject"/> class.
        /// </summary>
        /// <param name="marsh">Marshaller.</param>
        /// <param name="data">Raw data of this portable object.</param>
        /// <param name="offset">Offset in data array.</param>
        /// <param name="typeId">Type ID.</param>
        /// <param name="hashCode">Hash code.</param>
        public PortableUserObject(PortableMarshaller marsh, byte[] data, int offset, int typeId, int hashCode)
        {
            _marsh = marsh;

            _data = data;
            _offset = offset;

            _typeId = typeId;
            _hashCode = hashCode;
        }

        /** <inheritdoc /> */
        public int TypeId()
        {
            return _typeId;
        }

        /** <inheritdoc /> */
        public T Field<T>(string fieldName)
        {
            return Field<T>(fieldName, null);
        }

        /** <inheritdoc /> */
        public T Deserialize<T>()
        {
            return Deserialize<T>(PortableMode.Deserialize);
        }

        /// <summary>
        /// Internal deserialization routine.
        /// </summary>
        /// <param name="mode">The mode.</param>
        /// <returns>
        /// Deserialized object.
        /// </returns>
        private T Deserialize<T>(PortableMode mode)
        {
            if (_deserialized == null)
            {
                IPortableStream stream = new PortableHeapStream(_data);

                stream.Seek(_offset, SeekOrigin.Begin);

                T res = _marsh.Unmarshal<T>(stream, mode);

                IPortableTypeDescriptor desc = _marsh.Descriptor(true, _typeId);

                if (!desc.KeepDeserialized)
                    return res;

                _deserialized = res;
            }

            return (T)_deserialized;
        }

        /** <inheritdoc /> */
        public IPortableMetadata Metadata()
        {
            return _marsh.Metadata(_typeId);
        }

        /// <summary>
        /// Raw data of this portable object.
        /// </summary>
        public byte[] Data
        {
            get { return _data; }
        }

        /// <summary>
        /// Offset in data array.
        /// </summary>
        public int Offset
        {
            get { return _offset; }
        }

        /// <summary>
        /// Get field with builder.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="fieldName"></param>
        /// <param name="builder"></param>
        /// <returns></returns>
        public T Field<T>(string fieldName, PortableBuilderImpl builder)
        {
            IPortableTypeDescriptor desc = _marsh.Descriptor(true, _typeId);

            InitializeFields();

            int fieldId = PortableUtils.FieldId(_typeId, fieldName, desc.NameConverter, desc.Mapper);

            int pos;

            if (_fields.TryGetValue(fieldId, out pos))
            {
                if (builder != null)
                {
                    // Read in scope of build process.
                    T res;

                    if (!builder.CachedField(pos, out res))
                    {
                        res = Field0<T>(pos, builder);

                        builder.CacheField(pos, res);
                    }

                    return res;
                }
                return Field0<T>(pos, null);
            }
            return default(T);
        }

        /// <summary>
        /// Lazy fields initialization routine.
        /// </summary>
        private void InitializeFields()
        {
            if (_fields == null)
            {
                IPortableStream stream = new PortableHeapStream(_data);

                stream.Seek(_offset + 14, SeekOrigin.Begin);

                int rawDataOffset = stream.ReadInt();

                _fields = PortableUtils.ObjectFields(stream, _typeId, rawDataOffset);
            }
        }

        /// <summary>
        /// Gets field value on the given object.
        /// </summary>
        /// <param name="pos">Position.</param>
        /// <param name="builder">Builder.</param>
        /// <returns>Field value.</returns>
        private T Field0<T>(int pos, PortableBuilderImpl builder)
        {
            IPortableStream stream = new PortableHeapStream(_data);

            stream.Seek(pos, SeekOrigin.Begin);

            return _marsh.Unmarshal<T>(stream, PortableMode.ForcePortable, builder);
        }

        /** <inheritdoc /> */
        public override int GetHashCode()
        {
            return _hashCode;
        }

        /** <inheritdoc /> */
        public override bool Equals(object obj)
        {
            if (this == obj)
                return true;

            PortableUserObject that = obj as PortableUserObject;

            if (that != null)
            {
                if (_data == that._data && _offset == that._offset)
                    return true;

                // 1. Check hash code and type IDs.
                if (_hashCode == that._hashCode && _typeId == that._typeId)
                {
                    // 2. Check if objects have the same field sets.
                    InitializeFields();
                    that.InitializeFields();

                    if (_fields.Keys.Count != that._fields.Keys.Count)
                        return false;

                    foreach (int id in _fields.Keys)
                    {
                        if (!that._fields.Keys.Contains(id))
                            return false;
                    }

                    // 3. Check if objects have the same field values.
                    foreach (KeyValuePair<int, int> field in _fields)
                    {
                        object fieldVal = Field0<object>(field.Value, null);
                        object thatFieldVal = that.Field0<object>(that._fields[field.Key], null);

                        if (!Equals(fieldVal, thatFieldVal))
                            return false;
                    }

                    // 4. Check if objects have the same raw data.
                    IPortableStream stream = new PortableHeapStream(_data);
                    stream.Seek(_offset + 10, SeekOrigin.Begin);
                    int len = stream.ReadInt();
                    int rawOffset = stream.ReadInt();

                    IPortableStream thatStream = new PortableHeapStream(that._data);
                    thatStream.Seek(_offset + 10, SeekOrigin.Begin);
                    int thatLen = thatStream.ReadInt();
                    int thatRawOffset = thatStream.ReadInt();

                    return PortableUtils.CompareArrays(_data, _offset + rawOffset, len - rawOffset, that._data,
                        that._offset + thatRawOffset, thatLen - thatRawOffset);
                }
            }

            return false;
        }

        /** <inheritdoc /> */
        public override string ToString()
        {
            return ToString(new Dictionary<int, int>());            
        }

        /// <summary>
        /// ToString implementation.
        /// </summary>
        /// <param name="handled">Already handled objects.</param>
        /// <returns>Object string.</returns>
        private string ToString(IDictionary<int, int> handled)
        {
            int idHash;

            bool alreadyHandled = handled.TryGetValue(_offset, out idHash);

            if (!alreadyHandled)
                idHash = RuntimeHelpers.GetHashCode(this);

            StringBuilder sb;

            IPortableTypeDescriptor desc = _marsh.Descriptor(true, _typeId);

            IPortableMetadata meta;

            try
            {
                meta = _marsh.Metadata(_typeId);
            }
            catch (IgniteException)
            {
                meta = null;
            }

            if (meta == null)
                sb = new StringBuilder("PortableObject [typeId=").Append(_typeId).Append(", idHash=" + idHash);
            else
            {
                sb = new StringBuilder(meta.TypeName).Append(" [idHash=" + idHash);

                if (!alreadyHandled)
                {
                    handled[_offset] = idHash;

                    InitializeFields();
                    
                    foreach (string fieldName in meta.Fields)
                    {
                        sb.Append(", ");

                        int fieldId = PortableUtils.FieldId(_typeId, fieldName, desc.NameConverter, desc.Mapper);

                        int fieldPos;

                        if (_fields.TryGetValue(fieldId, out fieldPos))
                        {
                            sb.Append(fieldName).Append('=');

                            ToString0(sb, Field0<object>(fieldPos, null), handled);
                        }
                    }
                }
                else
                    sb.Append(", ...");
            }

            sb.Append(']');

            return sb.ToString();
        }

        /// <summary>
        /// Internal ToString routine with correct collections printout.
        /// </summary>
        /// <param name="sb">String builder.</param>
        /// <param name="obj">Object to print.</param>
        /// <param name="handled">Already handled objects.</param>
        /// <returns>The same string builder.</returns>
        private static void ToString0(StringBuilder sb, object obj, IDictionary<int, int> handled)
        {
            IEnumerable col = (obj is string) ? null : obj as IEnumerable;

            if (col == null)
            {
                PortableUserObject obj0 = obj as PortableUserObject;

                sb.Append(obj0 == null ? obj : obj0.ToString(handled));
            }
            else
            {
                sb.Append('[');

                bool first = true;

                foreach (object elem in col)
                {
                    if (first)
                        first = false;
                    else
                        sb.Append(", ");

                    ToString0(sb, elem, handled);
                }

                sb.Append(']');
            }
        }
    }
}
