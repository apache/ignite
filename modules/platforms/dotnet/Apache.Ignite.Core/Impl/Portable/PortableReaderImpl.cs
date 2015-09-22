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
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.Diagnostics.CodeAnalysis;
    using System.IO;
    using System.Runtime.Serialization;
    using Apache.Ignite.Core.Impl.Common;
    using Apache.Ignite.Core.Impl.Portable.IO;
    using Apache.Ignite.Core.Portable;

    /// <summary>
    /// Portable reader implementation. 
    /// </summary>
    internal class PortableReaderImpl : IPortableReader, IPortableRawReader
    {
        /** Marshaller. */
        private readonly PortableMarshaller _marsh;

        /** Type descriptors. */
        private readonly IDictionary<long, IPortableTypeDescriptor> _descs;

        /** Parent builder. */
        private readonly PortableBuilderImpl _builder;

        /** Handles. */
        private PortableReaderHandleDictionary _hnds;

        /** Current type ID. */
        private int _curTypeId;

        /** Current position. */
        private int _curPos;

        /** Current raw data offset. */
        private int _curRawOffset;

        /** Current converter. */
        private IPortableNameMapper _curConverter;

        /** Current mapper. */
        private IPortableIdMapper _curMapper;

        /** Current raw flag. */
        private bool _curRaw;

        /** Detach flag. */
        private bool _detach;

        /** Portable read mode. */
        private PortableMode _mode;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="marsh">Marshaller.</param>
        /// <param name="descs">Descriptors.</param>
        /// <param name="stream">Input stream.</param>
        /// <param name="mode">The mode.</param>
        /// <param name="builder">Builder.</param>
        public PortableReaderImpl
            (PortableMarshaller marsh,
            IDictionary<long, IPortableTypeDescriptor> descs, 
            IPortableStream stream, 
            PortableMode mode,
            PortableBuilderImpl builder)
        {
            _marsh = marsh;
            _descs = descs;
            _mode = mode;
            _builder = builder;

            Stream = stream;
        }

        /// <summary>
        /// Gets the marshaller.
        /// </summary>
        public PortableMarshaller Marshaller
        {
            get { return _marsh; }
        }

        /** <inheritdoc /> */
        public IPortableRawReader RawReader()
        {
            MarkRaw();

            return this;
        }

        /** <inheritdoc /> */
        public bool ReadBoolean(string fieldName)
        {
            return ReadField(fieldName, r => r.ReadBoolean());
        }

        /** <inheritdoc /> */
        public bool ReadBoolean()
        {
            return Stream.ReadBool();
        }

        /** <inheritdoc /> */
        public bool[] ReadBooleanArray(string fieldName)
        {
            return ReadField(fieldName, PortableUtils.ReadBooleanArray);
        }

        /** <inheritdoc /> */
        public bool[] ReadBooleanArray()
        {
            return Read(PortableUtils.ReadBooleanArray);
        }

        /** <inheritdoc /> */
        public byte ReadByte(string fieldName)
        {
            return ReadField(fieldName, ReadByte);
        }

        /** <inheritdoc /> */
        public byte ReadByte()
        {
            return Stream.ReadByte();
        }

        /** <inheritdoc /> */
        public byte[] ReadByteArray(string fieldName)
        {
            return ReadField(fieldName, PortableUtils.ReadByteArray);
        }

        /** <inheritdoc /> */
        public byte[] ReadByteArray()
        {
            return Read(PortableUtils.ReadByteArray);
        }

        /** <inheritdoc /> */
        public short ReadShort(string fieldName)
        {
            return ReadField(fieldName, ReadShort);
        }

        /** <inheritdoc /> */
        public short ReadShort()
        {
            return Stream.ReadShort();
        }

        /** <inheritdoc /> */
        public short[] ReadShortArray(string fieldName)
        {
            return ReadField(fieldName, PortableUtils.ReadShortArray);
        }

        /** <inheritdoc /> */
        public short[] ReadShortArray()
        {
            return Read(PortableUtils.ReadShortArray);
        }

        /** <inheritdoc /> */
        public char ReadChar(string fieldName)
        {
            return ReadField(fieldName, ReadChar);
        }

        /** <inheritdoc /> */
        public char ReadChar()
        {
            return Stream.ReadChar();
        }

        /** <inheritdoc /> */
        public char[] ReadCharArray(string fieldName)
        {
            return ReadField(fieldName, PortableUtils.ReadCharArray);
        }

        /** <inheritdoc /> */
        public char[] ReadCharArray()
        {
            return Read(PortableUtils.ReadCharArray);
        }

        /** <inheritdoc /> */
        public int ReadInt(string fieldName)
        {
            return ReadField(fieldName, ReadInt);
        }

        /** <inheritdoc /> */
        public int ReadInt()
        {
            return Stream.ReadInt();
        }

        /** <inheritdoc /> */
        public int[] ReadIntArray(string fieldName)
        {
            return ReadField(fieldName, PortableUtils.ReadIntArray);
        }

        /** <inheritdoc /> */
        public int[] ReadIntArray()
        {
            return Read(PortableUtils.ReadIntArray);
        }

        /** <inheritdoc /> */
        public long ReadLong(string fieldName)
        {
            return ReadField(fieldName, ReadLong);
        }

        /** <inheritdoc /> */
        public long ReadLong()
        {
            return Stream.ReadLong();
        }

        /** <inheritdoc /> */
        public long[] ReadLongArray(string fieldName)
        {
            return ReadField(fieldName, PortableUtils.ReadLongArray);
        }

        /** <inheritdoc /> */
        public long[] ReadLongArray()
        {
            return Read(PortableUtils.ReadLongArray);
        }

        /** <inheritdoc /> */
        public float ReadFloat(string fieldName)
        {
            return ReadField(fieldName, ReadFloat);
        }

        /** <inheritdoc /> */
        public float ReadFloat()
        {
            return Stream.ReadFloat();
        }

        /** <inheritdoc /> */
        public float[] ReadFloatArray(string fieldName)
        {
            return ReadField(fieldName, PortableUtils.ReadFloatArray);
        }

        /** <inheritdoc /> */
        public float[] ReadFloatArray()
        {
            return Read(PortableUtils.ReadFloatArray);
        }

        /** <inheritdoc /> */
        public double ReadDouble(string fieldName)
        {
            return ReadField(fieldName, ReadDouble);
        }

        /** <inheritdoc /> */
        public double ReadDouble()
        {
            return Stream.ReadDouble();
        }

        /** <inheritdoc /> */
        public double[] ReadDoubleArray(string fieldName)
        {
            return ReadField(fieldName, PortableUtils.ReadDoubleArray);
        }

        /** <inheritdoc /> */
        public double[] ReadDoubleArray()
        {
            return Read(PortableUtils.ReadDoubleArray);
        }

        /** <inheritdoc /> */
        public decimal ReadDecimal(string fieldName)
        {
            return ReadField(fieldName, PortableUtils.ReadDecimal);
        }

        /** <inheritdoc /> */
        public decimal ReadDecimal()
        {
            return Read(PortableUtils.ReadDecimal);
        }

        /** <inheritdoc /> */
        public decimal[] ReadDecimalArray(string fieldName)
        {
            return ReadField(fieldName, PortableUtils.ReadDecimalArray);
        }

        /** <inheritdoc /> */
        public decimal[] ReadDecimalArray()
        {
            return Read(PortableUtils.ReadDecimalArray);
        }

        /** <inheritdoc /> */
        public DateTime? ReadDate(string fieldName)
        {
            return ReadDate(fieldName, false);
        }

        /** <inheritdoc /> */
        public DateTime? ReadDate(string fieldName, bool local)
        {
            return ReadField(fieldName, r => PortableUtils.ReadDate(r, local));
        }

        /** <inheritdoc /> */
        public DateTime? ReadDate()
        {
            return ReadDate(false);
        }

        /** <inheritdoc /> */
        public DateTime? ReadDate(bool local)
        {
            return Read(r => PortableUtils.ReadDate(r, local));
        }

        /** <inheritdoc /> */
        public DateTime?[] ReadDateArray(string fieldName)
        {
            return ReadDateArray(fieldName, false);
        }

        /** <inheritdoc /> */
        public DateTime?[] ReadDateArray(string fieldName, bool local)
        {
            return ReadField(fieldName, r => PortableUtils.ReadDateArray(r, local));
        }

        /** <inheritdoc /> */
        public DateTime?[] ReadDateArray()
        {
            return ReadDateArray(false);
        }

        /** <inheritdoc /> */
        public DateTime?[] ReadDateArray(bool local)
        {
            return Read(r => PortableUtils.ReadDateArray(r, local));
        }

        /** <inheritdoc /> */
        public string ReadString(string fieldName)
        {
            return ReadField(fieldName, PortableUtils.ReadString);
        }

        /** <inheritdoc /> */
        public string ReadString()
        {
            return Read(PortableUtils.ReadString);
        }

        /** <inheritdoc /> */
        public string[] ReadStringArray(string fieldName)
        {
            return ReadField(fieldName, r => PortableUtils.ReadGenericArray<string>(r, false));
        }

        /** <inheritdoc /> */
        public string[] ReadStringArray()
        {
            return Read(r => PortableUtils.ReadGenericArray<string>(r, false));
        }

        /** <inheritdoc /> */
        public Guid? ReadGuid(string fieldName)
        {
            return ReadField(fieldName, PortableUtils.ReadGuid);
        }

        /** <inheritdoc /> */
        public Guid? ReadGuid()
        {
            return Read(PortableUtils.ReadGuid);
        }

        /** <inheritdoc /> */
        public Guid?[] ReadGuidArray(string fieldName)
        {
            return ReadField(fieldName, r => PortableUtils.ReadGenericArray<Guid?>(r, false));
        }

        /** <inheritdoc /> */
        public Guid?[] ReadGuidArray()
        {
            return Read(r => PortableUtils.ReadGenericArray<Guid?>(r, false));
        }

        /** <inheritdoc /> */
        public T ReadEnum<T>(string fieldName)
        {
            return ReadField(fieldName, PortableUtils.ReadEnum<T>);
        }

        /** <inheritdoc /> */
        public T ReadEnum<T>()
        {
            return Read(PortableUtils.ReadEnum<T>);
        }

        /** <inheritdoc /> */
        public T[] ReadEnumArray<T>(string fieldName)
        {
            return ReadField(fieldName, r => PortableUtils.ReadGenericArray<T>(r, true));
        }

        /** <inheritdoc /> */
        public T[] ReadEnumArray<T>()
        {
            return Read(r => PortableUtils.ReadGenericArray<T>(r, true));
        }

        /** <inheritdoc /> */
        public T ReadObject<T>(string fieldName)
        {
            if (_curRaw)
                throw new PortableException("Cannot read named fields after raw data is read.");

            int fieldId = PortableUtils.FieldId(_curTypeId, fieldName, _curConverter, _curMapper);

            if (SeekField(fieldId))
                return Deserialize<T>();

            return default(T);
        }

        /** <inheritdoc /> */
        public T ReadObject<T>()
        {
            return Deserialize<T>();
        }

        /** <inheritdoc /> */
        public T[] ReadObjectArray<T>(string fieldName)
        {
            return ReadField(fieldName, r => PortableUtils.ReadGenericArray<T>(r, true));
        }

        /** <inheritdoc /> */
        public T[] ReadObjectArray<T>()
        {
            return Read(r => PortableUtils.ReadGenericArray<T>(r, true));
        }

        /** <inheritdoc /> */
        public ICollection ReadCollection(string fieldName)
        {
            return ReadCollection(fieldName, null, null);
        }

        /** <inheritdoc /> */
        public ICollection ReadCollection()
        {
            return ReadCollection(null, null);
        }

        /** <inheritdoc /> */
        public ICollection ReadCollection(string fieldName, PortableCollectionFactory factory,
            PortableCollectionAdder adder)
        {
            return ReadField(fieldName, r => PortableUtils.ReadCollection(r, factory, adder));
        }

        /** <inheritdoc /> */
        public ICollection ReadCollection(PortableCollectionFactory factory,
            PortableCollectionAdder adder)
        {
            return Read(r => PortableUtils.ReadCollection(r, factory, adder));
        }

        /** <inheritdoc /> */
        public ICollection<T> ReadGenericCollection<T>(string fieldName)
        {
            return ReadGenericCollection<T>(fieldName, null);
        }

        /** <inheritdoc /> */
        public ICollection<T> ReadGenericCollection<T>()
        {
            return ReadGenericCollection((PortableGenericCollectionFactory<T>) null);
        }

        /** <inheritdoc /> */
        public ICollection<T> ReadGenericCollection<T>(string fieldName,
            PortableGenericCollectionFactory<T> factory)
        {
            return ReadField(fieldName, r => PortableUtils.ReadGenericCollection(r, factory));
        }

        /** <inheritdoc /> */
        public ICollection<T> ReadGenericCollection<T>(PortableGenericCollectionFactory<T> factory)
        {
            return Read(r => PortableUtils.ReadGenericCollection(r, factory));
        }

        /** <inheritdoc /> */
        public IDictionary ReadDictionary(string fieldName)
        {
            return ReadDictionary(fieldName, null);
        }

        /** <inheritdoc /> */
        public IDictionary ReadDictionary()
        {
            return ReadDictionary((PortableDictionaryFactory)null);
        }

        /** <inheritdoc /> */
        public IDictionary ReadDictionary(string fieldName, PortableDictionaryFactory factory)
        {
            return ReadField(fieldName, r => PortableUtils.ReadDictionary(r, factory));
        }

        /** <inheritdoc /> */
        public IDictionary ReadDictionary(PortableDictionaryFactory factory)
        {
            return Read(r => PortableUtils.ReadDictionary(r, factory));
        }

        /** <inheritdoc /> */
        public IDictionary<TK, TV> ReadGenericDictionary<TK, TV>(string fieldName)
        {
            return ReadGenericDictionary<TK, TV>(fieldName, null);
        }

        /** <inheritdoc /> */
        public IDictionary<TK, TV> ReadGenericDictionary<TK, TV>()
        {
            return ReadGenericDictionary((PortableGenericDictionaryFactory<TK, TV>) null);
        }

        /** <inheritdoc /> */
        public IDictionary<TK, TV> ReadGenericDictionary<TK, TV>(string fieldName,
            PortableGenericDictionaryFactory<TK, TV> factory)
        {
            return ReadField(fieldName, r => PortableUtils.ReadGenericDictionary(r, factory));
        }

        /** <inheritdoc /> */
        public IDictionary<TK, TV> ReadGenericDictionary<TK, TV>(PortableGenericDictionaryFactory<TK, TV> factory)
        {
            return Read(r => PortableUtils.ReadGenericDictionary(r, factory));
        }

        /// <summary>
        /// Enable detach mode for the next object read. 
        /// </summary>
        public void DetachNext()
        {
            _detach = true;
        }

        /// <summary>
        /// Deserialize object.
        /// </summary>
        /// <returns>Deserialized object.</returns>
        public T Deserialize<T>()
        {
            int pos = Stream.Position;

            byte hdr = Stream.ReadByte();

            var doDetach = _detach;  // save detach flag into a var and reset so it does not go deeper

            _detach = false;

            switch (hdr)
            {
                case PortableUtils.HdrNull:
                    return default(T);

                case PortableUtils.HdrHnd:
                    return ReadHandleObject<T>(pos);

                case PortableUtils.HdrFull:
                    return ReadFullObject<T>(pos);

                case PortableUtils.TypePortable:
                    return ReadPortableObject<T>(doDetach);
            }

            if (PortableUtils.IsPredefinedType(hdr))
                return PortableSystemHandlers.ReadSystemType<T>(hdr, this);

            throw new PortableException("Invalid header on deserialization [pos=" + pos + ", hdr=" + hdr + ']');
        }

        /// <summary>
        /// Reads the portable object.
        /// </summary>
        private T ReadPortableObject<T>(bool doDetach)
        {
            var len = Stream.ReadInt();

            var portablePos = Stream.Position;

            if (_mode != PortableMode.Deserialize)
                return TypeCaster<T>.Cast(ReadAsPortable(portablePos, len, doDetach));

            Stream.Seek(len, SeekOrigin.Current);

            var offset = Stream.ReadInt();

            var retPos = Stream.Position;

            Stream.Seek(portablePos + offset, SeekOrigin.Begin);

            _mode = PortableMode.KeepPortable;

            try
            {
                return Deserialize<T>();
            }
            finally
            {
                _mode = PortableMode.Deserialize;

                Stream.Seek(retPos, SeekOrigin.Begin);
            }
        }

        /// <summary>
        /// Reads the portable object in portable form.
        /// </summary>
        private PortableUserObject ReadAsPortable(int dataPos, int dataLen, bool doDetach)
        {
            try
            {
                Stream.Seek(dataLen + dataPos, SeekOrigin.Begin);

                var offs = Stream.ReadInt(); // offset inside data

                var pos = dataPos + offs;

                if (!doDetach)
                    return GetPortableUserObject(pos, pos, Stream.Array());
                
                Stream.Seek(pos + 10, SeekOrigin.Begin);

                var len = Stream.ReadInt();

                Stream.Seek(pos, SeekOrigin.Begin);

                return GetPortableUserObject(pos, 0, Stream.ReadByteArray(len));
            }
            finally
            {
                Stream.Seek(dataPos + dataLen + 4, SeekOrigin.Begin);
            }
        }

        /// <summary>
        /// Reads the full object.
        /// </summary>
        [SuppressMessage("Microsoft.Performance", "CA1804:RemoveUnusedLocals", MessageId = "hashCode")]
        private T ReadFullObject<T>(int pos)
        {
            // Read header.
            bool userType = Stream.ReadBool();
            int typeId = Stream.ReadInt();
            // ReSharper disable once UnusedVariable
            int hashCode = Stream.ReadInt();
            int len = Stream.ReadInt();
            int rawOffset = Stream.ReadInt();

            try
            {
                // Already read this object?
                object hndObj;

                if (_hnds != null && _hnds.TryGetValue(pos, out hndObj))
                    return (T) hndObj;

                if (userType && _mode == PortableMode.ForcePortable)
                {
                    PortableUserObject portObj;

                    if (_detach)
                    {
                        Stream.Seek(pos, SeekOrigin.Begin);

                        portObj = GetPortableUserObject(pos, 0, Stream.ReadByteArray(len));
                    }
                    else
                        portObj = GetPortableUserObject(pos, pos, Stream.Array());

                    T obj = _builder == null ? TypeCaster<T>.Cast(portObj) : TypeCaster<T>.Cast(_builder.Child(portObj));

                    AddHandle(pos, obj);

                    return obj;
                }
                else
                {
                    // Find descriptor.
                    IPortableTypeDescriptor desc;

                    if (!_descs.TryGetValue(PortableUtils.TypeKey(userType, typeId), out desc))
                        throw new PortableException("Unknown type ID: " + typeId);

                    // Instantiate object. 
                    if (desc.Type == null)
                        throw new PortableException("No matching type found for object [typeId=" +
                                                    desc.TypeId + ", typeName=" + desc.TypeName + ']');

                    // Preserve old frame.
                    int oldTypeId = _curTypeId;
                    int oldPos = _curPos;
                    int oldRawOffset = _curRawOffset;
                    IPortableNameMapper oldConverter = _curConverter;
                    IPortableIdMapper oldMapper = _curMapper;
                    bool oldRaw = _curRaw;

                    // Set new frame.
                    _curTypeId = typeId;
                    _curPos = pos;
                    _curRawOffset = rawOffset;
                    _curConverter = desc.NameConverter;
                    _curMapper = desc.Mapper;
                    _curRaw = false;

                    // Read object.
                    object obj;

                    var sysSerializer = desc.Serializer as IPortableSystemTypeSerializer;

                    if (sysSerializer != null)
                        obj = sysSerializer.ReadInstance(this);
                    else
                    {
                        try
                        {
                            obj = FormatterServices.GetUninitializedObject(desc.Type);

                            // Save handle.
                            AddHandle(pos, obj);
                        }
                        catch (Exception e)
                        {
                            throw new PortableException("Failed to create type instance: " +
                                                        desc.Type.AssemblyQualifiedName, e);
                        }

                        desc.Serializer.ReadPortable(obj, this);
                    }

                    // Restore old frame.
                    _curTypeId = oldTypeId;
                    _curPos = oldPos;
                    _curRawOffset = oldRawOffset;
                    _curConverter = oldConverter;
                    _curMapper = oldMapper;
                    _curRaw = oldRaw;

                    var wrappedSerializable = obj as SerializableObjectHolder;

                    return wrappedSerializable != null ? (T) wrappedSerializable.Item : (T) obj;
                }
            }
            finally
            {
                // Advance stream pointer.
                Stream.Seek(pos + len, SeekOrigin.Begin);
            }
        }

        /// <summary>
        /// Reads the handle object.
        /// </summary>
        private T ReadHandleObject<T>(int pos)
        {
            // Get handle position.
            int hndPos = pos - Stream.ReadInt();

            int retPos = Stream.Position;

            try
            {
                object hndObj;

                if (_builder == null || !_builder.CachedField(hndPos, out hndObj))
                {
                    if (_hnds == null || !_hnds.TryGetValue(hndPos, out hndObj))
                    {
                        // No such handler, i.e. we trying to deserialize inner object before deserializing outer.
                        Stream.Seek(hndPos, SeekOrigin.Begin);

                        hndObj = Deserialize<T>();
                    }

                    // Notify builder that we deserialized object on other location.
                    if (_builder != null)
                        _builder.CacheField(hndPos, hndObj);
                }

                return (T) hndObj;
            }
            finally
            {
                // Position stream to correct place.
                Stream.Seek(retPos, SeekOrigin.Begin);
            }
        }

        /// <summary>
        /// Adds a handle to the dictionary.
        /// </summary>
        /// <param name="pos">Position.</param>
        /// <param name="obj">Object.</param>
        private void AddHandle(int pos, object obj)
        {
            if (_hnds == null)
                _hnds = new PortableReaderHandleDictionary(pos, obj);
            else
                _hnds.Add(pos, obj);
        }

        /// <summary>
        /// Underlying stream.
        /// </summary>
        public IPortableStream Stream
        {
            get;
            private set;
        }

        /// <summary>
        /// Mark current output as raw. 
        /// </summary>
        private void MarkRaw()
        {
            if (!_curRaw)
            {
                _curRaw = true;

                Stream.Seek(_curPos + _curRawOffset, SeekOrigin.Begin);
            }
        }

        /// <summary>
        /// Seek field with the given ID in the current object.
        /// </summary>
        /// <param name="fieldId">Field ID.</param>
        /// <returns>True in case the field was found and position adjusted, false otherwise.</returns>
        private bool SeekField(int fieldId)
        {
            // This method is expected to be called when stream pointer is set either before
            // the field or on raw data offset.
            int start = _curPos + 18;
            int end = _curPos + _curRawOffset;

            int initial = Stream.Position;

            int cur = initial;

            while (cur < end)
            {
                int id = Stream.ReadInt();

                if (fieldId == id)
                {
                    // Field is found, return.
                    Stream.Seek(4, SeekOrigin.Current);

                    return true;
                }
                
                Stream.Seek(Stream.ReadInt(), SeekOrigin.Current);

                cur = Stream.Position;
            }

            Stream.Seek(start, SeekOrigin.Begin);

            cur = start;

            while (cur < initial)
            {
                int id = Stream.ReadInt();

                if (fieldId == id)
                {
                    // Field is found, return.
                    Stream.Seek(4, SeekOrigin.Current);

                    return true;
                }
                
                Stream.Seek(Stream.ReadInt(), SeekOrigin.Current);

                cur = Stream.Position;
            }

            return false;
        }

        /// <summary>
        /// Determines whether header at current position is HDR_NULL.
        /// </summary>
        private bool IsNullHeader()
        {
            var hdr = ReadByte();

            return hdr != PortableUtils.HdrNull;
        }

        /// <summary>
        /// Seeks the field by name, reads header and returns true if field is present and header is not null.
        /// </summary>
        private bool SeekField(string fieldName)
        {
            if (_curRaw)
                throw new PortableException("Cannot read named fields after raw data is read.");

            var fieldId = PortableUtils.FieldId(_curTypeId, fieldName, _curConverter, _curMapper);

            if (!SeekField(fieldId))
                return false;

            return IsNullHeader();
        }

        /// <summary>
        /// Seeks specified field and invokes provided func.
        /// </summary>
        private T ReadField<T>(string fieldName, Func<IPortableStream, T> readFunc)
        {
            return SeekField(fieldName) ? readFunc(Stream) : default(T);
        }

        /// <summary>
        /// Seeks specified field and invokes provided func.
        /// </summary>
        private T ReadField<T>(string fieldName, Func<PortableReaderImpl, T> readFunc)
        {
            return SeekField(fieldName) ? readFunc(this) : default(T);
        }

        /// <summary>
        /// Seeks specified field and invokes provided func.
        /// </summary>
        private T ReadField<T>(string fieldName, Func<T> readFunc)
        {
            return SeekField(fieldName) ? readFunc() : default(T);
        }

        /// <summary>
        /// Reads header and invokes specified func if the header is not null.
        /// </summary>
        private T Read<T>(Func<PortableReaderImpl, T> readFunc)
        {
            return IsNullHeader() ? readFunc(this) : default(T);
        }

        /// <summary>
        /// Reads header and invokes specified func if the header is not null.
        /// </summary>
        private T Read<T>(Func<IPortableStream, T> readFunc)
        {
            return IsNullHeader() ? readFunc(Stream) : default(T);
        }

        /// <summary>
        /// Gets the portable user object from a byte array.
        /// </summary>
        /// <param name="pos">Position in the current stream.</param>
        /// <param name="offs">Offset in the byte array.</param>
        /// <param name="bytes">Bytes.</param>
        private PortableUserObject GetPortableUserObject(int pos, int offs, byte[] bytes)
        {
            Stream.Seek(pos + 2, SeekOrigin.Begin);

            var id = Stream.ReadInt();

            var hash = Stream.ReadInt();

            return new PortableUserObject(_marsh, bytes, offs, id, hash);
        }
    }
}
