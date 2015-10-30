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
    using Apache.Ignite.Core.Impl.Portable.Structure;
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

        /** Current raw flag. */
        private bool _curRaw;

        /** Detach flag. */
        private bool _detach;

        /** Portable read mode. */
        private PortableMode _mode;

        /** Current type structure tracker. */
        private PortableStructureTracker _curStruct;

        /** */
        private int _curFooterStart;

        /** */
        private int _curFooterEnd;

        /** */
        private int[] _curSchema;

        /** */
        private static CopyOnWriteConcurrentDictionary<int, int[]> _schemas =
            new CopyOnWriteConcurrentDictionary<int, int[]>();

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
        public IPortableRawReader GetRawReader()
        {
            MarkRaw();

            return this;
        }

        /** <inheritdoc /> */
        public bool ReadBoolean(string fieldName)
        {
            return ReadField(fieldName, r => r.ReadBoolean(), PortableUtils.TypeBool);
        }

        /** <inheritdoc /> */
        public bool ReadBoolean()
        {
            return Stream.ReadBool();
        }

        /** <inheritdoc /> */
        public bool[] ReadBooleanArray(string fieldName)
        {
            return ReadField(fieldName, PortableUtils.ReadBooleanArray, PortableUtils.TypeArrayBool);
        }

        /** <inheritdoc /> */
        public bool[] ReadBooleanArray()
        {
            return Read(PortableUtils.ReadBooleanArray, PortableUtils.TypeArrayBool);
        }

        /** <inheritdoc /> */
        public byte ReadByte(string fieldName)
        {
            return ReadField(fieldName, ReadByte, PortableUtils.TypeByte);
        }

        /** <inheritdoc /> */
        public byte ReadByte()
        {
            return Stream.ReadByte();
        }

        /** <inheritdoc /> */
        public byte[] ReadByteArray(string fieldName)
        {
            return ReadField(fieldName, PortableUtils.ReadByteArray, PortableUtils.TypeArrayByte);
        }

        /** <inheritdoc /> */
        public byte[] ReadByteArray()
        {
            return Read(PortableUtils.ReadByteArray, PortableUtils.TypeArrayByte);
        }

        /** <inheritdoc /> */
        public short ReadShort(string fieldName)
        {
            return ReadField(fieldName, ReadShort, PortableUtils.TypeShort);
        }

        /** <inheritdoc /> */
        public short ReadShort()
        {
            return Stream.ReadShort();
        }

        /** <inheritdoc /> */
        public short[] ReadShortArray(string fieldName)
        {
            return ReadField(fieldName, PortableUtils.ReadShortArray, PortableUtils.TypeArrayShort);
        }

        /** <inheritdoc /> */
        public short[] ReadShortArray()
        {
            return Read(PortableUtils.ReadShortArray, PortableUtils.TypeArrayShort);
        }

        /** <inheritdoc /> */
        public char ReadChar(string fieldName)
        {
            return ReadField(fieldName, ReadChar, PortableUtils.TypeChar);
        }

        /** <inheritdoc /> */
        public char ReadChar()
        {
            return Stream.ReadChar();
        }

        /** <inheritdoc /> */
        public char[] ReadCharArray(string fieldName)
        {
            return ReadField(fieldName, PortableUtils.ReadCharArray, PortableUtils.TypeArrayChar);
        }

        /** <inheritdoc /> */
        public char[] ReadCharArray()
        {
            return Read(PortableUtils.ReadCharArray, PortableUtils.TypeArrayChar);
        }

        /** <inheritdoc /> */
        public int ReadInt(string fieldName)
        {
            return ReadField(fieldName, ReadInt, PortableUtils.TypeInt);
        }

        /** <inheritdoc /> */
        public int ReadInt()
        {
            return Stream.ReadInt();
        }

        /** <inheritdoc /> */
        public int[] ReadIntArray(string fieldName)
        {
            return ReadField(fieldName, PortableUtils.ReadIntArray, PortableUtils.TypeArrayInt);
        }

        /** <inheritdoc /> */
        public int[] ReadIntArray()
        {
            return Read(PortableUtils.ReadIntArray, PortableUtils.TypeArrayInt);
        }

        /** <inheritdoc /> */
        public long ReadLong(string fieldName)
        {
            return ReadField(fieldName, ReadLong, PortableUtils.TypeLong);
        }

        /** <inheritdoc /> */
        public long ReadLong()
        {
            return Stream.ReadLong();
        }

        /** <inheritdoc /> */
        public long[] ReadLongArray(string fieldName)
        {
            return ReadField(fieldName, PortableUtils.ReadLongArray, PortableUtils.TypeArrayLong);
        }

        /** <inheritdoc /> */
        public long[] ReadLongArray()
        {
            return Read(PortableUtils.ReadLongArray, PortableUtils.TypeArrayLong);
        }

        /** <inheritdoc /> */
        public float ReadFloat(string fieldName)
        {
            return ReadField(fieldName, ReadFloat, PortableUtils.TypeFloat);
        }

        /** <inheritdoc /> */
        public float ReadFloat()
        {
            return Stream.ReadFloat();
        }

        /** <inheritdoc /> */
        public float[] ReadFloatArray(string fieldName)
        {
            return ReadField(fieldName, PortableUtils.ReadFloatArray, PortableUtils.TypeArrayFloat);
        }

        /** <inheritdoc /> */
        public float[] ReadFloatArray()
        {
            return Read(PortableUtils.ReadFloatArray, PortableUtils.TypeArrayFloat);
        }

        /** <inheritdoc /> */
        public double ReadDouble(string fieldName)
        {
            return ReadField(fieldName, ReadDouble, PortableUtils.TypeDouble);
        }

        /** <inheritdoc /> */
        public double ReadDouble()
        {
            return Stream.ReadDouble();
        }

        /** <inheritdoc /> */
        public double[] ReadDoubleArray(string fieldName)
        {
            return ReadField(fieldName, PortableUtils.ReadDoubleArray, PortableUtils.TypeArrayDouble);
        }

        /** <inheritdoc /> */
        public double[] ReadDoubleArray()
        {
            return Read(PortableUtils.ReadDoubleArray, PortableUtils.TypeArrayDouble);
        }

        /** <inheritdoc /> */
        public decimal? ReadDecimal(string fieldName)
        {
            return ReadField(fieldName, PortableUtils.ReadDecimal, PortableUtils.TypeDecimal);
        }

        /** <inheritdoc /> */
        public decimal? ReadDecimal()
        {
            return Read(PortableUtils.ReadDecimal, PortableUtils.TypeDecimal);
        }

        /** <inheritdoc /> */
        public decimal?[] ReadDecimalArray(string fieldName)
        {
            return ReadField(fieldName, PortableUtils.ReadDecimalArray, PortableUtils.TypeArrayDecimal);
        }

        /** <inheritdoc /> */
        public decimal?[] ReadDecimalArray()
        {
            return Read(PortableUtils.ReadDecimalArray, PortableUtils.TypeArrayDecimal);
        }

        /** <inheritdoc /> */
        public DateTime? ReadTimestamp(string fieldName)
        {
            return ReadField(fieldName, PortableUtils.ReadTimestamp, PortableUtils.TypeTimestamp);
        }

        /** <inheritdoc /> */
        public DateTime? ReadTimestamp()
        {
            return Read(PortableUtils.ReadTimestamp, PortableUtils.TypeTimestamp);
        }
        
        /** <inheritdoc /> */
        public DateTime?[] ReadTimestampArray(string fieldName)
        {
            return ReadField(fieldName, PortableUtils.ReadTimestampArray, PortableUtils.TypeArrayTimestamp);
        }
        
        /** <inheritdoc /> */
        public DateTime?[] ReadTimestampArray()
        {
            return Read(PortableUtils.ReadTimestampArray, PortableUtils.TypeArrayTimestamp);
        }
        
        /** <inheritdoc /> */
        public string ReadString(string fieldName)
        {
            return ReadField(fieldName, PortableUtils.ReadString, PortableUtils.TypeString);
        }

        /** <inheritdoc /> */
        public string ReadString()
        {
            return Read(PortableUtils.ReadString, PortableUtils.TypeString);
        }

        /** <inheritdoc /> */
        public string[] ReadStringArray(string fieldName)
        {
            return ReadField(fieldName, r => PortableUtils.ReadArray<string>(r, false), PortableUtils.TypeArrayString);
        }

        /** <inheritdoc /> */
        public string[] ReadStringArray()
        {
            return Read(r => PortableUtils.ReadArray<string>(r, false), PortableUtils.TypeArrayString);
        }

        /** <inheritdoc /> */
        public Guid? ReadGuid(string fieldName)
        {
            return ReadField(fieldName, PortableUtils.ReadGuid, PortableUtils.TypeGuid);
        }

        /** <inheritdoc /> */
        public Guid? ReadGuid()
        {
            return Read(PortableUtils.ReadGuid, PortableUtils.TypeGuid);
        }

        /** <inheritdoc /> */
        public Guid?[] ReadGuidArray(string fieldName)
        {
            return ReadField(fieldName, r => PortableUtils.ReadArray<Guid?>(r, false), PortableUtils.TypeArrayGuid);
        }

        /** <inheritdoc /> */
        public Guid?[] ReadGuidArray()
        {
            return Read(r => PortableUtils.ReadArray<Guid?>(r, false), PortableUtils.TypeArrayGuid);
        }

        /** <inheritdoc /> */
        public T ReadEnum<T>(string fieldName)
        {
            return ReadField(fieldName, PortableUtils.ReadEnum<T>, PortableUtils.TypeEnum);
        }

        /** <inheritdoc /> */
        public T ReadEnum<T>()
        {
            return Read(PortableUtils.ReadEnum<T>, PortableUtils.TypeEnum);
        }

        /** <inheritdoc /> */
        public T[] ReadEnumArray<T>(string fieldName)
        {
            return ReadField(fieldName, r => PortableUtils.ReadArray<T>(r, true), PortableUtils.TypeArrayEnum);
        }

        /** <inheritdoc /> */
        public T[] ReadEnumArray<T>()
        {
            return Read(r => PortableUtils.ReadArray<T>(r, true), PortableUtils.TypeArrayEnum);
        }

        /** <inheritdoc /> */
        public T ReadObject<T>(string fieldName)
        {
            if (_curRaw)
                throw new PortableException("Cannot read named fields after raw data is read.");

            int fieldId = _curStruct.GetFieldId(fieldName);

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
        public T[] ReadArray<T>(string fieldName)
        {
            return ReadField(fieldName, r => PortableUtils.ReadArray<T>(r, true), PortableUtils.TypeArray);
        }

        /** <inheritdoc /> */
        public T[] ReadArray<T>()
        {
            return Read(r => PortableUtils.ReadArray<T>(r, true), PortableUtils.TypeArray);
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
            return ReadField(fieldName, r => PortableUtils.ReadCollection(r, factory, adder), PortableUtils.TypeCollection);
        }

        /** <inheritdoc /> */
        public ICollection ReadCollection(PortableCollectionFactory factory,
            PortableCollectionAdder adder)
        {
            return Read(r => PortableUtils.ReadCollection(r, factory, adder), PortableUtils.TypeCollection);
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
            return ReadField(fieldName, r => PortableUtils.ReadDictionary(r, factory), PortableUtils.TypeDictionary);
        }

        /** <inheritdoc /> */
        public IDictionary ReadDictionary(PortableDictionaryFactory factory)
        {
            return Read(r => PortableUtils.ReadDictionary(r, factory), PortableUtils.TypeDictionary);
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
                    if (default(T) != null)
                        throw new PortableException(string.Format("Invalid data on deserialization. " +
                            "Expected: '{0}' But was: null", typeof (T)));

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

            var portableBytesPos = Stream.Position;

            if (_mode != PortableMode.Deserialize)
                return TypeCaster<T>.Cast(ReadAsPortable(portableBytesPos, len, doDetach));

            Stream.Seek(len, SeekOrigin.Current);

            var offset = Stream.ReadInt();

            var retPos = Stream.Position;

            Stream.Seek(portableBytesPos + offset, SeekOrigin.Begin);

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
        private PortableUserObject ReadAsPortable(int portableBytesPos, int dataLen, bool doDetach)
        {
            try
            {
                Stream.Seek(dataLen + portableBytesPos, SeekOrigin.Begin);

                var offs = Stream.ReadInt(); // offset inside data

                var pos = portableBytesPos + offs;

                var hdr = PortableObjectHeader.Read(Stream, pos);

                if (!doDetach)
                    return new PortableUserObject(_marsh, Stream.GetArray(), pos, hdr);

                Stream.Seek(pos, SeekOrigin.Begin);

                return new PortableUserObject(_marsh, Stream.ReadByteArray(hdr.Length), 0, hdr);
            }
            finally
            {
                Stream.Seek(portableBytesPos + dataLen + 4, SeekOrigin.Begin);
            }
        }

        /// <summary>
        /// Reads the full object.
        /// </summary>
        [SuppressMessage("Microsoft.Performance", "CA1804:RemoveUnusedLocals", MessageId = "hashCode")]
        private T ReadFullObject<T>(int pos)
        {
            var hdr = PortableObjectHeader.Read(Stream, pos);

            // Validate protocol version.
            PortableUtils.ValidateProtocolVersion(hdr.Version);

            try
            {
                // Already read this object?
                object hndObj;

                if (_hnds != null && _hnds.TryGetValue(pos, out hndObj))
                    return (T) hndObj;

                if (hdr.IsUserType && _mode == PortableMode.ForcePortable)
                {
                    PortableUserObject portObj;

                    if (_detach)
                    {
                        Stream.Seek(pos, SeekOrigin.Begin);

                        portObj = new PortableUserObject(_marsh, Stream.ReadByteArray(hdr.Length), 0, hdr);
                    }
                    else
                        portObj = new PortableUserObject(_marsh, Stream.GetArray(), pos, hdr);

                    T obj = _builder == null ? TypeCaster<T>.Cast(portObj) : TypeCaster<T>.Cast(_builder.Child(portObj));

                    AddHandle(pos, obj);

                    return obj;
                }
                else
                {
                    // Find descriptor.
                    IPortableTypeDescriptor desc;

                    if (!_descs.TryGetValue(PortableUtils.TypeKey(hdr.IsUserType, hdr.TypeId), out desc))
                        throw new PortableException("Unknown type ID: " + hdr.TypeId);

                    // Instantiate object. 
                    if (desc.Type == null)
                        throw new PortableException("No matching type found for object [typeId=" +
                                                    desc.TypeId + ", typeName=" + desc.TypeName + ']');

                    // Preserve old frame.
                    int oldTypeId = _curTypeId;
                    int oldPos = _curPos;
                    int oldRawOffset = _curRawOffset;
                    var oldStruct = _curStruct;
                    bool oldRaw = _curRaw;
                    var oldFooterStart = _curFooterStart;
                    var oldFooterEnd = _curFooterEnd;
                    var oldSchema = _curSchema;

                    // Set new frame.
                    _curTypeId = hdr.TypeId;
                    _curPos = pos;
                    _curFooterEnd = hdr.GetSchemaEnd(pos);
                    _curFooterStart = hdr.GetSchemaStart(pos);

                    if (!_schemas.TryGetValue(hdr.SchemaId, out _curSchema))
                    {
                        _curSchema = _schemas.GetOrAdd(hdr.SchemaId, ReadSchema);
                    }

                    _curRawOffset = hdr.GetRawOffset(Stream, pos);
                    _curStruct = new PortableStructureTracker(desc, desc.ReaderTypeStructure);
                    _curRaw = false;

                    // Read object.
                    Stream.Seek(pos + PortableObjectHeader.Size, SeekOrigin.Begin);

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

                    _curStruct.UpdateReaderStructure();

                    // Restore old frame.
                    _curTypeId = oldTypeId;
                    _curPos = oldPos;
                    _curRawOffset = oldRawOffset;
                    _curStruct = oldStruct;
                    _curRaw = oldRaw;
                    _curFooterStart = oldFooterStart;
                    _curFooterEnd = oldFooterEnd;
                    _curSchema = oldSchema;

                    // Process wrappers. We could introduce a common interface, but for only 2 if-else is faster.
                    var wrappedSerializable = obj as SerializableObjectHolder;

                    if (wrappedSerializable != null) 
                        return (T) wrappedSerializable.Item;

                    var wrappedDateTime = obj as DateTimeHolder;

                    if (wrappedDateTime != null)
                        return TypeCaster<T>.Cast(wrappedDateTime.Item);
                    
                    return (T) obj;
                }
            }
            finally
            {
                // Advance stream pointer.
                Stream.Seek(pos + hdr.Length, SeekOrigin.Begin);
            }
        }

        private int[] ReadSchema(int schemaId)
        {
            Stream.Seek(_curFooterStart, SeekOrigin.Begin);
            
            var count = (_curFooterEnd - _curFooterStart) >> 3;
            
            var res = new int[count];

            for (int i = 0; i < count; i++)
            {
                res[i] = Stream.ReadInt();
                Stream.Seek(4, SeekOrigin.Current);
            }

            return res;
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

                if (_builder == null || !_builder.TryGetCachedField(hndPos, out hndObj))
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
            Stream.Seek(_curFooterStart, SeekOrigin.Begin);

            while (Stream.Position < _curFooterEnd)
            {
                var id = Stream.ReadInt();

                if (id == fieldId)
                {
                    var fieldOffset = Stream.ReadInt();

                    Stream.Seek(_curPos + fieldOffset, SeekOrigin.Begin);
                    return true;
                }

                Stream.Seek(4, SeekOrigin.Current);
            }

            return false;
        }

        /// <summary>
        /// Determines whether header at current position is HDR_NULL.
        /// </summary>
        private bool IsNotNullHeader(byte expHdr)
        {
            var hdr = ReadByte();
            
            if (hdr == PortableUtils.HdrNull)
                return false;

            if (expHdr != hdr)
                throw new PortableException(string.Format("Invalid header on deserialization. " +
                                                          "Expected: {0} but was: {1}", expHdr, hdr));

            return true;
        }

        /// <summary>
        /// Seeks the field by name, reads header and returns true if field is present and header is not null.
        /// </summary>
        private bool SeekField(string fieldName, byte expHdr)
        {
            if (_curRaw)
                throw new PortableException("Cannot read named fields after raw data is read.");

            var actionId = _curStruct.CurStructAction;

            var fieldId = _curStruct.GetFieldId(fieldName);

            if (_curSchema == null || actionId >= _curSchema.Length || fieldId != _curSchema[actionId])
            {
                _curSchema = null;   // read order is different, discard schema

                if (!SeekField(fieldId))
                    return false;
            }

            return IsNotNullHeader(expHdr);
        }

        /// <summary>
        /// Seeks specified field and invokes provided func.
        /// </summary>
        private T ReadField<T>(string fieldName, Func<IPortableStream, T> readFunc, byte expHdr)
        {
            return SeekField(fieldName, expHdr) ? readFunc(Stream) : default(T);
        }

        /// <summary>
        /// Seeks specified field and invokes provided func.
        /// </summary>
        private T ReadField<T>(string fieldName, Func<PortableReaderImpl, T> readFunc, byte expHdr)
        {
            return SeekField(fieldName, expHdr) ? readFunc(this) : default(T);
        }

        /// <summary>
        /// Seeks specified field and invokes provided func.
        /// </summary>
        private T ReadField<T>(string fieldName, Func<T> readFunc, byte expHdr)
        {
            return SeekField(fieldName, expHdr) ? readFunc() : default(T);
        }

        /// <summary>
        /// Reads header and invokes specified func if the header is not null.
        /// </summary>
        private T Read<T>(Func<PortableReaderImpl, T> readFunc, byte expHdr)
        {
            return IsNotNullHeader(expHdr) ? readFunc(this) : default(T);
        }

        /// <summary>
        /// Reads header and invokes specified func if the header is not null.
        /// </summary>
        private T Read<T>(Func<IPortableStream, T> readFunc, byte expHdr)
        {
            return IsNotNullHeader(expHdr) ? readFunc(Stream) : default(T);
        }
    }
}
