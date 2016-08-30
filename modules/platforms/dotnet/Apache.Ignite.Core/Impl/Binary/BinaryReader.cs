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
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.Diagnostics.CodeAnalysis;
    using System.IO;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Impl.Binary.IO;
    using Apache.Ignite.Core.Impl.Binary.Structure;
    using Apache.Ignite.Core.Impl.Common;

    /// <summary>
    /// Binary reader implementation. 
    /// </summary>
    internal class BinaryReader : IBinaryReader, IBinaryRawReader
    {
        /** Marshaller. */
        private readonly Marshaller _marsh;

        /** Parent builder. */
        private readonly BinaryObjectBuilder _builder;

        /** Handles. */
        private BinaryReaderHandleDictionary _hnds;

        /** Current position. */
        private int _curPos;

        /** Current raw flag. */
        private bool _curRaw;

        /** Detach flag. */
        private bool _detach;

        /** Binary read mode. */
        private BinaryMode _mode;

        /** Current type structure tracker. */
        private BinaryStructureTracker _curStruct;

        /** Current schema. */
        private int[] _curSchema;

        /** Current schema with positions. */
        private Dictionary<int, int> _curSchemaMap;

        /** Current header. */
        private BinaryObjectHeader _curHdr;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="marsh">Marshaller.</param>
        /// <param name="stream">Input stream.</param>
        /// <param name="mode">The mode.</param>
        /// <param name="builder">Builder.</param>
        public BinaryReader
            (Marshaller marsh,
            IBinaryStream stream, 
            BinaryMode mode,
            BinaryObjectBuilder builder)
        {
            _marsh = marsh;
            _mode = mode;
            _builder = builder;

            Stream = stream;
        }

        /// <summary>
        /// Gets the marshaller.
        /// </summary>
        public Marshaller Marshaller
        {
            get { return _marsh; }
        }

        /** <inheritdoc /> */
        public IBinaryRawReader GetRawReader()
        {
            MarkRaw();

            return this;
        }

        /** <inheritdoc /> */
        public bool ReadBoolean(string fieldName)
        {
            return ReadField(fieldName, r => r.ReadBoolean(), BinaryUtils.TypeBool);
        }

        /** <inheritdoc /> */
        public bool ReadBoolean()
        {
            return Stream.ReadBool();
        }

        /** <inheritdoc /> */
        public bool[] ReadBooleanArray(string fieldName)
        {
            return ReadField(fieldName, BinaryUtils.ReadBooleanArray, BinaryUtils.TypeArrayBool);
        }

        /** <inheritdoc /> */
        public bool[] ReadBooleanArray()
        {
            return Read(BinaryUtils.ReadBooleanArray, BinaryUtils.TypeArrayBool);
        }

        /** <inheritdoc /> */
        public byte ReadByte(string fieldName)
        {
            return ReadField(fieldName, ReadByte, BinaryUtils.TypeByte);
        }

        /** <inheritdoc /> */
        public byte ReadByte()
        {
            return Stream.ReadByte();
        }

        /** <inheritdoc /> */
        public byte[] ReadByteArray(string fieldName)
        {
            return ReadField(fieldName, BinaryUtils.ReadByteArray, BinaryUtils.TypeArrayByte);
        }

        /** <inheritdoc /> */
        public byte[] ReadByteArray()
        {
            return Read(BinaryUtils.ReadByteArray, BinaryUtils.TypeArrayByte);
        }

        /** <inheritdoc /> */
        public short ReadShort(string fieldName)
        {
            return ReadField(fieldName, ReadShort, BinaryUtils.TypeShort);
        }

        /** <inheritdoc /> */
        public short ReadShort()
        {
            return Stream.ReadShort();
        }

        /** <inheritdoc /> */
        public short[] ReadShortArray(string fieldName)
        {
            return ReadField(fieldName, BinaryUtils.ReadShortArray, BinaryUtils.TypeArrayShort);
        }

        /** <inheritdoc /> */
        public short[] ReadShortArray()
        {
            return Read(BinaryUtils.ReadShortArray, BinaryUtils.TypeArrayShort);
        }

        /** <inheritdoc /> */
        public char ReadChar(string fieldName)
        {
            return ReadField(fieldName, ReadChar, BinaryUtils.TypeChar);
        }

        /** <inheritdoc /> */
        public char ReadChar()
        {
            return Stream.ReadChar();
        }

        /** <inheritdoc /> */
        public char[] ReadCharArray(string fieldName)
        {
            return ReadField(fieldName, BinaryUtils.ReadCharArray, BinaryUtils.TypeArrayChar);
        }

        /** <inheritdoc /> */
        public char[] ReadCharArray()
        {
            return Read(BinaryUtils.ReadCharArray, BinaryUtils.TypeArrayChar);
        }

        /** <inheritdoc /> */
        public int ReadInt(string fieldName)
        {
            return ReadField(fieldName, ReadInt, BinaryUtils.TypeInt);
        }

        /** <inheritdoc /> */
        public int ReadInt()
        {
            return Stream.ReadInt();
        }

        /** <inheritdoc /> */
        public int[] ReadIntArray(string fieldName)
        {
            return ReadField(fieldName, BinaryUtils.ReadIntArray, BinaryUtils.TypeArrayInt);
        }

        /** <inheritdoc /> */
        public int[] ReadIntArray()
        {
            return Read(BinaryUtils.ReadIntArray, BinaryUtils.TypeArrayInt);
        }

        /** <inheritdoc /> */
        public long ReadLong(string fieldName)
        {
            return ReadField(fieldName, ReadLong, BinaryUtils.TypeLong);
        }

        /** <inheritdoc /> */
        public long ReadLong()
        {
            return Stream.ReadLong();
        }

        /** <inheritdoc /> */
        public long[] ReadLongArray(string fieldName)
        {
            return ReadField(fieldName, BinaryUtils.ReadLongArray, BinaryUtils.TypeArrayLong);
        }

        /** <inheritdoc /> */
        public long[] ReadLongArray()
        {
            return Read(BinaryUtils.ReadLongArray, BinaryUtils.TypeArrayLong);
        }

        /** <inheritdoc /> */
        public float ReadFloat(string fieldName)
        {
            return ReadField(fieldName, ReadFloat, BinaryUtils.TypeFloat);
        }

        /** <inheritdoc /> */
        public float ReadFloat()
        {
            return Stream.ReadFloat();
        }

        /** <inheritdoc /> */
        public float[] ReadFloatArray(string fieldName)
        {
            return ReadField(fieldName, BinaryUtils.ReadFloatArray, BinaryUtils.TypeArrayFloat);
        }

        /** <inheritdoc /> */
        public float[] ReadFloatArray()
        {
            return Read(BinaryUtils.ReadFloatArray, BinaryUtils.TypeArrayFloat);
        }

        /** <inheritdoc /> */
        public double ReadDouble(string fieldName)
        {
            return ReadField(fieldName, ReadDouble, BinaryUtils.TypeDouble);
        }

        /** <inheritdoc /> */
        public double ReadDouble()
        {
            return Stream.ReadDouble();
        }

        /** <inheritdoc /> */
        public double[] ReadDoubleArray(string fieldName)
        {
            return ReadField(fieldName, BinaryUtils.ReadDoubleArray, BinaryUtils.TypeArrayDouble);
        }

        /** <inheritdoc /> */
        public double[] ReadDoubleArray()
        {
            return Read(BinaryUtils.ReadDoubleArray, BinaryUtils.TypeArrayDouble);
        }

        /** <inheritdoc /> */
        public decimal? ReadDecimal(string fieldName)
        {
            return ReadField(fieldName, BinaryUtils.ReadDecimal, BinaryUtils.TypeDecimal);
        }

        /** <inheritdoc /> */
        public decimal? ReadDecimal()
        {
            return Read(BinaryUtils.ReadDecimal, BinaryUtils.TypeDecimal);
        }

        /** <inheritdoc /> */
        public decimal?[] ReadDecimalArray(string fieldName)
        {
            return ReadField(fieldName, BinaryUtils.ReadDecimalArray, BinaryUtils.TypeArrayDecimal);
        }

        /** <inheritdoc /> */
        public decimal?[] ReadDecimalArray()
        {
            return Read(BinaryUtils.ReadDecimalArray, BinaryUtils.TypeArrayDecimal);
        }

        /** <inheritdoc /> */
        public DateTime? ReadTimestamp(string fieldName)
        {
            return ReadField(fieldName, BinaryUtils.ReadTimestamp, BinaryUtils.TypeTimestamp);
        }

        /** <inheritdoc /> */
        public DateTime? ReadTimestamp()
        {
            return Read(BinaryUtils.ReadTimestamp, BinaryUtils.TypeTimestamp);
        }
        
        /** <inheritdoc /> */
        public DateTime?[] ReadTimestampArray(string fieldName)
        {
            return ReadField(fieldName, BinaryUtils.ReadTimestampArray, BinaryUtils.TypeArrayTimestamp);
        }
        
        /** <inheritdoc /> */
        public DateTime?[] ReadTimestampArray()
        {
            return Read(BinaryUtils.ReadTimestampArray, BinaryUtils.TypeArrayTimestamp);
        }
        
        /** <inheritdoc /> */
        public string ReadString(string fieldName)
        {
            return ReadField(fieldName, BinaryUtils.ReadString, BinaryUtils.TypeString);
        }

        /** <inheritdoc /> */
        public string ReadString()
        {
            return Read(BinaryUtils.ReadString, BinaryUtils.TypeString);
        }

        /** <inheritdoc /> */
        public string[] ReadStringArray(string fieldName)
        {
            return ReadField(fieldName, r => BinaryUtils.ReadArray<string>(r, false), BinaryUtils.TypeArrayString);
        }

        /** <inheritdoc /> */
        public string[] ReadStringArray()
        {
            return Read(r => BinaryUtils.ReadArray<string>(r, false), BinaryUtils.TypeArrayString);
        }

        /** <inheritdoc /> */
        public Guid? ReadGuid(string fieldName)
        {
            return ReadField(fieldName, BinaryUtils.ReadGuid, BinaryUtils.TypeGuid);
        }

        /** <inheritdoc /> */
        public Guid? ReadGuid()
        {
            return Read(BinaryUtils.ReadGuid, BinaryUtils.TypeGuid);
        }

        /** <inheritdoc /> */
        public Guid?[] ReadGuidArray(string fieldName)
        {
            return ReadField(fieldName, r => BinaryUtils.ReadArray<Guid?>(r, false), BinaryUtils.TypeArrayGuid);
        }

        /** <inheritdoc /> */
        public Guid?[] ReadGuidArray()
        {
            return Read(r => BinaryUtils.ReadArray<Guid?>(r, false), BinaryUtils.TypeArrayGuid);
        }

        /** <inheritdoc /> */
        public T ReadEnum<T>(string fieldName)
        {
            return SeekField(fieldName) ? ReadEnum<T>() : default(T);
        }

        /** <inheritdoc /> */
        public T ReadEnum<T>()
        {
            var hdr = ReadByte();

            switch (hdr)
            {
                case BinaryUtils.HdrNull:
                    return default(T);

                case BinaryUtils.TypeEnum:
                    // Never read enums in binary mode when reading a field (we do not support half-binary objects)
                    return ReadEnum0<T>(this, false);  

                case BinaryUtils.HdrFull:
                    // Unregistered enum written as serializable
                    Stream.Seek(-1, SeekOrigin.Current);

                    return ReadObject<T>(); 

                default:
                    throw new BinaryObjectException(
                        string.Format("Invalid header on enum deserialization. Expected: {0} or {1} but was: {2}",
                            BinaryUtils.TypeEnum, BinaryUtils.HdrFull, hdr));
            }
        }

        /** <inheritdoc /> */
        public T[] ReadEnumArray<T>(string fieldName)
        {
            return ReadField(fieldName, r => BinaryUtils.ReadArray<T>(r, true), BinaryUtils.TypeArrayEnum);
        }

        /** <inheritdoc /> */
        public T[] ReadEnumArray<T>()
        {
            return Read(r => BinaryUtils.ReadArray<T>(r, true), BinaryUtils.TypeArrayEnum);
        }

        /** <inheritdoc /> */
        public T ReadObject<T>(string fieldName)
        {
            if (_curRaw)
                throw new BinaryObjectException("Cannot read named fields after raw data is read.");

            if (SeekField(fieldName))
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
            return ReadField(fieldName, r => BinaryUtils.ReadArray<T>(r, true), BinaryUtils.TypeArray);
        }

        /** <inheritdoc /> */
        public T[] ReadArray<T>()
        {
            return Read(r => BinaryUtils.ReadArray<T>(r, true), BinaryUtils.TypeArray);
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
        public ICollection ReadCollection(string fieldName, Func<int, ICollection> factory, 
            Action<ICollection, object> adder)
        {
            return ReadField(fieldName, r => BinaryUtils.ReadCollection(r, factory, adder), BinaryUtils.TypeCollection);
        }

        /** <inheritdoc /> */
        public ICollection ReadCollection(Func<int, ICollection> factory, Action<ICollection, object> adder)
        {
            return Read(r => BinaryUtils.ReadCollection(r, factory, adder), BinaryUtils.TypeCollection);
        }

        /** <inheritdoc /> */
        public IDictionary ReadDictionary(string fieldName)
        {
            return ReadDictionary(fieldName, null);
        }

        /** <inheritdoc /> */
        public IDictionary ReadDictionary()
        {
            return ReadDictionary((Func<int, IDictionary>) null);
        }

        /** <inheritdoc /> */
        public IDictionary ReadDictionary(string fieldName, Func<int, IDictionary> factory)
        {
            return ReadField(fieldName, r => BinaryUtils.ReadDictionary(r, factory), BinaryUtils.TypeDictionary);
        }

        /** <inheritdoc /> */
        public IDictionary ReadDictionary(Func<int, IDictionary> factory)
        {
            return Read(r => BinaryUtils.ReadDictionary(r, factory), BinaryUtils.TypeDictionary);
        }

        /// <summary>
        /// Enable detach mode for the next object read. 
        /// </summary>
        public BinaryReader DetachNext()
        {
            _detach = true;

            return this;
        }

        /// <summary>
        /// Deserialize object.
        /// </summary>
        /// <returns>Deserialized object.</returns>
        public T Deserialize<T>()
        {
            T res;

            // ReSharper disable once CompareNonConstrainedGenericWithNull
            if (!TryDeserialize(out res) && default(T) != null)
                throw new BinaryObjectException(string.Format("Invalid data on deserialization. " +
                    "Expected: '{0}' But was: null", typeof (T)));

            return res;
        }

        /// <summary>
        /// Deserialize object.
        /// </summary>
        /// <returns>Deserialized object.</returns>
        public bool TryDeserialize<T>(out T res)
        {
            int pos = Stream.Position;

            byte hdr = Stream.ReadByte();

            var doDetach = _detach;  // save detach flag into a var and reset so it does not go deeper

            _detach = false;

            switch (hdr)
            {
                case BinaryUtils.HdrNull:
                    res = default(T);

                    return false;

                case BinaryUtils.HdrHnd:
                    res = ReadHandleObject<T>(pos);

                    return true;

                case BinaryUtils.HdrFull:
                    res = ReadFullObject<T>(pos);

                    return true;

                case BinaryUtils.TypeBinary:
                    res = ReadBinaryObject<T>(doDetach);

                    return true;

                case BinaryUtils.TypeEnum:
                    res = ReadEnum0<T>(this, _mode != BinaryMode.Deserialize);

                    return true;
            }

            if (BinarySystemHandlers.TryReadSystemType(hdr, this, out res))
                return true;

            throw new BinaryObjectException("Invalid header on deserialization [pos=" + pos + ", hdr=" + hdr + ']');
        }

        /// <summary>
        /// Reads the binary object.
        /// </summary>
        private T ReadBinaryObject<T>(bool doDetach)
        {
            var len = Stream.ReadInt();

            var binaryBytesPos = Stream.Position;

            if (_mode != BinaryMode.Deserialize)
                return TypeCaster<T>.Cast(ReadAsBinary(binaryBytesPos, len, doDetach));

            Stream.Seek(len, SeekOrigin.Current);

            var offset = Stream.ReadInt();

            var retPos = Stream.Position;

            Stream.Seek(binaryBytesPos + offset, SeekOrigin.Begin);

            _mode = BinaryMode.KeepBinary;

            try
            {
                return Deserialize<T>();
            }
            finally
            {
                _mode = BinaryMode.Deserialize;

                Stream.Seek(retPos, SeekOrigin.Begin);
            }
        }

        /// <summary>
        /// Reads the binary object in binary form.
        /// </summary>
        private BinaryObject ReadAsBinary(int binaryBytesPos, int dataLen, bool doDetach)
        {
            try
            {
                Stream.Seek(dataLen + binaryBytesPos, SeekOrigin.Begin);

                var offs = Stream.ReadInt(); // offset inside data

                var pos = binaryBytesPos + offs;

                var hdr = BinaryObjectHeader.Read(Stream, pos);

                if (!doDetach)
                    return new BinaryObject(_marsh, Stream.GetArray(), pos, hdr);

                Stream.Seek(pos, SeekOrigin.Begin);

                return new BinaryObject(_marsh, Stream.ReadByteArray(hdr.Length), 0, hdr);
            }
            finally
            {
                Stream.Seek(binaryBytesPos + dataLen + 4, SeekOrigin.Begin);
            }
        }

        /// <summary>
        /// Reads the full object.
        /// </summary>
        [SuppressMessage("Microsoft.Performance", "CA1804:RemoveUnusedLocals", MessageId = "hashCode")]
        private T ReadFullObject<T>(int pos)
        {
            var hdr = BinaryObjectHeader.Read(Stream, pos);

            // Validate protocol version.
            BinaryUtils.ValidateProtocolVersion(hdr.Version);

            try
            {
                // Already read this object?
                object hndObj;

                if (_hnds != null && _hnds.TryGetValue(pos, out hndObj))
                    return (T) hndObj;

                if (hdr.IsUserType && _mode == BinaryMode.ForceBinary)
                {
                    BinaryObject portObj;

                    if (_detach)
                    {
                        Stream.Seek(pos, SeekOrigin.Begin);

                        portObj = new BinaryObject(_marsh, Stream.ReadByteArray(hdr.Length), 0, hdr);
                    }
                    else
                        portObj = new BinaryObject(_marsh, Stream.GetArray(), pos, hdr);

                    T obj = _builder == null ? TypeCaster<T>.Cast(portObj) : TypeCaster<T>.Cast(_builder.Child(portObj));

                    AddHandle(pos, obj);

                    return obj;
                }
                else
                {
                    // Find descriptor.
                    var desc = _marsh.GetDescriptor(hdr.IsUserType, hdr.TypeId);

                    // Instantiate object. 
                    if (desc.Type == null)
                    {
                        if (desc is BinarySurrogateTypeDescriptor)
                            throw new BinaryObjectException("Unknown type ID: " + hdr.TypeId);

                        throw new BinaryObjectException("No matching type found for object [typeId=" +
                                                        desc.TypeId + ", typeName=" + desc.TypeName + ']');
                    }

                    // Preserve old frame.
                    var oldHdr = _curHdr;
                    int oldPos = _curPos;
                    var oldStruct = _curStruct;
                    bool oldRaw = _curRaw;
                    var oldSchema = _curSchema;
                    var oldSchemaMap = _curSchemaMap;

                    // Set new frame.
                    _curHdr = hdr;
                    _curPos = pos;
                    SetCurSchema(desc);
                    _curStruct = new BinaryStructureTracker(desc, desc.ReaderTypeStructure);
                    _curRaw = false;

                    // Read object.
                    Stream.Seek(pos + BinaryObjectHeader.Size, SeekOrigin.Begin);

                    var obj = desc.Serializer.ReadBinary<T>(this, desc.Type, pos);

                    _curStruct.UpdateReaderStructure();

                    // Restore old frame.
                    _curHdr = oldHdr;
                    _curPos = oldPos;
                    _curStruct = oldStruct;
                    _curRaw = oldRaw;
                    _curSchema = oldSchema;
                    _curSchemaMap = oldSchemaMap;

                    return obj;
                }
            }
            finally
            {
                // Advance stream pointer.
                Stream.Seek(pos + hdr.Length, SeekOrigin.Begin);
            }
        }

        /// <summary>
        /// Sets the current schema.
        /// </summary>
        private void SetCurSchema(IBinaryTypeDescriptor desc)
        {
            if (_curHdr.HasSchema)
            {
                _curSchema = desc.Schema.Get(_curHdr.SchemaId);

                if (_curSchema == null)
                {
                    _curSchema = ReadSchema();

                    desc.Schema.Add(_curHdr.SchemaId, _curSchema);
                }
            }
        }

        /// <summary>
        /// Reads the schema.
        /// </summary>
        private int[] ReadSchema()
        {
            if (_curHdr.IsCompactFooter)
            {
                // Get schema from Java
                var schema = Marshaller.Ignite.ClusterGroup.GetSchema(_curHdr.TypeId, _curHdr.SchemaId);

                if (schema == null)
                    throw new BinaryObjectException("Cannot find schema for object with compact footer [" +
                        "typeId=" + _curHdr.TypeId + ", schemaId=" + _curHdr.SchemaId + ']');

                return schema;
            }

            Stream.Seek(_curPos + _curHdr.SchemaOffset, SeekOrigin.Begin);

            var count = _curHdr.SchemaFieldCount;

            var offsetSize = _curHdr.SchemaFieldOffsetSize;

            var res = new int[count];

            for (int i = 0; i < count; i++)
            {
                res[i] = Stream.ReadInt();
                Stream.Seek(offsetSize, SeekOrigin.Current);
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
        internal void AddHandle(int pos, object obj)
        {
            if (_hnds == null)
                _hnds = new BinaryReaderHandleDictionary(pos, obj);
            else
                _hnds.Add(pos, obj);
        }

        /// <summary>
        /// Underlying stream.
        /// </summary>
        public IBinaryStream Stream
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

                Stream.Seek(_curPos + _curHdr.GetRawOffset(Stream, _curPos), SeekOrigin.Begin);
            }
        }

        /// <summary>
        /// Seeks the field by name.
        /// </summary>
        private bool SeekField(string fieldName)
        {
            if (_curRaw)
                throw new BinaryObjectException("Cannot read named fields after raw data is read.");

            if (!_curHdr.HasSchema)
                return false;

            var actionId = _curStruct.CurStructAction;

            var fieldId = _curStruct.GetFieldId(fieldName);

            if (_curSchema == null || actionId >= _curSchema.Length || fieldId != _curSchema[actionId])
            {
                _curSchemaMap = _curSchemaMap ?? BinaryObjectSchemaSerializer.ReadSchema(Stream, _curPos, _curHdr,
                                    () => _curSchema).ToDictionary();

                _curSchema = null; // read order is different, ignore schema for future reads

                int pos;

                if (!_curSchemaMap.TryGetValue(fieldId, out pos))
                    return false;

                Stream.Seek(pos + _curPos, SeekOrigin.Begin);
            }

            return true;
        }

        /// <summary>
        /// Seeks specified field and invokes provided func.
        /// </summary>
        private T ReadField<T>(string fieldName, Func<IBinaryStream, T> readFunc, byte expHdr)
        {
            return SeekField(fieldName) ? Read(readFunc, expHdr) : default(T);
        }

        /// <summary>
        /// Seeks specified field and invokes provided func.
        /// </summary>
        private T ReadField<T>(string fieldName, Func<BinaryReader, T> readFunc, byte expHdr)
        {
            return SeekField(fieldName) ? Read(readFunc, expHdr) : default(T);
        }

        /// <summary>
        /// Seeks specified field and invokes provided func.
        /// </summary>
        private T ReadField<T>(string fieldName, Func<T> readFunc, byte expHdr)
        {
            return SeekField(fieldName) ? Read(readFunc, expHdr) : default(T);
        }

        /// <summary>
        /// Reads header and invokes specified func if the header is not null.
        /// </summary>
        private T Read<T>(Func<BinaryReader, T> readFunc, byte expHdr)
        {
            return Read(() => readFunc(this), expHdr);
        }

        /// <summary>
        /// Reads header and invokes specified func if the header is not null.
        /// </summary>
        private T Read<T>(Func<IBinaryStream, T> readFunc, byte expHdr)
        {
            return Read(() => readFunc(Stream), expHdr);
        }

        /// <summary>
        /// Reads header and invokes specified func if the header is not null.
        /// </summary>
        private T Read<T>(Func<T> readFunc, byte expHdr)
        {
            var hdr = ReadByte();

            if (hdr == BinaryUtils.HdrNull)
                return default(T);

            if (hdr == BinaryUtils.HdrHnd)
                return ReadHandleObject<T>(Stream.Position - 1);

            if (expHdr != hdr)
                throw new BinaryObjectException(string.Format("Invalid header on deserialization. " +
                                                          "Expected: {0} but was: {1}", expHdr, hdr));

            return readFunc();
        }

        /// <summary>
        /// Reads the enum.
        /// </summary>
        private static T ReadEnum0<T>(BinaryReader reader, bool keepBinary)
        {
            var enumType = reader.ReadInt();

            var enumValue = reader.ReadInt();

            if (!keepBinary)
                return BinaryUtils.GetEnumValue<T>(enumValue, enumType, reader.Marshaller);

            return TypeCaster<T>.Cast(new BinaryEnum(enumType, enumValue, reader.Marshaller));
        }
    }
}
