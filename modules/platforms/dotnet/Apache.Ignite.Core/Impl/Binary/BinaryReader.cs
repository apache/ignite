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

        /** Whether to read arrays lengths in varint encoding. */
        private readonly bool _useVarintArrayLength;

        /** Handles. */
        private BinaryReaderHandleDictionary _hnds;

        /** Detach flag. */
        private bool _detach;

        /** Binary read mode. */
        private BinaryMode _mode;

        /** Current frame. */
        private Frame _frame;

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
            _frame.Pos = stream.Position;
            _useVarintArrayLength = _marsh.UseVarintArrayLength;

            Stream = stream;
        }

        /// <summary>
        /// Gets the marshaller.
        /// </summary>
        public Marshaller Marshaller
        {
            get { return _marsh; }
        }

        /// <summary>
        /// Gets the mode.
        /// </summary>
        public BinaryMode Mode
        {
            get { return _mode; }
        }

        /// <summary>
        /// Indicates whether to read arrays lengths in varint encoding.
        /// </summary>
        internal bool UseVarintArrayLength
        {
            get { return _useVarintArrayLength; }
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
            return ReadField(fieldName, r => r.ReadBoolean(), BinaryTypeId.Bool);
        }

        /** <inheritdoc /> */
        public bool ReadBoolean()
        {
            return Stream.ReadBool();
        }

        /** <inheritdoc /> */
        public bool[] ReadBooleanArray(string fieldName)
        {
            return ReadField(fieldName, BinaryUtils.ReadBooleanArray, BinaryTypeId.ArrayBool);
        }

        /** <inheritdoc /> */
        public bool[] ReadBooleanArray()
        {
            return Read(BinaryUtils.ReadBooleanArray, BinaryTypeId.ArrayBool);
        }

        /** <inheritdoc /> */
        public byte ReadByte(string fieldName)
        {
            return ReadField(fieldName, ReadByte, BinaryTypeId.Byte);
        }

        /** <inheritdoc /> */
        public byte ReadByte()
        {
            return Stream.ReadByte();
        }

        /** <inheritdoc /> */
        public byte[] ReadByteArray(string fieldName)
        {
            return ReadField(fieldName, BinaryUtils.ReadByteArray, BinaryTypeId.ArrayByte);
        }

        /** <inheritdoc /> */
        public byte[] ReadByteArray()
        {
            return Read(BinaryUtils.ReadByteArray, BinaryTypeId.ArrayByte);
        }

        /** <inheritdoc /> */
        public short ReadShort(string fieldName)
        {
            return ReadField(fieldName, ReadShort, BinaryTypeId.Short);
        }

        /** <inheritdoc /> */
        public short ReadShort()
        {
            return Stream.ReadShort();
        }

        /** <inheritdoc /> */
        public short[] ReadShortArray(string fieldName)
        {
            return ReadField(fieldName, BinaryUtils.ReadShortArray, BinaryTypeId.ArrayShort);
        }

        /** <inheritdoc /> */
        public short[] ReadShortArray()
        {
            return Read(BinaryUtils.ReadShortArray, BinaryTypeId.ArrayShort);
        }

        /** <inheritdoc /> */
        public char ReadChar(string fieldName)
        {
            return ReadField(fieldName, ReadChar, BinaryTypeId.Char);
        }

        /** <inheritdoc /> */
        public char ReadChar()
        {
            return Stream.ReadChar();
        }

        /** <inheritdoc /> */
        public char[] ReadCharArray(string fieldName)
        {
            return ReadField(fieldName, BinaryUtils.ReadCharArray, BinaryTypeId.ArrayChar);
        }

        /** <inheritdoc /> */
        public char[] ReadCharArray()
        {
            return Read(BinaryUtils.ReadCharArray, BinaryTypeId.ArrayChar);
        }

        /** <inheritdoc /> */
        public int ReadInt(string fieldName)
        {
            return ReadField(fieldName, ReadInt, BinaryTypeId.Int);
        }

        /** <inheritdoc /> */
        public int ReadInt()
        {
            return Stream.ReadInt();
        }

        /** <inheritdoc /> */
        public int[] ReadIntArray(string fieldName)
        {
            return ReadField(fieldName, BinaryUtils.ReadIntArray, BinaryTypeId.ArrayInt);
        }

        /** <inheritdoc /> */
        public int[] ReadIntArray()
        {
            return Read(BinaryUtils.ReadIntArray, BinaryTypeId.ArrayInt);
        }

        /** <inheritdoc /> */
        public long ReadLong(string fieldName)
        {
            return ReadField(fieldName, ReadLong, BinaryTypeId.Long);
        }

        /** <inheritdoc /> */
        public long ReadLong()
        {
            return Stream.ReadLong();
        }

        /** <inheritdoc /> */
        public long[] ReadLongArray(string fieldName)
        {
            return ReadField(fieldName, BinaryUtils.ReadLongArray, BinaryTypeId.ArrayLong);
        }

        /** <inheritdoc /> */
        public long[] ReadLongArray()
        {
            return Read(BinaryUtils.ReadLongArray, BinaryTypeId.ArrayLong);
        }

        /** <inheritdoc /> */
        public float ReadFloat(string fieldName)
        {
            return ReadField(fieldName, ReadFloat, BinaryTypeId.Float);
        }

        /** <inheritdoc /> */
        public float ReadFloat()
        {
            return Stream.ReadFloat();
        }

        /** <inheritdoc /> */
        public float[] ReadFloatArray(string fieldName)
        {
            return ReadField(fieldName, BinaryUtils.ReadFloatArray, BinaryTypeId.ArrayFloat);
        }

        /** <inheritdoc /> */
        public float[] ReadFloatArray()
        {
            return Read(BinaryUtils.ReadFloatArray, BinaryTypeId.ArrayFloat);
        }

        /** <inheritdoc /> */
        public double ReadDouble(string fieldName)
        {
            return ReadField(fieldName, ReadDouble, BinaryTypeId.Double);
        }

        /** <inheritdoc /> */
        public double ReadDouble()
        {
            return Stream.ReadDouble();
        }

        /** <inheritdoc /> */
        public double[] ReadDoubleArray(string fieldName)
        {
            return ReadField(fieldName, BinaryUtils.ReadDoubleArray, BinaryTypeId.ArrayDouble);
        }

        /** <inheritdoc /> */
        public double[] ReadDoubleArray()
        {
            return Read(BinaryUtils.ReadDoubleArray, BinaryTypeId.ArrayDouble);
        }

        /** <inheritdoc /> */
        public decimal? ReadDecimal(string fieldName)
        {
            return ReadField(fieldName, BinaryUtils.ReadDecimal, BinaryTypeId.Decimal);
        }

        /** <inheritdoc /> */
        public decimal? ReadDecimal()
        {
            return Read(BinaryUtils.ReadDecimal, BinaryTypeId.Decimal);
        }

        /** <inheritdoc /> */
        public decimal?[] ReadDecimalArray(string fieldName)
        {
            return ReadField(fieldName, BinaryUtils.ReadDecimalArray, BinaryTypeId.ArrayDecimal);
        }

        /** <inheritdoc /> */
        public decimal?[] ReadDecimalArray()
        {
            return Read(BinaryUtils.ReadDecimalArray, BinaryTypeId.ArrayDecimal);
        }

        /** <inheritdoc /> */
        public DateTime? ReadTimestamp(string fieldName)
        {
            return ReadField(fieldName, BinaryUtils.ReadTimestamp, BinaryTypeId.Timestamp);
        }

        /** <inheritdoc /> */
        public DateTime? ReadTimestamp()
        {
            return Read(BinaryUtils.ReadTimestamp, BinaryTypeId.Timestamp);
        }
        
        /** <inheritdoc /> */
        public DateTime?[] ReadTimestampArray(string fieldName)
        {
            return ReadField(fieldName, BinaryUtils.ReadTimestampArray, BinaryTypeId.ArrayTimestamp);
        }
        
        /** <inheritdoc /> */
        public DateTime?[] ReadTimestampArray()
        {
            return Read(BinaryUtils.ReadTimestampArray, BinaryTypeId.ArrayTimestamp);
        }
        
        /** <inheritdoc /> */
        public string ReadString(string fieldName)
        {
            return ReadField(fieldName, BinaryUtils.ReadString, BinaryTypeId.String);
        }

        /** <inheritdoc /> */
        public string ReadString()
        {
            return Read(BinaryUtils.ReadString, BinaryTypeId.String);
        }

        /** <inheritdoc /> */
        public string[] ReadStringArray(string fieldName)
        {
            return ReadField(fieldName, r => BinaryUtils.ReadArray<string>(r, false), BinaryTypeId.ArrayString);
        }

        /** <inheritdoc /> */
        public string[] ReadStringArray()
        {
            return Read(r => BinaryUtils.ReadArray<string>(r, false), BinaryTypeId.ArrayString);
        }

        /** <inheritdoc /> */
        public Guid? ReadGuid(string fieldName)
        {
            return ReadField<Guid?>(fieldName, r => BinaryUtils.ReadGuid(r), BinaryTypeId.Guid);
        }

        /** <inheritdoc /> */
        public Guid? ReadGuid()
        {
            return Read<Guid?>(r => BinaryUtils.ReadGuid(r), BinaryTypeId.Guid);
        }

        /** <inheritdoc /> */
        public Guid?[] ReadGuidArray(string fieldName)
        {
            return ReadField(fieldName, r => BinaryUtils.ReadArray<Guid?>(r, false), BinaryTypeId.ArrayGuid);
        }

        /** <inheritdoc /> */
        public Guid?[] ReadGuidArray()
        {
            return Read(r => BinaryUtils.ReadArray<Guid?>(r, false), BinaryTypeId.ArrayGuid);
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

                case BinaryTypeId.Enum:
                    return ReadEnum0<T>(this, _mode == BinaryMode.ForceBinary);

                case BinaryTypeId.BinaryEnum:
                    return ReadEnum0<T>(this, _mode != BinaryMode.Deserialize);

                case BinaryUtils.HdrFull:
                    // Unregistered enum written as serializable
                    Stream.Seek(-1, SeekOrigin.Current);

                    return ReadObject<T>(); 

                default:
                    throw new BinaryObjectException(string.Format(
                        "Invalid header on enum deserialization. Expected: {0} or {1} or {2} but was: {3}",
                            BinaryTypeId.Enum, BinaryTypeId.BinaryEnum, BinaryUtils.HdrFull, hdr));
            }
        }

        /** <inheritdoc /> */
        public T[] ReadEnumArray<T>(string fieldName)
        {
            return ReadField(fieldName, r => BinaryUtils.ReadArray<T>(r, true), BinaryTypeId.ArrayEnum);
        }

        /** <inheritdoc /> */
        public T[] ReadEnumArray<T>()
        {
            return Read(r => BinaryUtils.ReadArray<T>(r, true), BinaryTypeId.ArrayEnum);
        }

        /** <inheritdoc /> */
        public T ReadObject<T>(string fieldName)
        {
            if (_frame.Raw)
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
            return ReadField(fieldName, r => BinaryUtils.ReadArray<T>(r, true), BinaryTypeId.Array);
        }

        /** <inheritdoc /> */
        public T[] ReadArray<T>()
        {
            return Read(r => BinaryUtils.ReadArray<T>(r, true), BinaryTypeId.Array);
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
            return ReadField(fieldName, r => BinaryUtils.ReadCollection(r, factory, adder), BinaryTypeId.Collection);
        }

        /** <inheritdoc /> */
        public ICollection ReadCollection(Func<int, ICollection> factory, Action<ICollection, object> adder)
        {
            return Read(r => BinaryUtils.ReadCollection(r, factory, adder), BinaryTypeId.Collection);
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
            return ReadField(fieldName, r => BinaryUtils.ReadDictionary(r, factory), BinaryTypeId.Dictionary);
        }

        /** <inheritdoc /> */
        public IDictionary ReadDictionary(Func<int, IDictionary> factory)
        {
            return Read(r => BinaryUtils.ReadDictionary(r, factory), BinaryTypeId.Dictionary);
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
        /// <param name="typeOverride">The type override.
        /// There can be multiple versions of the same type when peer assembly loading is enabled.
        /// Only first one is registered in Marshaller.
        /// This parameter specifies exact type to be instantiated.</param>
        /// <returns>Deserialized object.</returns>
        public T Deserialize<T>(Type typeOverride = null)
        {
            T res;

            // ReSharper disable once CompareNonConstrainedGenericWithNull
            if (!TryDeserialize(out res, typeOverride) && default(T) != null)
                throw new BinaryObjectException(string.Format("Invalid data on deserialization. " +
                    "Expected: '{0}' But was: null", typeof (T)));

            return res;
        }

        /// <summary>
        /// Deserialize object.
        /// </summary>
        /// <param name="res">Deserialized object.</param>
        /// <param name="typeOverride">The type override.
        /// There can be multiple versions of the same type when peer assembly loading is enabled.
        /// Only first one is registered in Marshaller.
        /// This parameter specifies exact type to be instantiated.</param>
        /// <returns>
        /// Deserialized object.
        /// </returns>
        public bool TryDeserialize<T>(out T res, Type typeOverride = null)
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
                    res = ReadHandleObject<T>(pos, typeOverride);

                    return true;

                case BinaryUtils.HdrFull:
                    res = ReadFullObject<T>(pos, typeOverride);

                    return true;

                case BinaryTypeId.Binary:
                    res = ReadBinaryObject<T>(doDetach);

                    return true;

                case BinaryTypeId.Enum:
                    res = ReadEnum0<T>(this, _mode == BinaryMode.ForceBinary);

                    return true;

                case BinaryTypeId.BinaryEnum:
                    res = ReadEnum0<T>(this, _mode != BinaryMode.Deserialize);

                    return true;
            }

            if (BinarySystemHandlers.TryReadSystemType(hdr, this, out res))
                return true;

            throw new BinaryObjectException("Invalid header on deserialization [pos=" + pos + ", hdr=" + hdr + ']');
        }

        /// <summary>
        /// Gets the flag indicating that there is custom type information in raw region.
        /// </summary>
        public bool GetCustomTypeDataFlag()
        {
            return _frame.Hdr.IsCustomDotNetType;
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
        /// <param name="pos">The position.</param>
        /// <param name="typeOverride">The type override.
        /// There can be multiple versions of the same type when peer assembly loading is enabled.
        /// Only first one is registered in Marshaller.
        /// This parameter specifies exact type to be instantiated.</param>
        /// <returns>Resulting object</returns>
        [SuppressMessage("Microsoft.Performance", "CA1804:RemoveUnusedLocals", MessageId = "hashCode")]
        private T ReadFullObject<T>(int pos, Type typeOverride)
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
                    var desc = hdr.TypeId == BinaryTypeId.Unregistered
                        ? _marsh.GetDescriptor(ReadUnregisteredType(typeOverride))
                        : _marsh.GetDescriptor(hdr.IsUserType, hdr.TypeId, true, null, typeOverride);

                    // Instantiate object. 
                    if (desc.Type == null)
                    {
                        throw new BinaryObjectException(string.Format(
                            "No matching type found for object [typeId={0}, typeName={1}]. " +
                            "This usually indicates that assembly with specified type is not loaded on a node. " +
                            "When using Apache.Ignite.exe, make sure to load assemblies with -assembly parameter. " +
                            "Alternatively, set IgniteConfiguration.PeerAssemblyLoadingEnabled to true.",
                            desc.TypeId, desc.TypeName));
                    }

                    // Preserve old frame.
                    var oldFrame = _frame;

                    // Set new frame.
                    _frame.Hdr = hdr;
                    _frame.Pos = pos;
                    SetCurSchema(desc);
                    _frame.Struct = new BinaryStructureTracker(desc, desc.ReaderTypeStructure);
                    _frame.Raw = false;

                    // Read object.
                    var obj = desc.Serializer.ReadBinary<T>(this, desc, pos, typeOverride);

                    _frame.Struct.UpdateReaderStructure();

                    // Restore old frame.
                    _frame = oldFrame;

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
        /// Reads the unregistered type.
        /// </summary>
        private Type ReadUnregisteredType(Type knownType)
        {
            var typeName = ReadString();  // Must read always.

            return knownType ?? Marshaller.ResolveType(typeName);
        }

        /// <summary>
        /// Sets the current schema.
        /// </summary>
        private void SetCurSchema(IBinaryTypeDescriptor desc)
        {
            _frame.SchemaMap = null;

            if (_frame.Hdr.HasSchema)
            {
                _frame.Schema = desc.Schema.Get(_frame.Hdr.SchemaId);

                if (_frame.Schema == null)
                {
                    _frame.Schema = 
                        BinaryObjectSchemaSerializer.GetFieldIds(_frame.Hdr, Marshaller.Ignite, Stream, _frame.Pos);

                    desc.Schema.Add(_frame.Hdr.SchemaId, _frame.Schema);
                }
            }
            else
            {
                _frame.Schema = null;
            }
        }

        /// <summary>
        /// Reads the handle object.
        /// </summary>
        private T ReadHandleObject<T>(int pos, Type typeOverride)
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

                        hndObj = Deserialize<T>(typeOverride);
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
        /// Seeks to raw data.
        /// </summary>
        internal void SeekToRaw()
        {
            Stream.Seek(_frame.Pos + _frame.Hdr.GetRawOffset(Stream, _frame.Pos), SeekOrigin.Begin);
        }

        /// <summary>
        /// Mark current output as raw. 
        /// </summary>
        private void MarkRaw()
        {
            if (!_frame.Raw)
            {
                _frame.Raw = true;

                SeekToRaw();
            }
        }

        /// <summary>
        /// Seeks the field by name.
        /// </summary>
        private bool SeekField(string fieldName)
        {
            if (_frame.Raw)
                throw new BinaryObjectException("Cannot read named fields after raw data is read.");

            if (!_frame.Hdr.HasSchema)
                return false;

            var actionId = _frame.Struct.CurStructAction;

            var fieldId = _frame.Struct.GetFieldId(fieldName);

            if (_frame.Schema == null || actionId >= _frame.Schema.Length || fieldId != _frame.Schema[actionId])
            {
                _frame.SchemaMap = _frame.SchemaMap ?? BinaryObjectSchemaSerializer.ReadSchema(Stream, _frame.Pos,
                    _frame.Hdr, () => _frame.Schema).ToDictionary();

                _frame.Schema = null; // read order is different, ignore schema for future reads

                int pos;

                if (_frame.SchemaMap == null || !_frame.SchemaMap.TryGetValue(fieldId, out pos))
                    return false;

                Stream.Seek(pos + _frame.Pos, SeekOrigin.Begin);
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
        private T ReadField<T>(string fieldName, Func<IBinaryStream, bool, T> readFunc, byte expHdr)
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
        private T Read<T>(Func<IBinaryStream, bool, T> readFunc, byte expHdr)
        {
            return Read(() => readFunc(Stream, _useVarintArrayLength), expHdr);
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
                return ReadHandleObject<T>(Stream.Position - 1, null);

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
            {
                return BinaryUtils.GetEnumValue<T>(enumValue, enumType, reader.Marshaller);
            }

            return TypeCaster<T>.Cast(new BinaryEnum(enumType, enumValue, reader.Marshaller));
        }

        /// <summary>
        /// Stores current reader stack frame.
        /// </summary>
        private struct Frame
        {
            /** Current position. */
            public int Pos;

            /** Current raw flag. */
            public bool Raw;

            /** Current type structure tracker. */
            public BinaryStructureTracker Struct;

            /** Current schema. */
            public int[] Schema;

            /** Current schema with positions. */
            public Dictionary<int, int> SchemaMap;

            /** Current header. */
            public BinaryObjectHeader Hdr;
        }
    }
}
