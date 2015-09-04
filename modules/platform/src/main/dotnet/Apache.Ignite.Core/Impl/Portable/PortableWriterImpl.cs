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
    using Apache.Ignite.Core.Impl.Portable.IO;
    using Apache.Ignite.Core.Impl.Portable.Metadata;
    using Apache.Ignite.Core.Portable;

    /// <summary>
    /// Portable writer implementation.
    /// </summary>
    internal class PortableWriterImpl : IPortableWriter, IPortableRawWriter
    {
        /** Marshaller. */
        private readonly PortableMarshaller _marsh;

        /** Stream. */
        private readonly IPortableStream _stream;

        /** Builder (used only during build). */
        private PortableBuilderImpl _builder;

        /** Handles. */
        private PortableHandleDictionary<object, long> _hnds;

        /** Metadatas collected during this write session. */
        private IDictionary<int, IPortableMetadata> _metas;

        /** Current type ID. */
        private int _curTypeId;

        /** Current name converter */
        private IPortableNameMapper _curConverter;

        /** Current mapper. */
        private IPortableIdMapper _curMapper;

        /** Current metadata handler. */
        private IPortableMetadataHandler _curMetaHnd;

        /** Current raw flag. */
        private bool _curRaw;

        /** Current raw position. */
        private long _curRawPos;

        /** Ignore handles flag. */
        private bool _detach;

        /** Object started ignore mode. */
        private bool _detachMode;

        /// <summary>
        /// Gets the marshaller.
        /// </summary>
        internal PortableMarshaller Marshaller
        {
            get { return _marsh; }
        }

        /// <summary>
        /// Write named boolean value.
        /// </summary>
        /// <param name="fieldName">Field name.</param>
        /// <param name="val">Boolean value.</param>
        public void WriteBoolean(string fieldName, bool val)
        {
            WriteSimpleField(fieldName, PortableUtils.TypeBool, val, PortableSystemHandlers.WriteHndBoolTyped, 1);
        }
        
        /// <summary>
        /// Write boolean value.
        /// </summary>
        /// <param name="val">Boolean value.</param>
        public void WriteBoolean(bool val)
        {
            _stream.WriteBool(val);
        }

        /// <summary>
        /// Write named boolean array.
        /// </summary>
        /// <param name="fieldName">Field name.</param>
        /// <param name="val">Boolean array.</param>
        public void WriteBooleanArray(string fieldName, bool[] val)
        {
            WriteSimpleNullableField(fieldName, PortableUtils.TypeArrayBool, val,
                PortableSystemHandlers.WriteHndBoolArrayTyped, val != null ? val.Length + 4 : 0);
        }

        /// <summary>
        /// Write boolean array.
        /// </summary>
        /// <param name="val">Boolean array.</param>
        public void WriteBooleanArray(bool[] val)
        {
            WriteSimpleNullableRawField(val, PortableSystemHandlers.WriteHndBoolArrayTyped);
        }

        /// <summary>
        /// Write named byte value.
        /// </summary>
        /// <param name="fieldName">Field name.</param>
        /// <param name="val">Byte value.</param>
        public void WriteByte(string fieldName, byte val)
        {
            WriteSimpleField(fieldName, PortableUtils.TypeByte, val, PortableSystemHandlers.WriteHndByteTyped, 1);
        }

        /// <summary>
        /// Write byte value.
        /// </summary>
        /// <param name="val">Byte value.</param>
        public void WriteByte(byte val)
        {
            _stream.WriteByte(val);
        }

        /// <summary>
        /// Write named byte array.
        /// </summary>
        /// <param name="fieldName">Field name.</param>
        /// <param name="val">Byte array.</param>
        public void WriteByteArray(string fieldName, byte[] val)
        {
            WriteSimpleNullableField(fieldName, PortableUtils.TypeArrayByte, val,
                PortableSystemHandlers.WriteHndByteArrayTyped, val != null ? val.Length + 4 : 0);
        }

        /// <summary>
        /// Write byte array.
        /// </summary>
        /// <param name="val">Byte array.</param>
        public void WriteByteArray(byte[] val)
        {
            WriteSimpleNullableRawField(val, PortableSystemHandlers.WriteHndByteArrayTyped);
        }

        /// <summary>
        /// Write named short value.
        /// </summary>
        /// <param name="fieldName">Field name.</param>
        /// <param name="val">Short value.</param>
        public void WriteShort(string fieldName, short val)
        {
            WriteSimpleField(fieldName, PortableUtils.TypeShort, val, PortableSystemHandlers.WriteHndShortTyped, 2);
        }

        /// <summary>
        /// Write short value.
        /// </summary>
        /// <param name="val">Short value.</param>
        public void WriteShort(short val)
        {
            _stream.WriteShort(val);
        }

        /// <summary>
        /// Write named short array.
        /// </summary>
        /// <param name="fieldName">Field name.</param>
        /// <param name="val">Short array.</param>
        public void WriteShortArray(string fieldName, short[] val)
        {
            WriteSimpleNullableField(fieldName, PortableUtils.TypeArrayShort, val,
                PortableSystemHandlers.WriteHndShortArrayTyped, val != null ? 2 * val.Length + 4 : 0);
        }

        /// <summary>
        /// Write short array.
        /// </summary>
        /// <param name="val">Short array.</param>
        public void WriteShortArray(short[] val)
        {
            WriteSimpleNullableRawField(val, PortableSystemHandlers.WriteHndShortArrayTyped);
        }

        /// <summary>
        /// Write named char value.
        /// </summary>
        /// <param name="fieldName">Field name.</param>
        /// <param name="val">Char value.</param>
        public void WriteChar(string fieldName, char val)
        {
            WriteSimpleField(fieldName, PortableUtils.TypeChar, val, PortableSystemHandlers.WriteHndCharTyped, 2);
        }

        /// <summary>
        /// Write char value.
        /// </summary>
        /// <param name="val">Char value.</param>
        public void WriteChar(char val)
        {
            _stream.WriteChar(val);
        }

        /// <summary>
        /// Write named char array.
        /// </summary>
        /// <param name="fieldName">Field name.</param>
        /// <param name="val">Char array.</param>
        public void WriteCharArray(string fieldName, char[] val)
        {
            WriteSimpleNullableField(fieldName, PortableUtils.TypeArrayChar, val,
                PortableSystemHandlers.WriteHndCharArrayTyped, val != null ? 2 * val.Length + 4 : 0);
        }

        /// <summary>
        /// Write char array.
        /// </summary>
        /// <param name="val">Char array.</param>
        public void WriteCharArray(char[] val)
        {
            WriteSimpleNullableRawField(val, PortableSystemHandlers.WriteHndCharArrayTyped);
        }

        /// <summary>
        /// Write named int value.
        /// </summary>
        /// <param name="fieldName">Field name.</param>
        /// <param name="val">Int value.</param>
        public void WriteInt(string fieldName, int val)
        {
            WriteSimpleField(fieldName, PortableUtils.TypeInt, val, PortableSystemHandlers.WriteHndIntTyped, 4);
        }

        /// <summary>
        /// Write int value.
        /// </summary>
        /// <param name="val">Int value.</param>
        public void WriteInt(int val)
        {
            _stream.WriteInt(val);
        }

        /// <summary>
        /// Write named int array.
        /// </summary>
        /// <param name="fieldName">Field name.</param>
        /// <param name="val">Int array.</param>
        public void WriteIntArray(string fieldName, int[] val)
        {
            WriteSimpleNullableField(fieldName, PortableUtils.TypeArrayInt, val,
                PortableSystemHandlers.WriteHndIntArrayTyped, val != null ? 4 * val.Length + 4 : 0);
        }

        /// <summary>
        /// Write int array.
        /// </summary>
        /// <param name="val">Int array.</param>
        public void WriteIntArray(int[] val)
        {
            WriteSimpleNullableRawField(val, PortableSystemHandlers.WriteHndIntArrayTyped);
        }

        /// <summary>
        /// Write named long value.
        /// </summary>
        /// <param name="fieldName">Field name.</param>
        /// <param name="val">Long value.</param>
        public void WriteLong(string fieldName, long val)
        {
            WriteSimpleField(fieldName, PortableUtils.TypeLong, val, PortableSystemHandlers.WriteHndLongTyped, 8);
        }

        /// <summary>
        /// Write long value.
        /// </summary>
        /// <param name="val">Long value.</param>
        public void WriteLong(long val)
        {
            _stream.WriteLong(val);
        }

        /// <summary>
        /// Write named long array.
        /// </summary>
        /// <param name="fieldName">Field name.</param>
        /// <param name="val">Long array.</param>
        public void WriteLongArray(string fieldName, long[] val)
        {
            WriteSimpleNullableField(fieldName, PortableUtils.TypeArrayLong, val,
                PortableSystemHandlers.WriteHndLongArrayTyped, val != null ? 8 * val.Length + 4 : 0);
        }

        /// <summary>
        /// Write long array.
        /// </summary>
        /// <param name="val">Long array.</param>
        public void WriteLongArray(long[] val)
        {
            WriteSimpleNullableRawField(val, PortableSystemHandlers.WriteHndLongArrayTyped);
        }

        /// <summary>
        /// Write named float value.
        /// </summary>
        /// <param name="fieldName">Field name.</param>
        /// <param name="val">Float value.</param>
        public void WriteFloat(string fieldName, float val)
        {
            WriteSimpleField(fieldName, PortableUtils.TypeFloat, val, PortableSystemHandlers.WriteHndFloatTyped, 4);
        }

        /// <summary>
        /// Write float value.
        /// </summary>
        /// <param name="val">Float value.</param>
        public void WriteFloat(float val)
        {
            _stream.WriteFloat(val);
        }

        /// <summary>
        /// Write named float array.
        /// </summary>
        /// <param name="fieldName">Field name.</param>
        /// <param name="val">Float array.</param>
        public void WriteFloatArray(string fieldName, float[] val)
        {
            WriteSimpleNullableField(fieldName, PortableUtils.TypeArrayFloat, val,
                PortableSystemHandlers.WriteHndFloatArrayTyped, val != null ? 4 * val.Length + 4 : 0);
        }

        /// <summary>
        /// Write float array.
        /// </summary>
        /// <param name="val">Float array.</param>
        public void WriteFloatArray(float[] val)
        {
            WriteSimpleNullableRawField(val, PortableSystemHandlers.WriteHndFloatArrayTyped);
        }

        /// <summary>
        /// Write named double value.
        /// </summary>
        /// <param name="fieldName">Field name.</param>
        /// <param name="val">Double value.</param>
        public void WriteDouble(string fieldName, double val)
        {
            WriteSimpleField(fieldName, PortableUtils.TypeDouble, val, PortableSystemHandlers.WriteHndDoubleTyped, 8);
        }

        /// <summary>
        /// Write double value.
        /// </summary>
        /// <param name="val">Double value.</param>
        public void WriteDouble(double val)
        {
            _stream.WriteDouble(val);
        }

        /// <summary>
        /// Write named double array.
        /// </summary>
        /// <param name="fieldName">Field name.</param>
        /// <param name="val">Double array.</param>
        public void WriteDoubleArray(string fieldName, double[] val)
        {
            WriteSimpleNullableField(fieldName, PortableUtils.TypeArrayDouble, val,
                PortableSystemHandlers.WriteHndDoubleArrayTyped, val != null ? 8 * val.Length + 4 : 0);
        }

        /// <summary>
        /// Write double array.
        /// </summary>
        /// <param name="val">Double array.</param>
        public void WriteDoubleArray(double[] val)
        {
            WriteSimpleNullableRawField(val, PortableSystemHandlers.WriteHndDoubleArrayTyped);
        }

        /// <summary>
        /// Write named decimal value.
        /// </summary>
        /// <param name="fieldName">Field name.</param>
        /// <param name="val">Decimal value.</param>
        public void WriteDecimal(string fieldName, decimal val)
        {
            WriteSimpleNullableField(fieldName, PortableUtils.TypeDecimal, val, PortableSystemHandlers.WriteHndDecimalTyped);
        }

        /// <summary>
        /// Write decimal value.
        /// </summary>
        /// <param name="val">Decimal value.</param>
        public void WriteDecimal(decimal val)
        {
            WriteSimpleNullableRawField(val, PortableSystemHandlers.WriteHndDecimalTyped);
        }

        /// <summary>
        /// Write named decimal array.
        /// </summary>
        /// <param name="fieldName">Field name.</param>
        /// <param name="val">Decimal array.</param>
        public void WriteDecimalArray(string fieldName, decimal[] val)
        {
            WriteSimpleNullableField(fieldName, PortableUtils.TypeArrayDecimal, val,
                PortableSystemHandlers.WriteHndDecimalArrayTyped);
        }
        
        /// <summary>
        /// Write decimal array.
        /// </summary>
        /// <param name="val">Decimal array.</param>
        public void WriteDecimalArray(decimal[] val)
        {
            WriteSimpleNullableRawField(val, PortableSystemHandlers.WriteHndDecimalArrayTyped);
        }

        /// <summary>
        /// Write named date value.
        /// </summary>
        /// <param name="fieldName">Field name.</param>
        /// <param name="val">Date value.</param>
        public void WriteDate(string fieldName, DateTime? val)
        {
            WriteSimpleNullableField(fieldName, PortableUtils.TypeDate, val, PortableSystemHandlers.WriteHndDateTyped,
                val.HasValue ? 12 : 0);
        }
        
        /// <summary>
        /// Write date value.
        /// </summary>
        /// <param name="val">Date value.</param>
        public void WriteDate(DateTime? val)
        {
            WriteSimpleNullableRawField(val, PortableSystemHandlers.WriteHndDateTyped);
        }

        /// <summary>
        /// Write named date array.
        /// </summary>
        /// <param name="fieldName">Field name.</param>
        /// <param name="val">Date array.</param>
        public void WriteDateArray(string fieldName, DateTime?[] val)
        {
            WriteSimpleNullableField(fieldName, PortableUtils.TypeArrayDate, val,
                PortableSystemHandlers.WriteHndDateArrayTyped);
        }

        /// <summary>
        /// Write date array.
        /// </summary>
        /// <param name="val">Date array.</param>
        public void WriteDateArray(DateTime?[] val)
        {
            WriteSimpleNullableRawField(val, PortableSystemHandlers.WriteHndDateArrayTyped);
        }

        /// <summary>
        /// Write named string value.
        /// </summary>
        /// <param name="fieldName">Field name.</param>
        /// <param name="val">String value.</param>
        public void WriteString(string fieldName, string val)
        {
            WriteSimpleNullableField(fieldName, PortableUtils.TypeString, val, PortableSystemHandlers.WriteHndStringTyped);
        }

        /// <summary>
        /// Write string value.
        /// </summary>
        /// <param name="val">String value.</param>
        public void WriteString(string val)
        {
            WriteSimpleNullableRawField(val, PortableSystemHandlers.WriteHndStringTyped);
        }

        /// <summary>
        /// Write named string array.
        /// </summary>
        /// <param name="fieldName">Field name.</param>
        /// <param name="val">String array.</param>
        public void WriteStringArray(string fieldName, string[] val)
        {
            WriteSimpleNullableField(fieldName, PortableUtils.TypeArrayString, val,
                PortableSystemHandlers.WriteHndStringArrayTyped);
        }

        /// <summary>
        /// Write string array.
        /// </summary>
        /// <param name="val">String array.</param>
        public void WriteStringArray(string[] val)
        {
            WriteSimpleNullableRawField(val, PortableSystemHandlers.WriteHndStringArrayTyped);
        }

        /// <summary>
        /// Write named GUID value.
        /// </summary>
        /// <param name="fieldName">Field name.</param>
        /// <param name="val">GUID value.</param>
        public void WriteGuid(string fieldName, Guid? val)
        {
            WriteSimpleNullableField(fieldName, PortableUtils.TypeGuid, val, PortableSystemHandlers.WriteHndGuidTyped,
                val.HasValue ? 16 : 0);
        }

        /// <summary>
        /// Write GUID value.
        /// </summary>
        /// <param name="val">GUID value.</param>
        public void WriteGuid(Guid? val)
        {
            WriteSimpleNullableRawField(val, PortableSystemHandlers.WriteHndGuidTyped);
        }

        /// <summary>
        /// Write named GUID array.
        /// </summary>
        /// <param name="fieldName">Field name.</param>
        /// <param name="val">GUID array.</param>
        public void WriteGuidArray(string fieldName, Guid?[] val)
        {
            WriteSimpleNullableField(fieldName, PortableUtils.TypeArrayGuid, val,
                PortableSystemHandlers.WriteHndGuidArrayTyped);
        }

        /// <summary>
        /// Write GUID array.
        /// </summary>
        /// <param name="val">GUID array.</param>
        public void WriteGuidArray(Guid?[] val)
        {
            WriteSimpleNullableRawField(val, PortableSystemHandlers.WriteHndGuidArrayTyped);
        }

        /// <summary>
        /// Write named enum value.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="fieldName">Field name.</param>
        /// <param name="val">Enum value.</param>
        public void WriteEnum<T>(string fieldName, T val)
        {
            WriteField(fieldName, PortableUtils.TypeEnum, val, PortableSystemHandlers.WriteHndEnum);
        }

        /// <summary>
        /// Write enum value.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="val">Enum value.</param>
        public void WriteEnum<T>(T val)
        {
            Write(val, PortableSystemHandlers.WriteHndEnum);
        }

        /// <summary>
        /// Write named enum array.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="fieldName">Field name.</param>
        /// <param name="val">Enum array.</param>
        public void WriteEnumArray<T>(string fieldName, T[] val)
        {
            WriteField(fieldName, PortableUtils.TypeArrayEnum, val, PortableSystemHandlers.WriteHndEnumArray);
        }

        /// <summary>
        /// Write enum array.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="val">Enum array.</param>
        public void WriteEnumArray<T>(T[] val)
        {
            Write(val, PortableSystemHandlers.WriteHndEnumArray);
        }

        /// <summary>
        /// Write named object value.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="fieldName">Field name.</param>
        /// <param name="val">Object value.</param>
        public void WriteObject<T>(string fieldName, T val)
        {
            WriteField(fieldName, PortableUtils.TypeObject, val, null);
        }

        /// <summary>
        /// Write object value.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="val">Object value.</param>
        public void WriteObject<T>(T val)
        {
            Write(val);
        }

        /// <summary>
        /// Write named object array.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="fieldName">Field name.</param>
        /// <param name="val">Object array.</param>
        public void WriteObjectArray<T>(string fieldName, T[] val)
        {
            WriteField(fieldName, PortableUtils.TypeArray, val, PortableSystemHandlers.WriteHndArray);
        }

        /// <summary>
        /// Write object array.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="val">Object array.</param>
        public void WriteObjectArray<T>(T[] val)
        {
            Write(val, PortableSystemHandlers.WriteHndArray);
        }

        /// <summary>
        /// Write named collection.
        /// </summary>
        /// <param name="fieldName">Field name.</param>
        /// <param name="val">Collection.</param>
        public void WriteCollection(string fieldName, ICollection val)
        {
            WriteField(fieldName, PortableUtils.TypeCollection, val, null);
        }

        /// <summary>
        /// Write collection.
        /// </summary>
        /// <param name="val">Collection.</param>
        public void WriteCollection(ICollection val)
        {
            Write(val);
        }

        /// <summary>
        /// Write named generic collection.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="fieldName">Field name.</param>
        /// <param name="val">Collection.</param>
        public void WriteGenericCollection<T>(string fieldName, ICollection<T> val)
        {
            WriteField(fieldName, PortableUtils.TypeCollection, val, null);
        }

        /// <summary>
        /// Write generic collection.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="val">Collection.</param>
        public void WriteGenericCollection<T>(ICollection<T> val)
        {
            Write(val);
        }

        /// <summary>
        /// Write named dictionary.
        /// </summary>
        /// <param name="fieldName">Field name.</param>
        /// <param name="val">Dictionary.</param>
        public void WriteDictionary(string fieldName, IDictionary val)
        {
            WriteField(fieldName, PortableUtils.TypeDictionary, val, null);
        }

        /// <summary>
        /// Write dictionary.
        /// </summary>
        /// <param name="val">Dictionary.</param>
        public void WriteDictionary(IDictionary val)
        {
            Write(val);
        }

        /// <summary>
        /// Write named generic dictionary.
        /// </summary>
        /// <param name="fieldName">Field name.</param>
        /// <param name="val">Dictionary.</param>
        public void WriteGenericDictionary<TK, TV>(string fieldName, IDictionary<TK, TV> val)
        {
            WriteField(fieldName, PortableUtils.TypeDictionary, val, null);
        }

        /// <summary>
        /// Write generic dictionary.
        /// </summary>
        /// <param name="val">Dictionary.</param>
        public void WriteGenericDictionary<TK, TV>(IDictionary<TK, TV> val)
        {
            Write(val);
        }

        /// <summary>
        /// Get raw writer.
        /// </summary>
        /// <returns>
        /// Raw writer.
        /// </returns>
        public IPortableRawWriter RawWriter()
        {
            if (!_curRaw)
            {
                _curRaw = true;
                _curRawPos = _stream.Position;
            }

            return this;
        }

        /// <summary>
        /// Set new builder.
        /// </summary>
        /// <param name="builder">Builder.</param>
        /// <returns>Previous builder.</returns>
        internal PortableBuilderImpl Builder(PortableBuilderImpl builder)
        {
            PortableBuilderImpl ret = _builder;

            _builder = builder;

            return ret;
        }

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="marsh">Marshaller.</param>
        /// <param name="stream">Stream.</param>
        internal PortableWriterImpl(PortableMarshaller marsh, IPortableStream stream)
        {
            _marsh = marsh;
            _stream = stream;
        }

        /// <summary>
        /// Write object.
        /// </summary>
        /// <param name="obj">Object.</param>
        internal void Write<T>(T obj)
        {
            Write(obj, null);
        }

        /// <summary>
        /// Write object.
        /// </summary>
        /// <param name="obj">Object.</param>
        /// <param name="handler">Optional write handler.</param>
        [SuppressMessage("ReSharper", "FunctionComplexityOverflow")]
        internal void Write<T>(T obj, object handler)
        {
            // Apply detach mode if needed.
            PortableHandleDictionary<object, long> oldHnds = null;

            bool resetDetach = false;

            if (_detach)
            {
                _detach = false;
                _detachMode = true;
                resetDetach = true;

                oldHnds = _hnds;

                _hnds = null;
            }

            try
            {
                // Write null.
                if (obj == null)
                {
                    _stream.WriteByte(PortableUtils.HdrNull);

                    return;
                }

                if (_builder != null)
                {
                    // Special case for portable object during build.
                    PortableUserObject portObj = obj as PortableUserObject;

                    if (portObj != null)
                    {
                        if (!WriteHandle(_stream.Position, portObj))
                            _builder.ProcessPortable(_stream, portObj);

                        return;
                    }

                    // Special case for builder during build.
                    PortableBuilderImpl portBuilder = obj as PortableBuilderImpl;

                    if (portBuilder != null)
                    {
                        if (!WriteHandle(_stream.Position, portBuilder))
                            _builder.ProcessBuilder(_stream, portBuilder);

                        return;
                    }
                }                

                // Try writting as well-known type.
                if (InvokeHandler(handler, handler as PortableSystemWriteDelegate, obj))
                    return;

                Type type = obj.GetType();

                IPortableTypeDescriptor desc = _marsh.Descriptor(type);

                object typedHandler;
                PortableSystemWriteDelegate untypedHandler;

                if (desc == null)
                {
                    typedHandler = null;
                    untypedHandler = PortableSystemHandlers.WriteHandler(type);
                }
                else
                {
                    typedHandler = desc.TypedHandler;
                    untypedHandler = desc.UntypedHandler;
                }

                if (InvokeHandler(typedHandler, untypedHandler, obj))
                    return;

                if (desc == null)
                {
                    if (!type.IsSerializable)
                        // If neither handler, nor descriptor exist, and not serializable, this is an exception.
                        throw new PortableException("Unsupported object type [type=" + type +
                            ", object=" + obj + ']');

                    Write(new SerializableObjectHolder(obj));

                    return;
                }

                int pos = _stream.Position;

                // Dealing with handles.
                if (!(desc.Serializer is IPortableSystemTypeSerializer) && WriteHandle(pos, obj))
                    return;

                // Write header.
                _stream.WriteByte(PortableUtils.HdrFull);

                _stream.WriteBool(desc.UserType);
                _stream.WriteInt(desc.TypeId);
                _stream.WriteInt(obj.GetHashCode());

                // Skip length as it is not known in the first place.
                _stream.Seek(8, SeekOrigin.Current);

                // Preserve old frame.
                int oldTypeId = _curTypeId;
                IPortableNameMapper oldConverter = _curConverter;
                IPortableIdMapper oldMapper = _curMapper;
                IPortableMetadataHandler oldMetaHnd = _curMetaHnd;
                bool oldRaw = _curRaw;
                long oldRawPos = _curRawPos;

                // Push new frame.
                _curTypeId = desc.TypeId;
                _curConverter = desc.NameConverter;
                _curMapper = desc.Mapper;
                _curMetaHnd = desc.MetadataEnabled ? _marsh.MetadataHandler(desc) : null;
                _curRaw = false;
                _curRawPos = 0;

                // Write object fields.
                desc.Serializer.WritePortable(obj, this);

                // Calculate and write length.
                int retPos = _stream.Position;

                _stream.Seek(pos + 10, SeekOrigin.Begin);

                int len = retPos - pos;

                _stream.WriteInt(len);

                if (_curRawPos != 0)
                    // When set, it is difference between object head and raw position.
                    _stream.WriteInt((int)(_curRawPos - pos));
                else
                    // When no set, it is equal to object length.
                    _stream.WriteInt(len);

                _stream.Seek(retPos, SeekOrigin.Begin);

                // 13. Collect metadata.
                if (_curMetaHnd != null)
                {
                    IDictionary<string, int> meta = _curMetaHnd.OnObjectWriteFinished();

                    if (meta != null)
                        SaveMetadata(_curTypeId, desc.TypeName, desc.AffinityKeyFieldName, meta);
                }

                // Restore old frame.
                _curTypeId = oldTypeId;
                _curConverter = oldConverter;
                _curMapper = oldMapper;
                _curMetaHnd = oldMetaHnd;
                _curRaw = oldRaw;
                _curRawPos = oldRawPos;
            }
            finally
            {
                // Restore handles if needed.
                if (resetDetach)
                {
                    // Add newly recorded handles without overriding already existing ones.
                    if (_hnds != null)
                    {
                        if (oldHnds == null)
                            oldHnds = _hnds;
                        else
                            oldHnds.Merge(_hnds);
                    }

                    _hnds = oldHnds;

                    _detachMode = false;
                }
            }
        }

        /// <summary>
        /// Add handle to handles map.
        /// </summary>
        /// <param name="pos">Position in stream.</param>
        /// <param name="obj">Object.</param>
        /// <returns><c>true</c> if object was written as handle.</returns>
        private bool WriteHandle(long pos, object obj)
        {
            if (_hnds == null)
            {
                // Cache absolute handle position.
                _hnds = new PortableHandleDictionary<object, long>(obj, pos);

                return false;
            }

            long hndPos;

            if (!_hnds.TryGetValue(obj, out hndPos))
            {
                // Cache absolute handle position.
                _hnds.Add(obj, pos);

                return false;
            }

            _stream.WriteByte(PortableUtils.HdrHnd);

            // Handle is written as difference between position before header and handle position.
            _stream.WriteInt((int)(pos - hndPos));

            return true;
        }

        /// <summary>
        /// Try invoking predefined handler on object.
        /// </summary>
        /// <param name="typedHandler">Handler</param>
        /// <param name="untypedHandler">Not typed handler.</param>
        /// <param name="obj">Object.</param>
        /// <returns>True if handler was called.</returns>
        private bool InvokeHandler<T>(object typedHandler, PortableSystemWriteDelegate untypedHandler, T obj)
        {
            var typedHandler0 = typedHandler as PortableSystemTypedWriteDelegate<T>;

            if (typedHandler0 != null)
            {
                typedHandler0.Invoke(_stream, obj);

                return true;
            }

            if (untypedHandler != null)
            {
                untypedHandler.Invoke(this, obj);

                return true;
            }

            return false;
        }

        /// <summary>
        /// Write simple field with known length.
        /// </summary>
        /// <param name="fieldId">Field ID.</param>
        /// <param name="val">Value.</param>
        /// <param name="handler">Handler.</param>
        /// <param name="len">Length.</param>
        private void WriteSimpleField<T>(int fieldId, T val, PortableSystemTypedWriteDelegate<T> handler, int len)
        {
            CheckNotRaw();

            _stream.WriteInt(fieldId);
            _stream.WriteInt(1 + len); // Additional byte for field type.

            handler(_stream, val);
        }

        /// <summary>
        /// Write simple nullable field with unknown length.
        /// </summary>
        /// <param name="fieldId">Field ID.</param>
        /// <param name="val">Value.</param>
        /// <param name="handler">Handler.</param>
        private void WriteSimpleNullableField<T>(int fieldId, T val, PortableSystemTypedWriteDelegate<T> handler)
        {
            CheckNotRaw();

            _stream.WriteInt(fieldId);

            if (val == null)
            {
                _stream.WriteInt(1);

                _stream.WriteByte(PortableUtils.HdrNull);
            }
            else
            {
                int pos = _stream.Position;

                _stream.Seek(4, SeekOrigin.Current);

                handler(_stream, val);

                WriteFieldLength(_stream, pos);
            }
        }

        /// <summary>
        /// Write simple nullable field with known length.
        /// </summary>
        /// <param name="fieldId">Field ID.</param>
        /// <param name="val">Value.</param>
        /// <param name="handler">Handler.</param>
        /// <param name="len">Length.</param>
        private void WriteSimpleNullableField<T>(int fieldId, T val, PortableSystemTypedWriteDelegate<T> handler, int len)
        {
            CheckNotRaw();

            _stream.WriteInt(fieldId);

            if (val == null)
            {
                _stream.WriteInt(1);

                _stream.WriteByte(PortableUtils.HdrNull);
            }
            else
            {
                _stream.WriteInt(1 + len);

                handler(_stream, val);
            }
        }

        /// <summary>
        /// Write field.
        /// </summary>
        /// <param name="fieldId">Field ID.</param>
        /// <param name="val">Value.</param>
        /// <param name="handler">Handler.</param>
        private void WriteField(int fieldId, object val, PortableSystemWriteDelegate handler)
        {
            CheckNotRaw();

            _stream.WriteInt(fieldId);

            int pos = _stream.Position;

            _stream.Seek(4, SeekOrigin.Current);

            Write(val, handler);

            WriteFieldLength(_stream, pos);
        }

        /// <summary>
        /// Enable detach mode for the next object.
        /// </summary>
        internal void DetachNext()
        {
            if (!_detachMode)
                _detach = true;
        }

        /// <summary>
        /// Stream.
        /// </summary>
        internal IPortableStream Stream
        {
            get { return _stream; }
        }

        /// <summary>
        /// Gets collected metadatas.
        /// </summary>
        /// <returns>Collected metadatas (if any).</returns>
        internal IDictionary<int, IPortableMetadata> Metadata()
        {
            return _metas;
        }

        /// <summary>
        /// Check whether the given object is portable, i.e. it can be 
        /// serialized with portable marshaller.
        /// </summary>
        /// <param name="obj">Object.</param>
        /// <returns>True if portable.</returns>
        internal bool IsPortable(object obj)
        {
            if (obj != null)
            {
                Type type = obj.GetType();

                // We assume object as portable only in case it has descriptor.
                // Collections, Enums and non-primitive arrays do not have descriptors
                // and this is fine here because we cannot know whether their members
                // are portable.
                return _marsh.Descriptor(type) != null;
            }

            return true;
        }

        /// <summary>
        /// Write simple field with known length.
        /// </summary>
        /// <param name="fieldName">Field name.</param>
        /// <param name="typeId">Type ID.</param>
        /// <param name="val">Value.</param>
        /// <param name="handler">Handler.</param>
        /// <param name="len">Length.</param>
        private void WriteSimpleField<T>(string fieldName, byte typeId, T val,
            PortableSystemTypedWriteDelegate<T> handler, int len)
        {
            int fieldId = PortableUtils.FieldId(_curTypeId, fieldName, _curConverter, _curMapper);

            WriteSimpleField(fieldId, val, handler, len);

            if (_curMetaHnd != null)
                _curMetaHnd.OnFieldWrite(fieldId, fieldName, typeId);
        }

        /// <summary>
        /// Write simple nullable field with unknown length.
        /// </summary>
        /// <param name="fieldName">Field name.</param>
        /// <param name="typeId">Type ID.</param>
        /// <param name="val">Value.</param>
        /// <param name="handler">Handler.</param>
        private void WriteSimpleNullableField<T>(string fieldName, byte typeId, T val,
            PortableSystemTypedWriteDelegate<T> handler)
        {
            int fieldId = PortableUtils.FieldId(_curTypeId, fieldName, _curConverter, _curMapper);

            WriteSimpleNullableField(fieldId, val, handler);

            if (_curMetaHnd != null)
                _curMetaHnd.OnFieldWrite(fieldId, fieldName, typeId);
        }

        /// <summary>
        /// Write simple nullable field with known length.
        /// </summary>
        /// <param name="fieldName">Field name.</param>
        /// <param name="typeId">Type ID.</param>
        /// <param name="val">Value.</param>
        /// <param name="handler">Handler.</param>
        /// <param name="len">Length.</param>
        private void WriteSimpleNullableField<T>(string fieldName, byte typeId, T val,
            PortableSystemTypedWriteDelegate<T> handler, int len)
        {
            int fieldId = PortableUtils.FieldId(_curTypeId, fieldName, _curConverter, _curMapper);

            WriteSimpleNullableField(fieldId, val, handler, len);

            if (_curMetaHnd != null)
                _curMetaHnd.OnFieldWrite(fieldId, fieldName, typeId);
        }

        /// <summary>
        /// Write nullable raw field.
        /// </summary>
        /// <param name="val">Value.</param>
        /// <param name="handler">Handler.</param>
        private void WriteSimpleNullableRawField<T>(T val, PortableSystemTypedWriteDelegate<T> handler)
        {
            if (val == null)
                _stream.WriteByte(PortableUtils.HdrNull);
            else
                handler(_stream, val);
        }

        /// <summary>
        /// Write field.
        /// </summary>
        /// <param name="fieldName">Field name.</param>
        /// <param name="typeId">Type ID.</param>
        /// <param name="val">Value.</param>
        /// <param name="handler">Handler.</param>
        private void WriteField(string fieldName, byte typeId, object val,
            PortableSystemWriteDelegate handler)
        {
            int fieldId = PortableUtils.FieldId(_curTypeId, fieldName, _curConverter, _curMapper);

            WriteField(fieldId, val, handler);

            if (_curMetaHnd != null)
                _curMetaHnd.OnFieldWrite(fieldId, fieldName, typeId);
        }

        /// <summary>
        /// Write field length.
        /// </summary>
        /// <param name="stream">Stream.</param>
        /// <param name="pos">Position where length should reside</param>
        private static void WriteFieldLength(IPortableStream stream, int pos)
        {
            int retPos = stream.Position;

            stream.Seek(pos, SeekOrigin.Begin);

            stream.WriteInt(retPos - pos - 4);

            stream.Seek(retPos, SeekOrigin.Begin);
        }

        /// <summary>
        /// Ensure that we are not in raw mode.
        /// </summary>
        private void CheckNotRaw()
        {
            if (_curRaw)
                throw new PortableException("Cannot write named fields after raw data is written.");
        }

        /// <summary>
        /// Saves metadata for this session.
        /// </summary>
        /// <param name="typeId">Type ID.</param>
        /// <param name="typeName">Type name.</param>
        /// <param name="affKeyFieldName">Affinity key field name.</param>
        /// <param name="fields">Fields metadata.</param>
        internal void SaveMetadata(int typeId, string typeName, string affKeyFieldName, IDictionary<string, int> fields)
        {
            if (_metas == null)
            {
                PortableMetadataImpl meta =
                    new PortableMetadataImpl(typeId, typeName, fields, affKeyFieldName);

                _metas = new Dictionary<int, IPortableMetadata>(1);

                _metas[typeId] = meta;
            }
            else
            {
                IPortableMetadata meta;

                if (_metas.TryGetValue(typeId, out meta))
                {
                    IDictionary<string, int> existingFields = ((PortableMetadataImpl)meta).FieldsMap();

                    foreach (KeyValuePair<string, int> field in fields)
                    {
                        if (!existingFields.ContainsKey(field.Key))
                            existingFields[field.Key] = field.Value;
                    }
                }
                else
                    _metas[typeId] = new PortableMetadataImpl(typeId, typeName, fields, affKeyFieldName);
            }
        }
    }
}
