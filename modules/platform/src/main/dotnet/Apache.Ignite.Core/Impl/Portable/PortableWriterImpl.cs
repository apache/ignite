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
    public class PortableWriterImpl : IPortableWriterEx
    {
        /** Marshaller. */
        private readonly PortableMarshaller marsh;

        /** Stream. */
        private readonly IPortableStream stream;

        /** Builder (used only during build). */
        private PortableBuilderImpl builder;

        /** Handles. */
        private PortableHandleDictionary<object, long> hnds;

        /** Metadatas collected during this write session. */
        private IDictionary<int, IPortableMetadata> metas;

        /** Current type ID. */
        private int curTypeId;

        /** Current name converter */
        private IPortableNameMapper curConverter;

        /** Current mapper. */
        private IPortableIdMapper curMapper;

        /** Current metadata handler. */
        private IPortableMetadataHandler curMetaHnd;

        /** Current raw flag. */
        private bool curRaw;

        /** Current raw position. */
        private long curRawPos;

        /** Ignore handles flag. */
        private bool detach;

        /** Object started ignore mode. */
        private bool detachMode;

        /// <summary>
        /// Gets the marshaller.
        /// </summary>
        PortableMarshaller IPortableWriterEx.Marshaller
        {
            get { return marsh; }
        }

        /// <summary>
        /// Write named boolean value.
        /// </summary>
        /// <param name="fieldName">GetField name.</param>
        /// <param name="val">Boolean value.</param>
        public void WriteBoolean(string fieldName, bool val)
        {
            WriteSimpleField(fieldName, PortableUtils.TypeBool, val, PortableSystemHandlers.WRITE_HND_BOOL_TYPED, 1);
        }
        
        /// <summary>
        /// Write boolean value.
        /// </summary>
        /// <param name="val">Boolean value.</param>
        public void WriteBoolean(bool val)
        {
            stream.WriteBool(val);
        }

        /// <summary>
        /// Write named boolean array.
        /// </summary>
        /// <param name="fieldName">GetField name.</param>
        /// <param name="val">Boolean array.</param>
        public void WriteBooleanArray(string fieldName, bool[] val)
        {
            WriteSimpleNullableField(fieldName, PortableUtils.TypeArrayBool, val,
                PortableSystemHandlers.WRITE_HND_BOOL_ARRAY_TYPED, val != null ? val.Length + 4 : 0);
        }

        /// <summary>
        /// Write boolean array.
        /// </summary>
        /// <param name="val">Boolean array.</param>
        public void WriteBooleanArray(bool[] val)
        {
            WriteSimpleNullableRawField(val, PortableSystemHandlers.WRITE_HND_BOOL_ARRAY_TYPED);
        }

        /// <summary>
        /// Write named byte value.
        /// </summary>
        /// <param name="fieldName">GetField name.</param>
        /// <param name="val">Byte value.</param>
        public void WriteByte(string fieldName, byte val)
        {
            WriteSimpleField(fieldName, PortableUtils.TypeByte, val, PortableSystemHandlers.WRITE_HND_BYTE_TYPED, 1);
        }

        /// <summary>
        /// Write byte value.
        /// </summary>
        /// <param name="val">Byte value.</param>
        public void WriteByte(byte val)
        {
            stream.WriteByte(val);
        }

        /// <summary>
        /// Write named byte array.
        /// </summary>
        /// <param name="fieldName">GetField name.</param>
        /// <param name="val">Byte array.</param>
        public void WriteByteArray(string fieldName, byte[] val)
        {
            WriteSimpleNullableField(fieldName, PortableUtils.TypeArrayByte, val,
                PortableSystemHandlers.WRITE_HND_BYTE_ARRAY_TYPED, val != null ? val.Length + 4 : 0);
        }

        /// <summary>
        /// Write byte array.
        /// </summary>
        /// <param name="val">Byte array.</param>
        public void WriteByteArray(byte[] val)
        {
            WriteSimpleNullableRawField(val, PortableSystemHandlers.WRITE_HND_BYTE_ARRAY_TYPED);
        }

        /// <summary>
        /// Write named short value.
        /// </summary>
        /// <param name="fieldName">GetField name.</param>
        /// <param name="val">Short value.</param>
        public void WriteShort(string fieldName, short val)
        {
            WriteSimpleField(fieldName, PortableUtils.TypeShort, val, PortableSystemHandlers.WRITE_HND_SHORT_TYPED, 2);
        }

        /// <summary>
        /// Write short value.
        /// </summary>
        /// <param name="val">Short value.</param>
        public void WriteShort(short val)
        {
            stream.WriteShort(val);
        }

        /// <summary>
        /// Write named short array.
        /// </summary>
        /// <param name="fieldName">GetField name.</param>
        /// <param name="val">Short array.</param>
        public void WriteShortArray(string fieldName, short[] val)
        {
            WriteSimpleNullableField(fieldName, PortableUtils.TypeArrayShort, val,
                PortableSystemHandlers.WRITE_HND_SHORT_ARRAY_TYPED, val != null ? 2 * val.Length + 4 : 0);
        }

        /// <summary>
        /// Write short array.
        /// </summary>
        /// <param name="val">Short array.</param>
        public void WriteShortArray(short[] val)
        {
            WriteSimpleNullableRawField(val, PortableSystemHandlers.WRITE_HND_SHORT_ARRAY_TYPED);
        }

        /// <summary>
        /// Write named char value.
        /// </summary>
        /// <param name="fieldName">GetField name.</param>
        /// <param name="val">Char value.</param>
        public void WriteChar(string fieldName, char val)
        {
            WriteSimpleField(fieldName, PortableUtils.TypeChar, val, PortableSystemHandlers.WRITE_HND_CHAR_TYPED, 2);
        }

        /// <summary>
        /// Write char value.
        /// </summary>
        /// <param name="val">Char value.</param>
        public void WriteChar(char val)
        {
            stream.WriteChar(val);
        }

        /// <summary>
        /// Write named char array.
        /// </summary>
        /// <param name="fieldName">GetField name.</param>
        /// <param name="val">Char array.</param>
        public void WriteCharArray(string fieldName, char[] val)
        {
            WriteSimpleNullableField(fieldName, PortableUtils.TypeArrayChar, val,
                PortableSystemHandlers.WRITE_HND_CHAR_ARRAY_TYPED, val != null ? 2 * val.Length + 4 : 0);
        }

        /// <summary>
        /// Write char array.
        /// </summary>
        /// <param name="val">Char array.</param>
        public void WriteCharArray(char[] val)
        {
            WriteSimpleNullableRawField(val, PortableSystemHandlers.WRITE_HND_CHAR_ARRAY_TYPED);
        }

        /// <summary>
        /// Write named int value.
        /// </summary>
        /// <param name="fieldName">GetField name.</param>
        /// <param name="val">Int value.</param>
        public void WriteInt(string fieldName, int val)
        {
            WriteSimpleField(fieldName, PortableUtils.TypeInt, val, PortableSystemHandlers.WRITE_HND_INT_TYPED, 4);
        }

        /// <summary>
        /// Write int value.
        /// </summary>
        /// <param name="val">Int value.</param>
        public void WriteInt(int val)
        {
            stream.WriteInt(val);
        }

        /// <summary>
        /// Write named int array.
        /// </summary>
        /// <param name="fieldName">GetField name.</param>
        /// <param name="val">Int array.</param>
        public void WriteIntArray(string fieldName, int[] val)
        {
            WriteSimpleNullableField(fieldName, PortableUtils.TypeArrayInt, val,
                PortableSystemHandlers.WRITE_HND_INT_ARRAY_TYPED, val != null ? 4 * val.Length + 4 : 0);
        }

        /// <summary>
        /// Write int array.
        /// </summary>
        /// <param name="val">Int array.</param>
        public void WriteIntArray(int[] val)
        {
            WriteSimpleNullableRawField(val, PortableSystemHandlers.WRITE_HND_INT_ARRAY_TYPED);
        }

        /// <summary>
        /// Write named long value.
        /// </summary>
        /// <param name="fieldName">GetField name.</param>
        /// <param name="val">Long value.</param>
        public void WriteLong(string fieldName, long val)
        {
            WriteSimpleField(fieldName, PortableUtils.TypeLong, val, PortableSystemHandlers.WRITE_HND_LONG_TYPED, 8);
        }

        /// <summary>
        /// Write long value.
        /// </summary>
        /// <param name="val">Long value.</param>
        public void WriteLong(long val)
        {
            stream.WriteLong(val);
        }

        /// <summary>
        /// Write named long array.
        /// </summary>
        /// <param name="fieldName">GetField name.</param>
        /// <param name="val">Long array.</param>
        public void WriteLongArray(string fieldName, long[] val)
        {
            WriteSimpleNullableField(fieldName, PortableUtils.TypeArrayLong, val,
                PortableSystemHandlers.WRITE_HND_LONG_ARRAY_TYPED, val != null ? 8 * val.Length + 4 : 0);
        }

        /// <summary>
        /// Write long array.
        /// </summary>
        /// <param name="val">Long array.</param>
        public void WriteLongArray(long[] val)
        {
            WriteSimpleNullableRawField(val, PortableSystemHandlers.WRITE_HND_LONG_ARRAY_TYPED);
        }

        /// <summary>
        /// Write named float value.
        /// </summary>
        /// <param name="fieldName">GetField name.</param>
        /// <param name="val">Float value.</param>
        public void WriteFloat(string fieldName, float val)
        {
            WriteSimpleField(fieldName, PortableUtils.TypeFloat, val, PortableSystemHandlers.WRITE_HND_FLOAT_TYPED, 4);
        }

        /// <summary>
        /// Write float value.
        /// </summary>
        /// <param name="val">Float value.</param>
        public void WriteFloat(float val)
        {
            stream.WriteFloat(val);
        }

        /// <summary>
        /// Write named float array.
        /// </summary>
        /// <param name="fieldName">GetField name.</param>
        /// <param name="val">Float array.</param>
        public void WriteFloatArray(string fieldName, float[] val)
        {
            WriteSimpleNullableField(fieldName, PortableUtils.TypeArrayFloat, val,
                PortableSystemHandlers.WRITE_HND_FLOAT_ARRAY_TYPED, val != null ? 4 * val.Length + 4 : 0);
        }

        /// <summary>
        /// Write float array.
        /// </summary>
        /// <param name="val">Float array.</param>
        public void WriteFloatArray(float[] val)
        {
            WriteSimpleNullableRawField(val, PortableSystemHandlers.WRITE_HND_FLOAT_ARRAY_TYPED);
        }

        /// <summary>
        /// Write named double value.
        /// </summary>
        /// <param name="fieldName">GetField name.</param>
        /// <param name="val">Double value.</param>
        public void WriteDouble(string fieldName, double val)
        {
            WriteSimpleField(fieldName, PortableUtils.TypeDouble, val, PortableSystemHandlers.WRITE_HND_DOUBLE_TYPED, 8);
        }

        /// <summary>
        /// Write double value.
        /// </summary>
        /// <param name="val">Double value.</param>
        public void WriteDouble(double val)
        {
            stream.WriteDouble(val);
        }

        /// <summary>
        /// Write named double array.
        /// </summary>
        /// <param name="fieldName">GetField name.</param>
        /// <param name="val">Double array.</param>
        public void WriteDoubleArray(string fieldName, double[] val)
        {
            WriteSimpleNullableField(fieldName, PortableUtils.TypeArrayDouble, val,
                PortableSystemHandlers.WRITE_HND_DOUBLE_ARRAY_TYPED, val != null ? 8 * val.Length + 4 : 0);
        }

        /// <summary>
        /// Write double array.
        /// </summary>
        /// <param name="val">Double array.</param>
        public void WriteDoubleArray(double[] val)
        {
            WriteSimpleNullableRawField(val, PortableSystemHandlers.WRITE_HND_DOUBLE_ARRAY_TYPED);
        }

        /// <summary>
        /// Write named decimal value.
        /// </summary>
        /// <param name="fieldName">GetField name.</param>
        /// <param name="val">Decimal value.</param>
        public void WriteDecimal(string fieldName, decimal val)
        {
            WriteSimpleNullableField(fieldName, PortableUtils.TypeDecimal, val, PortableSystemHandlers.WRITE_HND_DECIMAL_TYPED);
        }

        /// <summary>
        /// Write decimal value.
        /// </summary>
        /// <param name="val">Decimal value.</param>
        public void WriteDecimal(decimal val)
        {
            WriteSimpleNullableRawField(val, PortableSystemHandlers.WRITE_HND_DECIMAL_TYPED);
        }

        /// <summary>
        /// Write named decimal array.
        /// </summary>
        /// <param name="fieldName">GetField name.</param>
        /// <param name="val">Decimal array.</param>
        public void WriteDecimalArray(string fieldName, decimal[] val)
        {
            WriteSimpleNullableField(fieldName, PortableUtils.TypeArrayDecimal, val,
                PortableSystemHandlers.WRITE_HND_DECIMAL_ARRAY_TYPED);
        }
        
        /// <summary>
        /// Write decimal array.
        /// </summary>
        /// <param name="val">Decimal array.</param>
        public void WriteDecimalArray(decimal[] val)
        {
            WriteSimpleNullableRawField(val, PortableSystemHandlers.WRITE_HND_DECIMAL_ARRAY_TYPED);
        }

        /// <summary>
        /// Write named date value.
        /// </summary>
        /// <param name="fieldName">GetField name.</param>
        /// <param name="val">Date value.</param>
        public void WriteDate(string fieldName, DateTime? val)
        {
            WriteSimpleNullableField(fieldName, PortableUtils.TypeDate, val, PortableSystemHandlers.WRITE_HND_DATE_TYPED,
                val.HasValue ? 12 : 0);
        }
        
        /// <summary>
        /// Write date value.
        /// </summary>
        /// <param name="val">Date value.</param>
        public void WriteDate(DateTime? val)
        {
            WriteSimpleNullableRawField(val, PortableSystemHandlers.WRITE_HND_DATE_TYPED);
        }

        /// <summary>
        /// Write named date array.
        /// </summary>
        /// <param name="fieldName">GetField name.</param>
        /// <param name="val">Date array.</param>
        public void WriteDateArray(string fieldName, DateTime?[] val)
        {
            WriteSimpleNullableField(fieldName, PortableUtils.TypeArrayDate, val,
                PortableSystemHandlers.WRITE_HND_DATE_ARRAY_TYPED);
        }

        /// <summary>
        /// Write date array.
        /// </summary>
        /// <param name="val">Date array.</param>
        public void WriteDateArray(DateTime?[] val)
        {
            WriteSimpleNullableRawField(val, PortableSystemHandlers.WRITE_HND_DATE_ARRAY_TYPED);
        }

        /// <summary>
        /// Write named string value.
        /// </summary>
        /// <param name="fieldName">GetField name.</param>
        /// <param name="val">String value.</param>
        public void WriteString(string fieldName, string val)
        {
            WriteSimpleNullableField(fieldName, PortableUtils.TypeString, val, PortableSystemHandlers.WRITE_HND_STRING_TYPED);
        }

        /// <summary>
        /// Write string value.
        /// </summary>
        /// <param name="val">String value.</param>
        public void WriteString(string val)
        {
            WriteSimpleNullableRawField(val, PortableSystemHandlers.WRITE_HND_STRING_TYPED);
        }

        /// <summary>
        /// Write named string array.
        /// </summary>
        /// <param name="fieldName">GetField name.</param>
        /// <param name="val">String array.</param>
        public void WriteStringArray(string fieldName, string[] val)
        {
            WriteSimpleNullableField(fieldName, PortableUtils.TypeArrayString, val,
                PortableSystemHandlers.WRITE_HND_STRING_ARRAY_TYPED);
        }

        /// <summary>
        /// Write string array.
        /// </summary>
        /// <param name="val">String array.</param>
        public void WriteStringArray(string[] val)
        {
            WriteSimpleNullableRawField(val, PortableSystemHandlers.WRITE_HND_STRING_ARRAY_TYPED);
        }

        /// <summary>
        /// Write named GUID value.
        /// </summary>
        /// <param name="fieldName">GetField name.</param>
        /// <param name="val">GUID value.</param>
        public void WriteGuid(string fieldName, Guid? val)
        {
            WriteSimpleNullableField(fieldName, PortableUtils.TypeGuid, val, PortableSystemHandlers.WRITE_HND_GUID_TYPED,
                val.HasValue ? 16 : 0);
        }

        /// <summary>
        /// Write GUID value.
        /// </summary>
        /// <param name="val">GUID value.</param>
        public void WriteGuid(Guid? val)
        {
            WriteSimpleNullableRawField(val, PortableSystemHandlers.WRITE_HND_GUID_TYPED);
        }

        /// <summary>
        /// Write named GUID array.
        /// </summary>
        /// <param name="fieldName">GetField name.</param>
        /// <param name="val">GUID array.</param>
        public void WriteGuidArray(string fieldName, Guid?[] val)
        {
            WriteSimpleNullableField(fieldName, PortableUtils.TypeArrayGuid, val,
                PortableSystemHandlers.WRITE_HND_GUID_ARRAY_TYPED);
        }

        /// <summary>
        /// Write GUID array.
        /// </summary>
        /// <param name="val">GUID array.</param>
        public void WriteGuidArray(Guid?[] val)
        {
            WriteSimpleNullableRawField(val, PortableSystemHandlers.WRITE_HND_GUID_ARRAY_TYPED);
        }

        /// <summary>
        /// Write named enum value.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="fieldName">GetField name.</param>
        /// <param name="val">Enum value.</param>
        public void WriteEnum<T>(string fieldName, T val)
        {
            WriteField(fieldName, PortableUtils.TypeEnum, val, PortableSystemHandlers.WRITE_HND_ENUM);
        }

        /// <summary>
        /// Write enum value.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="val">Enum value.</param>
        public void WriteEnum<T>(T val)
        {
            Write(val, PortableSystemHandlers.WRITE_HND_ENUM);
        }

        /// <summary>
        /// Write named enum array.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="fieldName">GetField name.</param>
        /// <param name="val">Enum array.</param>
        public void WriteEnumArray<T>(string fieldName, T[] val)
        {
            WriteField(fieldName, PortableUtils.TypeArrayEnum, val, PortableSystemHandlers.WRITE_HND_ENUM_ARRAY);
        }

        /// <summary>
        /// Write enum array.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="val">Enum array.</param>
        public void WriteEnumArray<T>(T[] val)
        {
            Write(val, PortableSystemHandlers.WRITE_HND_ENUM_ARRAY);
        }

        /// <summary>
        /// Write named object value.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="fieldName">GetField name.</param>
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
        /// <param name="fieldName">GetField name.</param>
        /// <param name="val">Object array.</param>
        public void WriteObjectArray<T>(string fieldName, T[] val)
        {
            WriteField(fieldName, PortableUtils.TypeArray, val, PortableSystemHandlers.WRITE_HND_ARRAY);
        }

        /// <summary>
        /// Write object array.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="val">Object array.</param>
        public void WriteObjectArray<T>(T[] val)
        {
            Write(val, PortableSystemHandlers.WRITE_HND_ARRAY);
        }

        /// <summary>
        /// Write named collection.
        /// </summary>
        /// <param name="fieldName">GetField name.</param>
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
        /// <param name="fieldName">GetField name.</param>
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
        /// <param name="fieldName">GetField name.</param>
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
        /// <typeparam name="K"></typeparam>
        /// <typeparam name="V"></typeparam>
        /// <param name="fieldName">GetField name.</param>
        /// <param name="val">Dictionary.</param>
        public void WriteGenericDictionary<K, V>(string fieldName, IDictionary<K, V> val)
        {
            WriteField(fieldName, PortableUtils.TypeDictionary, val, null);
        }

        /// <summary>
        /// Write generic dictionary.
        /// </summary>
        /// <typeparam name="K"></typeparam>
        /// <typeparam name="V"></typeparam>
        /// <param name="val">Dictionary.</param>
        public void WriteGenericDictionary<K, V>(IDictionary<K, V> val)
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
            if (!curRaw)
            {
                curRaw = true;
                curRawPos = stream.Position;
            }

            return this;
        }

        /// <summary>
        /// Sets new builder.
        /// </summary>
        /// <param name="portableBuilder">Builder.</param>
        /// <returns>Previous builder.</returns>
        PortableBuilderImpl IPortableWriterEx.SetBuilder(PortableBuilderImpl portableBuilder)
        {
            var ret = builder;

            builder = portableBuilder;

            return ret;
        }

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="marsh">Marshaller.</param>
        /// <param name="stream">Stream.</param>
        internal PortableWriterImpl(PortableMarshaller marsh, IPortableStream stream)
        {
            this.marsh = marsh;
            this.stream = stream;
        }

        /// <summary>
        /// Write object.
        /// </summary>
        /// <param name="obj">Object.</param>
        void IPortableWriterEx.Write<T>(T obj)
        {
            Write(obj, null);
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

            if (detach)
            {
                detach = false;
                detachMode = true;
                resetDetach = true;

                oldHnds = hnds;

                hnds = null;
            }

            try
            {
                // Write null.
                if (obj == null)
                {
                    stream.WriteByte(PortableUtils.HdrNull);

                    return;
                }

                if (builder != null)
                {
                    // Special case for portable object during build.
                    PortableUserObject portObj = obj as PortableUserObject;

                    if (portObj != null)
                    {
                        if (!WriteHandle(stream.Position, portObj))
                            builder.ProcessPortable(stream, portObj);

                        return;
                    }

                    // Special case for builder during build.
                    PortableBuilderImpl portBuilder = obj as PortableBuilderImpl;

                    if (portBuilder != null)
                    {
                        if (!WriteHandle(stream.Position, portBuilder))
                            builder.ProcessBuilder(stream, portBuilder);

                        return;
                    }
                }                

                // Try writting as well-known type.
                if (InvokeHandler(handler, handler as PortableSystemWriteDelegate, obj))
                    return;

                Type type = obj.GetType();

                IPortableTypeDescriptor desc = marsh.Descriptor(type);

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

                int pos = stream.Position;

                // Dealing with handles.
                if (!(desc.Serializer is IPortableSystemTypeSerializer) && WriteHandle(pos, obj))
                    return;

                // Write header.
                stream.WriteByte(PortableUtils.HdrFull);

                stream.WriteBool(desc.UserType);
                stream.WriteInt(desc.TypeId);
                stream.WriteInt(obj.GetHashCode());

                // Skip length as it is not known in the first place.
                stream.Seek(8, SeekOrigin.Current);

                // Preserve old frame.
                int oldTypeId = curTypeId;
                IPortableNameMapper oldConverter = curConverter;
                IPortableIdMapper oldMapper = curMapper;
                IPortableMetadataHandler oldMetaHnd = curMetaHnd;
                bool oldRaw = curRaw;
                long oldRawPos = curRawPos;

                // Push new frame.
                curTypeId = desc.TypeId;
                curConverter = desc.NameConverter;
                curMapper = desc.Mapper;
                curMetaHnd = desc.MetadataEnabled ? marsh.MetadataHandler(desc) : null;
                curRaw = false;
                curRawPos = 0;

                // Write object fields.
                desc.Serializer.WritePortable(obj, this);

                // Calculate and write length.
                int retPos = stream.Position;

                stream.Seek(pos + 10, SeekOrigin.Begin);

                int len = retPos - pos;

                stream.WriteInt(len);

                if (curRawPos != 0)
                    // When set, it is difference between object head and raw position.
                    stream.WriteInt((int)(curRawPos - pos));
                else
                    // When no set, it is equal to object length.
                    stream.WriteInt(len);

                stream.Seek(retPos, SeekOrigin.Begin);

                // 13. Collect metadata.
                if (curMetaHnd != null)
                {
                    IDictionary<string, int> meta = curMetaHnd.OnObjectWriteFinished();

                    if (meta != null)
                        SaveMetadata(curTypeId, desc.TypeName, desc.AffinityKeyFieldName, meta);
                }

                // Restore old frame.
                curTypeId = oldTypeId;
                curConverter = oldConverter;
                curMapper = oldMapper;
                curMetaHnd = oldMetaHnd;
                curRaw = oldRaw;
                curRawPos = oldRawPos;
            }
            finally
            {
                // Restore handles if needed.
                if (resetDetach)
                {
                    // Add newly recorded handles without overriding already existing ones.
                    if (hnds != null)
                    {
                        if (oldHnds == null)
                            oldHnds = hnds;
                        else
                            oldHnds.Merge(hnds);
                    }

                    hnds = oldHnds;

                    detachMode = false;
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
            if (hnds == null)
            {
                // Cache absolute handle position.
                hnds = new PortableHandleDictionary<object, long>(obj, pos);

                return false;
            }

            long hndPos;

            if (!hnds.TryGetValue(obj, out hndPos))
            {
                // Cache absolute handle position.
                hnds.Add(obj, pos);

                return false;
            }

            stream.WriteByte(PortableUtils.HdrHnd);

            // Handle is written as difference between position before header and handle position.
            stream.WriteInt((int)(pos - hndPos));

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
            if (typedHandler != null)
            {
                PortableSystemTypedWriteDelegate<T> typedHandler0 =
                    typedHandler as PortableSystemTypedWriteDelegate<T>;

                if (typedHandler0 != null)
                {
                    typedHandler0.Invoke(stream, obj);

                    return true;
                }
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
        /// <param name="fieldId">GetField ID.</param>
        /// <param name="typeId">Type ID.</param>
        /// <param name="val">Value.</param>
        /// <param name="handler">Handler.</param>
        /// <param name="len">Length.</param>
        internal void WriteSimpleField<T>(int fieldId, byte typeId, T val,
            PortableSystemTypedWriteDelegate<T> handler, int len)
        {
            CheckNotRaw();

            stream.WriteInt(fieldId);
            stream.WriteInt(1 + len); // Additional byte for field type.

            handler(stream, val);
        }

        /// <summary>
        /// Write simple nullable field with unknown length.
        /// </summary>
        /// <param name="fieldId">GetField ID.</param>
        /// <param name="typeId">Type ID.</param>
        /// <param name="val">Value.</param>
        /// <param name="handler">Handler.</param>
        internal void WriteSimpleNullableField<T>(int fieldId, byte typeId, T val,
            PortableSystemTypedWriteDelegate<T> handler)
        {
            CheckNotRaw();

            stream.WriteInt(fieldId);

            if (val == null)
            {
                stream.WriteInt(1);

                stream.WriteByte(PortableUtils.HdrNull);
            }
            else
            {
                int pos = stream.Position;

                stream.Seek(4, SeekOrigin.Current);

                handler(stream, val);

                WriteFieldLength(stream, pos);
            }
        }

        /// <summary>
        /// Write simple nullable field with known length.
        /// </summary>
        /// <param name="fieldId">GetField ID.</param>
        /// <param name="typeId">Type ID.</param>
        /// <param name="val">Value.</param>
        /// <param name="handler">Handler.</param>
        /// <param name="len">Length.</param>
        internal void WriteSimpleNullableField<T>(int fieldId, byte typeId, T val,
            PortableSystemTypedWriteDelegate<T> handler, int len)
        {
            CheckNotRaw();

            stream.WriteInt(fieldId);

            if (val == null)
            {
                stream.WriteInt(1);

                stream.WriteByte(PortableUtils.HdrNull);
            }
            else
            {
                stream.WriteInt(1 + len);

                handler(stream, val);
            }
        }

        /// <summary>
        /// Write field.
        /// </summary>
        /// <param name="fieldId">GetField ID.</param>
        /// <param name="typeId">Type ID.</param>
        /// <param name="val">Value.</param>
        /// <param name="handler">Handler.</param>
        internal void WriteField(int fieldId, byte typeId, object val, PortableSystemWriteDelegate handler)
        {
            CheckNotRaw();

            stream.WriteInt(fieldId);

            int pos = stream.Position;

            stream.Seek(4, SeekOrigin.Current);

            Write(val, handler);

            WriteFieldLength(stream, pos);
        }

        /// <summary>
        /// Enable detach mode for the next object.
        /// </summary>
        void IPortableWriterEx.DetachNext()
        {
            if (!detachMode)
                detach = true;
        }

        /// <summary>
        /// Stream.
        /// </summary>
        IPortableStream IPortableWriterEx.Stream
        {
            get { return stream; }
        }

        /// <summary>
        /// Gets collected metadatas.
        /// </summary>
        /// <value>Collected metadatas (if any).</value>
        IDictionary<int, IPortableMetadata> IPortableWriterEx.Metadata
        {
            get { return metas; }
        }

        /// <summary>
        /// Write simple field with known length.
        /// </summary>
        /// <param name="fieldName">GetField name.</param>
        /// <param name="typeId">Type ID.</param>
        /// <param name="val">Value.</param>
        /// <param name="handler">Handler.</param>
        /// <param name="len">Length.</param>
        private void WriteSimpleField<T>(string fieldName, byte typeId, T val,
            PortableSystemTypedWriteDelegate<T> handler, int len)
        {
            int fieldId = PortableUtils.FieldId(curTypeId, fieldName, curConverter, curMapper);

            WriteSimpleField(fieldId, typeId, val, handler, len);

            if (curMetaHnd != null)
                curMetaHnd.OnFieldWrite(fieldId, fieldName, typeId);
        }

        /// <summary>
        /// Write simple nullable field with unknown length.
        /// </summary>
        /// <param name="fieldName">GetField name.</param>
        /// <param name="typeId">Type ID.</param>
        /// <param name="val">Value.</param>
        /// <param name="handler">Handler.</param>
        private void WriteSimpleNullableField<T>(string fieldName, byte typeId, T val,
            PortableSystemTypedWriteDelegate<T> handler)
        {
            int fieldId = PortableUtils.FieldId(curTypeId, fieldName, curConverter, curMapper);

            WriteSimpleNullableField(fieldId, typeId, val, handler);

            if (curMetaHnd != null)
                curMetaHnd.OnFieldWrite(fieldId, fieldName, typeId);
        }

        /// <summary>
        /// Write simple nullable field with known length.
        /// </summary>
        /// <param name="fieldName">GetField name.</param>
        /// <param name="typeId">Type ID.</param>
        /// <param name="val">Value.</param>
        /// <param name="handler">Handler.</param>
        /// <param name="len">Length.</param>
        private void WriteSimpleNullableField<T>(string fieldName, byte typeId, T val,
            PortableSystemTypedWriteDelegate<T> handler, int len)
        {
            int fieldId = PortableUtils.FieldId(curTypeId, fieldName, curConverter, curMapper);

            WriteSimpleNullableField(fieldId, typeId, val, handler, len);

            if (curMetaHnd != null)
                curMetaHnd.OnFieldWrite(fieldId, fieldName, typeId);
        }

        /// <summary>
        /// Write nullable raw field.
        /// </summary>
        /// <param name="val">Value.</param>
        /// <param name="handler">Handler.</param>
        private void WriteSimpleNullableRawField<T>(T val, PortableSystemTypedWriteDelegate<T> handler)
        {
            if (val == null)
                stream.WriteByte(PortableUtils.HdrNull);
            else
                handler(stream, val);
        }

        /// <summary>
        /// Write field.
        /// </summary>
        /// <param name="fieldName">GetField name.</param>
        /// <param name="typeId">Type ID.</param>
        /// <param name="val">Value.</param>
        /// <param name="handler">Handler.</param>
        private void WriteField(string fieldName, byte typeId, object val,
            PortableSystemWriteDelegate handler)
        {
            int fieldId = PortableUtils.FieldId(curTypeId, fieldName, curConverter, curMapper);

            WriteField(fieldId, typeId, val, handler);

            if (curMetaHnd != null)
                curMetaHnd.OnFieldWrite(fieldId, fieldName, typeId);
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
            if (curRaw)
                throw new PortableException("Cannot write named fields after raw data is written.");
        }

        /** <inheritdoc /> */
        void IPortableWriterEx.SaveMetadata(int typeId, string typeName, string affKeyFieldName,
            IDictionary<string, int> fields)
        {
            SaveMetadata(typeId, typeName, affKeyFieldName, fields);
        }

        /// <summary>
        /// Saves metadata for this session.
        /// </summary>
        /// <param name="typeId">Type ID.</param>
        /// <param name="typeName">Type name.</param>
        /// <param name="affKeyFieldName">Affinity key field name.</param>
        /// <param name="fields">Fields metadata.</param>
        private void SaveMetadata(int typeId, string typeName, string affKeyFieldName, IDictionary<string, int> fields)
        {
            if (metas == null)
            {
                PortableMetadataImpl meta =
                    new PortableMetadataImpl(typeId, typeName, fields, affKeyFieldName);

                metas = new Dictionary<int, IPortableMetadata>(1);

                metas[typeId] = meta;
            }
            else
            {
                IPortableMetadata meta;

                if (metas.TryGetValue(typeId, out meta))
                {
                    IDictionary<string, int> existingFields = ((PortableMetadataImpl)meta).FieldsMap();

                    foreach (KeyValuePair<string, int> field in fields)
                    {
                        if (!existingFields.ContainsKey(field.Key))
                            existingFields[field.Key] = field.Value;
                    }
                }
                else
                    metas[typeId] = new PortableMetadataImpl(typeId, typeName, fields, affKeyFieldName);
            }
        }
    }
}
