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
    using System.IO;

    using Apache.Ignite.Core.Impl.Portable.IO;
    using Apache.Ignite.Core.Impl.Portable.Metadata;
    using Apache.Ignite.Core.Impl.Portable.Structure;
    using Apache.Ignite.Core.Portable;

    using PU = PortableUtils;

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
        
        /** Current raw position. */
        private long _curRawPos;

        /** Current type structure. */
        private PortableStructure _curStruct;

        /** Current type structure path index. */
        private int _curStructPath;

        /** Current type structure action index. */
        private int _curStructAction;

        /** Current type structure updates. */
        private List<PortableStructureUpdate> _curStructUpdates; 
        
        /** Whether we are currently detaching an object. */
        private bool _detaching;

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
            WriteFieldId(fieldName, PU.TypeBool);

            _stream.WriteInt(PU.LengthTypeId + 1);
            _stream.WriteByte(PU.TypeBool);
            _stream.WriteBool(val);
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
            WriteFieldId(fieldName, PU.TypeArrayBool);

            if (val == null)
                WriteNullField();
            else
            {
                _stream.WriteInt(PU.LengthTypeId + PU.LengthArraySize + val.Length);
                _stream.WriteByte(PU.TypeArrayBool);
                PU.WriteBooleanArray(val, _stream);
            }
        }

        /// <summary>
        /// Write boolean array.
        /// </summary>
        /// <param name="val">Boolean array.</param>
        public void WriteBooleanArray(bool[] val)
        {
            if (val == null)
                WriteNullRawField();
            else
            {
                _stream.WriteByte(PU.TypeArrayBool);
                PU.WriteBooleanArray(val, _stream);
            }
        }

        /// <summary>
        /// Write named byte value.
        /// </summary>
        /// <param name="fieldName">Field name.</param>
        /// <param name="val">Byte value.</param>
        public void WriteByte(string fieldName, byte val)
        {
            WriteFieldId(fieldName, PU.TypeBool);

            _stream.WriteInt(PU.LengthTypeId + 1);
            _stream.WriteByte(PU.TypeByte);
            _stream.WriteByte(val);
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
            WriteFieldId(fieldName, PU.TypeArrayByte);

            if (val == null)
                WriteNullField();
            else
            {
                _stream.WriteInt(PU.LengthTypeId + PU.LengthArraySize + val.Length);
                _stream.WriteByte(PU.TypeArrayByte);
                PU.WriteByteArray(val, _stream);
            }
        }

        /// <summary>
        /// Write byte array.
        /// </summary>
        /// <param name="val">Byte array.</param>
        public void WriteByteArray(byte[] val)
        {
            if (val == null)
                WriteNullRawField();
            else
            {
                _stream.WriteByte(PU.TypeArrayByte);
                PU.WriteByteArray(val, _stream);
            }
        }

        /// <summary>
        /// Write named short value.
        /// </summary>
        /// <param name="fieldName">Field name.</param>
        /// <param name="val">Short value.</param>
        public void WriteShort(string fieldName, short val)
        {
            WriteFieldId(fieldName, PU.TypeShort);

            _stream.WriteInt(PU.LengthTypeId + 2);
            _stream.WriteByte(PU.TypeShort);
            _stream.WriteShort(val);
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
            WriteFieldId(fieldName, PU.TypeArrayShort);

            if (val == null)
                WriteNullField();
            else
            {
                _stream.WriteInt(PU.LengthTypeId + PU.LengthArraySize + (val.Length << 1));
                _stream.WriteByte(PU.TypeArrayShort);
                PU.WriteShortArray(val, _stream);
            }
        }

        /// <summary>
        /// Write short array.
        /// </summary>
        /// <param name="val">Short array.</param>
        public void WriteShortArray(short[] val)
        {
            if (val == null)
                WriteNullRawField();
            else
            {
                _stream.WriteByte(PU.TypeArrayShort);
                PU.WriteShortArray(val, _stream);
            }
        }

        /// <summary>
        /// Write named char value.
        /// </summary>
        /// <param name="fieldName">Field name.</param>
        /// <param name="val">Char value.</param>
        public void WriteChar(string fieldName, char val)
        {
            WriteFieldId(fieldName, PU.TypeChar);

            _stream.WriteInt(PU.LengthTypeId + 2);
            _stream.WriteByte(PU.TypeChar);
            _stream.WriteChar(val);
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
            WriteFieldId(fieldName, PU.TypeArrayChar);

            if (val == null)
                WriteNullField();
            else
            {
                _stream.WriteInt(PU.LengthTypeId + PU.LengthArraySize + (val.Length << 1));
                _stream.WriteByte(PU.TypeArrayChar);
                PU.WriteCharArray(val, _stream);
            }
        }

        /// <summary>
        /// Write char array.
        /// </summary>
        /// <param name="val">Char array.</param>
        public void WriteCharArray(char[] val)
        {
            if (val == null)
                WriteNullRawField();
            else
            {
                _stream.WriteByte(PU.TypeArrayChar);
                PU.WriteCharArray(val, _stream);
            }
        }

        /// <summary>
        /// Write named int value.
        /// </summary>
        /// <param name="fieldName">Field name.</param>
        /// <param name="val">Int value.</param>
        public void WriteInt(string fieldName, int val)
        {
            WriteFieldId(fieldName, PU.TypeInt);

            _stream.WriteInt(PU.LengthTypeId + 4);
            _stream.WriteByte(PU.TypeInt);
            _stream.WriteInt(val);
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
            WriteFieldId(fieldName, PU.TypeArrayInt);

            if (val == null)
                WriteNullField();
            else
            {
                _stream.WriteInt(PU.LengthTypeId + PU.LengthArraySize + (val.Length << 2));
                _stream.WriteByte(PU.TypeArrayInt);
                PU.WriteIntArray(val, _stream);
            }
        }

        /// <summary>
        /// Write int array.
        /// </summary>
        /// <param name="val">Int array.</param>
        public void WriteIntArray(int[] val)
        {
            if (val == null)
                WriteNullRawField();
            else
            {
                _stream.WriteByte(PU.TypeArrayInt);
                PU.WriteIntArray(val, _stream);
            }
        }

        /// <summary>
        /// Write named long value.
        /// </summary>
        /// <param name="fieldName">Field name.</param>
        /// <param name="val">Long value.</param>
        public void WriteLong(string fieldName, long val)
        {
            WriteFieldId(fieldName, PU.TypeLong);

            _stream.WriteInt(PU.LengthTypeId + 8);
            _stream.WriteByte(PU.TypeLong);
            _stream.WriteLong(val);
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
            WriteFieldId(fieldName, PU.TypeArrayLong);

            if (val == null)
                WriteNullField();
            else
            {
                _stream.WriteInt(PU.LengthTypeId + PU.LengthArraySize + (val.Length << 3));
                _stream.WriteByte(PU.TypeArrayLong);
                PU.WriteLongArray(val, _stream);
            }
        }

        /// <summary>
        /// Write long array.
        /// </summary>
        /// <param name="val">Long array.</param>
        public void WriteLongArray(long[] val)
        {
            if (val == null)
                WriteNullRawField();
            else
            {
                _stream.WriteByte(PU.TypeArrayLong);
                PU.WriteLongArray(val, _stream);
            }
        }

        /// <summary>
        /// Write named float value.
        /// </summary>
        /// <param name="fieldName">Field name.</param>
        /// <param name="val">Float value.</param>
        public void WriteFloat(string fieldName, float val)
        {
            WriteFieldId(fieldName, PU.TypeFloat);

            _stream.WriteInt(PU.LengthTypeId + 4);
            _stream.WriteByte(PU.TypeFloat);
            _stream.WriteFloat(val);
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
            WriteFieldId(fieldName, PU.TypeArrayFloat);

            if (val == null)
                WriteNullField();
            else
            {
                _stream.WriteInt(PU.LengthTypeId + PU.LengthArraySize + (val.Length << 2));
                _stream.WriteByte(PU.TypeArrayFloat);
                PU.WriteFloatArray(val, _stream);
            }
        }

        /// <summary>
        /// Write float array.
        /// </summary>
        /// <param name="val">Float array.</param>
        public void WriteFloatArray(float[] val)
        {
            if (val == null)
                WriteNullRawField();
            else
            {
                _stream.WriteByte(PU.TypeArrayFloat);
                PU.WriteFloatArray(val, _stream);
            }
        }

        /// <summary>
        /// Write named double value.
        /// </summary>
        /// <param name="fieldName">Field name.</param>
        /// <param name="val">Double value.</param>
        public void WriteDouble(string fieldName, double val)
        {
            WriteFieldId(fieldName, PU.TypeDouble);

            _stream.WriteInt(PU.LengthTypeId + 8);
            _stream.WriteByte(PU.TypeDouble);
            _stream.WriteDouble(val);
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
            WriteFieldId(fieldName, PU.TypeArrayDouble);

            if (val == null)
                WriteNullField();
            else
            {
                _stream.WriteInt(PU.LengthTypeId + PU.LengthArraySize + (val.Length << 3));
                _stream.WriteByte(PU.TypeArrayDouble);
                PU.WriteDoubleArray(val, _stream);
            }
        }

        /// <summary>
        /// Write double array.
        /// </summary>
        /// <param name="val">Double array.</param>
        public void WriteDoubleArray(double[] val)
        {
            if (val == null)
                WriteNullRawField();
            else
            {
                _stream.WriteByte(PU.TypeArrayDouble);
                PU.WriteDoubleArray(val, _stream);
            }
        }

        /// <summary>
        /// Write named decimal value.
        /// </summary>
        /// <param name="fieldName">Field name.</param>
        /// <param name="val">Decimal value.</param>
        public void WriteDecimal(string fieldName, decimal? val)
        {
            WriteFieldId(fieldName, PU.TypeDecimal);

            if (val == null)
                WriteNullField();
            else
            {
                int pos = SkipFieldLength();

                _stream.WriteByte(PU.TypeDecimal);
                PortableUtils.WriteDecimal(val.Value, _stream);

                WriteFieldLength(_stream, pos);
            }
        }

        /// <summary>
        /// Write decimal value.
        /// </summary>
        /// <param name="val">Decimal value.</param>
        public void WriteDecimal(decimal? val)
        {
            if (val == null)
                WriteNullRawField();
            else
            {
                _stream.WriteByte(PU.TypeDecimal);
                PortableUtils.WriteDecimal(val.Value, _stream);
            }
        }

        /// <summary>
        /// Write named decimal array.
        /// </summary>
        /// <param name="fieldName">Field name.</param>
        /// <param name="val">Decimal array.</param>
        public void WriteDecimalArray(string fieldName, decimal?[] val)
        {
            WriteFieldId(fieldName, PU.TypeArrayDecimal);

            if (val == null)
                WriteNullField();
            else
            {
                int pos = SkipFieldLength();

                _stream.WriteByte(PU.TypeArrayDecimal);
                PU.WriteDecimalArray(val, _stream);

                WriteFieldLength(_stream, pos);
            }
        }
        
        /// <summary>
        /// Write decimal array.
        /// </summary>
        /// <param name="val">Decimal array.</param>
        public void WriteDecimalArray(decimal?[] val)
        {
            if (val == null)
                WriteNullRawField();
            else
            {
                _stream.WriteByte(PU.TypeArrayDecimal);
                PU.WriteDecimalArray(val, _stream);
            }
        }

        /// <summary>
        /// Write named date value.
        /// </summary>
        /// <param name="fieldName">Field name.</param>
        /// <param name="val">Date value.</param>
        public void WriteDate(string fieldName, DateTime? val)
        {
            WriteFieldId(fieldName, PU.TypeDate);

            if (val == null)
                WriteNullField();
            else
            {
                _stream.WriteInt(PU.LengthTypeId + 12);

                _stream.WriteByte(PortableUtils.TypeDate);
                PortableUtils.WriteDate(val.Value, _stream);
            }
        }
        
        /// <summary>
        /// Write date value.
        /// </summary>
        /// <param name="val">Date value.</param>
        public void WriteDate(DateTime? val)
        {
            if (val == null)
                WriteNullRawField();
            else
            {
                _stream.WriteByte(PortableUtils.TypeDate);
                PortableUtils.WriteDate(val.Value, _stream);
            }
        }

        /// <summary>
        /// Write named date array.
        /// </summary>
        /// <param name="fieldName">Field name.</param>
        /// <param name="val">Date array.</param>
        public void WriteDateArray(string fieldName, DateTime?[] val)
        {
            WriteFieldId(fieldName, PU.TypeDate);

            if (val == null)
                WriteNullField();
            else
            {
                int pos = SkipFieldLength();

                _stream.WriteByte(PortableUtils.TypeArrayDate);
                PortableUtils.WriteDateArray(val, _stream);

                WriteFieldLength(_stream, pos);
            }
        }

        /// <summary>
        /// Write date array.
        /// </summary>
        /// <param name="val">Date array.</param>
        public void WriteDateArray(DateTime?[] val)
        {
            if (val == null)
                WriteNullRawField();
            else
            {
                _stream.WriteByte(PortableUtils.TypeArrayDate);
                PortableUtils.WriteDateArray(val, _stream);
            }
        }

        /// <summary>
        /// Write named string value.
        /// </summary>
        /// <param name="fieldName">Field name.</param>
        /// <param name="val">String value.</param>
        public void WriteString(string fieldName, string val)
        {
            WriteFieldId(fieldName, PU.TypeString);

            if (val == null)
                WriteNullField();
            else
            {
                int pos = SkipFieldLength();

                _stream.WriteByte(PU.TypeString);
                PU.WriteString(val, _stream);

                WriteFieldLength(_stream, pos);
            }
        }

        /// <summary>
        /// Write string value.
        /// </summary>
        /// <param name="val">String value.</param>
        public void WriteString(string val)
        {
            if (val == null)
                WriteNullRawField();
            else
            {
                _stream.WriteByte(PU.TypeString);
                PU.WriteString(val, _stream);
            }
        }

        /// <summary>
        /// Write named string array.
        /// </summary>
        /// <param name="fieldName">Field name.</param>
        /// <param name="val">String array.</param>
        public void WriteStringArray(string fieldName, string[] val)
        {
            WriteFieldId(fieldName, PU.TypeArrayString);

            if (val == null)
                WriteNullField();
            else
            {
                int pos = SkipFieldLength();

                _stream.WriteByte(PU.TypeArrayString);
                PU.WriteStringArray(val, _stream);

                WriteFieldLength(_stream, pos);
            }
        }

        /// <summary>
        /// Write string array.
        /// </summary>
        /// <param name="val">String array.</param>
        public void WriteStringArray(string[] val)
        {
            if (val == null)
                WriteNullRawField();
            else
            {
                _stream.WriteByte(PU.TypeArrayString);
                PU.WriteStringArray(val, _stream);
            }
        }

        /// <summary>
        /// Write named GUID value.
        /// </summary>
        /// <param name="fieldName">Field name.</param>
        /// <param name="val">GUID value.</param>
        public void WriteGuid(string fieldName, Guid? val)
        {
            WriteFieldId(fieldName, PU.TypeGuid);

            if (val == null)
                WriteNullField();
            else
            {
                _stream.WriteInt(PU.LengthTypeId + 16);

                _stream.WriteByte(PU.TypeGuid);
                PU.WriteGuid(val.Value, _stream);
            }
        }

        /// <summary>
        /// Write GUID value.
        /// </summary>
        /// <param name="val">GUID value.</param>
        public void WriteGuid(Guid? val)
        {
            if (val == null)
                WriteNullRawField();
            else
            {
                _stream.WriteByte(PU.TypeGuid);
                PU.WriteGuid(val.Value, _stream);
            }
        }

        /// <summary>
        /// Write named GUID array.
        /// </summary>
        /// <param name="fieldName">Field name.</param>
        /// <param name="val">GUID array.</param>
        public void WriteGuidArray(string fieldName, Guid?[] val)
        {
            WriteFieldId(fieldName, PU.TypeArrayGuid);

            if (val == null)
                WriteNullField();
            else
            {
                int pos = SkipFieldLength();

                _stream.WriteByte(PU.TypeArrayGuid);
                PU.WriteGuidArray(val, _stream);

                WriteFieldLength(_stream, pos);
            }
        }

        /// <summary>
        /// Write GUID array.
        /// </summary>
        /// <param name="val">GUID array.</param>
        public void WriteGuidArray(Guid?[] val)
        {
            if (val == null)
                WriteNullRawField();
            else
            {
                _stream.WriteByte(PU.TypeArrayGuid);
                PU.WriteGuidArray(val, _stream);
            }
        }

        /// <summary>
        /// Write named enum value.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="fieldName">Field name.</param>
        /// <param name="val">Enum value.</param>
        public void WriteEnum<T>(string fieldName, T val)
        {
            WriteFieldId(fieldName, PU.TypeEnum);

            _stream.WriteInt(PU.LengthTypeId + 16);

            _stream.WriteByte(PU.TypeEnum);
            PortableUtils.WriteEnum(_stream, (Enum)(object)val);
        }

        /// <summary>
        /// Write enum value.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="val">Enum value.</param>
        public void WriteEnum<T>(T val)
        {
            _stream.WriteByte(PU.TypeEnum);
            PortableUtils.WriteEnum(_stream, (Enum)(object)val);
        }

        /// <summary>
        /// Write named enum array.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="fieldName">Field name.</param>
        /// <param name="val">Enum array.</param>
        public void WriteEnumArray<T>(string fieldName, T[] val)
        {
            WriteFieldId(fieldName, PU.TypeArrayEnum);

            if (val == null)
                WriteNullField();
            else
            {
                int pos = SkipFieldLength();

                _stream.WriteByte(PU.TypeArrayEnum);
                PortableUtils.WriteArray(val, this);

                WriteFieldLength(_stream, pos);
            }
        }

        /// <summary>
        /// Write enum array.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="val">Enum array.</param>
        public void WriteEnumArray<T>(T[] val)
        {
            if (val == null)
                WriteNullRawField();
            else
            {
                _stream.WriteByte(PU.TypeArrayEnum);
                PortableUtils.WriteArray(val, this);
            }
        }

        /// <summary>
        /// Write named object value.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="fieldName">Field name.</param>
        /// <param name="val">Object value.</param>
        public void WriteObject<T>(string fieldName, T val)
        {
            WriteFieldId(fieldName, PU.TypeObject);

            if (val == null)
                WriteNullField();
            else
            {
                int pos = SkipFieldLength();

                Write(val);

                WriteFieldLength(_stream, pos);
            }
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
        /// <param name="fieldName">Field name.</param>
        /// <param name="val">Object array.</param>
        public void WriteArray<T>(string fieldName, T[] val)
        {
            WriteFieldId(fieldName, PU.TypeArray);

            if (val == null)
                WriteNullField();
            else
            {
                int pos = SkipFieldLength();

                _stream.WriteByte(PU.TypeArray);
                PortableUtils.WriteArray(val, this);

                WriteFieldLength(_stream, pos);
            }
        }

        /// <summary>
        /// Write object array.
        /// </summary>
        /// <param name="val">Object array.</param>
        public void WriteArray<T>(T[] val)
        {
            if (val == null)
                WriteNullRawField();
            else
            {
                _stream.WriteByte(PU.TypeArray);
                PortableUtils.WriteArray(val, this);
            }
        }

        /// <summary>
        /// Write named collection.
        /// </summary>
        /// <param name="fieldName">Field name.</param>
        /// <param name="val">Collection.</param>
        public void WriteCollection(string fieldName, ICollection val)
        {
            WriteFieldId(fieldName, PU.TypeCollection);

            if (val == null)
                WriteNullField();
            else
            {
                int pos = SkipFieldLength();

                WriteCollection(val);

                WriteFieldLength(_stream, pos);
            }
        }

        /// <summary>
        /// Write collection.
        /// </summary>
        /// <param name="val">Collection.</param>
        public void WriteCollection(ICollection val)
        {
            WriteByte(PU.TypeCollection);
            PU.WriteCollection(val, this);
        }

        /// <summary>
        /// Write named dictionary.
        /// </summary>
        /// <param name="fieldName">Field name.</param>
        /// <param name="val">Dictionary.</param>
        public void WriteDictionary(string fieldName, IDictionary val)
        {
            WriteFieldId(fieldName, PU.TypeDictionary);

            if (val == null)
                WriteNullField();
            else
            {
                int pos = SkipFieldLength();

                WriteDictionary(val);

                WriteFieldLength(_stream, pos);
            }
        }

        /// <summary>
        /// Write dictionary.
        /// </summary>
        /// <param name="val">Dictionary.</param>
        public void WriteDictionary(IDictionary val)
        {
            WriteByte(PU.TypeDictionary);
            PU.WriteDictionary(val, this);
        }

        /// <summary>
        /// Write NULL field.
        /// </summary>
        public void WriteNullField()
        {
            _stream.WriteInt(1);
            _stream.WriteByte(PU.HdrNull);
        }

        /// <summary>
        /// Write NULL raw field.
        /// </summary>
        public void WriteNullRawField()
        {
            _stream.WriteByte(PU.HdrNull);
        }

        /// <summary>
        /// Get raw writer.
        /// </summary>
        /// <returns>
        /// Raw writer.
        /// </returns>
        public IPortableRawWriter GetRawWriter()
        {
            if (_curRawPos == 0)
                _curRawPos = _stream.Position;

            return this;
        }

        /// <summary>
        /// Set new builder.
        /// </summary>
        /// <param name="builder">Builder.</param>
        /// <returns>Previous builder.</returns>
        internal PortableBuilderImpl SetBuilder(PortableBuilderImpl builder)
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
        public void Write<T>(T obj)
        {
            // Handle special case for null.
            if (obj == null)
            {
                _stream.WriteByte(PU.HdrNull);

                return;
            }

            // We use GetType() of a real object instead of typeof(T) to take advantage of 
            // automatic Nullable'1 unwrapping.
            Type type = obj.GetType();

            // Handle common case when primitive is written.
            if (type.IsPrimitive)
            {
                WritePrimitive(obj, type);

                return;
            }

            // Handle special case for builder.
            if (WriteBuilderSpecials(obj))
                return;

            // Suppose that we faced normal object and perform descriptor lookup.
            IPortableTypeDescriptor desc = _marsh.GetDescriptor(type);

            if (desc != null)
            {
                // Writing normal object.
                var pos = _stream.Position;

                // Dealing with handles.
                if (!(desc.Serializer is IPortableSystemTypeSerializer) && WriteHandle(pos, obj))
                    return;

                // Write header.
                _stream.WriteByte(PU.HdrFull);
                _stream.WriteBool(desc.UserType);
                _stream.WriteInt(desc.TypeId);
                _stream.WriteInt(obj.GetHashCode());

                // Skip length as it is not known in the first place.
                _stream.Seek(8, SeekOrigin.Current);

                // Preserve old frame.
                int oldTypeId = _curTypeId;
                IPortableNameMapper oldConverter = _curConverter;
                IPortableIdMapper oldMapper = _curMapper;
                long oldRawPos = _curRawPos;
                
                PortableStructure oldStruct = _curStruct;
                int oldStructPath = _curStructPath;
                int oldStructAction = _curStructAction;
                var oldStructUpdates = _curStructUpdates;

                // Push new frame.
                _curTypeId = desc.TypeId;
                _curConverter = desc.NameConverter;
                _curMapper = desc.Mapper;
                _curRawPos = 0;

                _curStruct = desc.TypeStructure;
                _curStructPath = 0;
                _curStructAction = 0;
                _curStructUpdates = null;

                // Write object fields.
                desc.Serializer.WritePortable(obj, this);

                // Calculate and write length.
                int len = _stream.Position - pos;

                _stream.WriteInt(pos + 10, len);

                if (_curRawPos != 0)
                    _stream.WriteInt(pos + 14, (int)(_curRawPos - pos));
                else
                    _stream.WriteInt(pos + 14, len);

                // Apply structure updates if any.
                if (_curStructUpdates != null)
                {
                    desc.UpdateStructure(_curStruct, _curStructPath, _curStructUpdates);

                    IPortableMetadataHandler metaHnd = _marsh.GetMetadataHandler(desc);

                    if (metaHnd != null)
                    {
                        foreach (var u in _curStructUpdates)
                            metaHnd.OnFieldWrite(u.FieldId, u.FieldName, u.FieldType);

                        IDictionary<string, int> meta = metaHnd.OnObjectWriteFinished();

                        if (meta != null)
                            SaveMetadata(_curTypeId, desc.TypeName, desc.AffinityKeyFieldName, meta);
                    }
                }

                // Restore old frame.
                _curTypeId = oldTypeId;
                _curConverter = oldConverter;
                _curMapper = oldMapper;
                _curRawPos = oldRawPos;

                _curStruct = oldStruct;
                _curStructPath = oldStructPath;
                _curStructAction = oldStructAction;
                _curStructUpdates = oldStructUpdates;
            }
            else
            {
                // Are we dealing with a well-known type?
                PortableSystemWriteDelegate handler = PortableSystemHandlers.GetWriteHandler(type);

                if (handler != null)
                    handler.Invoke(this, obj);
                else
                {
                    if (type.IsSerializable)
                        Write(new SerializableObjectHolder(obj));
                    else
                    // We did our best, object cannot be marshalled.
                        throw new PortableException("Unsupported object type [type=" + type + ", object=" + obj + ']');
                }
            }
        }

        /// <summary>
        /// Write primitive type.
        /// </summary>
        /// <param name="val">Object.</param>
        /// <param name="type">Type.</param>
        public unsafe void WritePrimitive<T>(T val, Type type)
        {
            // .Net defines 14 primitive types. We support 12 - excluding IntPtr and UIntPtr.
            // Types check sequence is designed to minimize comparisons for the most frequent types.

            if (type == typeof(int))
            {
                _stream.WriteByte(PU.TypeInt);
                _stream.WriteInt((int)(object)val);
            }
            else if (type == typeof(long))
            {
                _stream.WriteByte(PU.TypeLong);
                _stream.WriteLong((long)(object)val);
            }
            else if (type == typeof(bool))
            {
                _stream.WriteByte(PU.TypeBool);
                _stream.WriteBool((bool)(object)val);
            }
            else if (type == typeof(byte))
            {
                _stream.WriteByte(PU.TypeByte);
                _stream.WriteByte((byte)(object)val);
            }
            else if (type == typeof(short))
            {
                _stream.WriteByte(PU.TypeShort);
                _stream.WriteShort((short)(object)val);
            }
            else if (type == typeof (char))
            {
                _stream.WriteByte(PU.TypeChar);
                _stream.WriteChar((char)(object)val);
            }
            else if (type == typeof(float))
            {
                _stream.WriteByte(PU.TypeFloat);
                _stream.WriteFloat((float)(object)val);
            }
            else if (type == typeof(double))
            {
                _stream.WriteByte(PU.TypeDouble);
                _stream.WriteDouble((double)(object)val);
            }
            else if (type == typeof(sbyte))
            {
                sbyte val0 = (sbyte)(object)val;

                _stream.WriteByte(PU.TypeByte);
                _stream.WriteByte(*(byte*)&val0);
            }
            else if (type == typeof(ushort))
            {
                ushort val0 = (ushort)(object)val;

                _stream.WriteByte(PU.TypeShort);
                _stream.WriteShort(*(short*)&val0);
            }
            else if (type == typeof(uint))
            {
                uint val0 = (uint)(object)val;

                _stream.WriteByte(PU.TypeInt);
                _stream.WriteInt(*(int*)&val0);
            }
            else if (type == typeof(ulong))
            {
                ulong val0 = (ulong)(object)val;

                _stream.WriteByte(PU.TypeLong);
                _stream.WriteLong(*(long*)&val0);
            }
            else
                throw new PortableException("Unsupported object type [type=" + type.FullName + ", object=" + val + ']');
        }

        /// <summary>
        /// Try writing object as special builder type.
        /// </summary>
        /// <param name="obj">Object.</param>
        /// <returns>True if object was written, false otherwise.</returns>
        private bool WriteBuilderSpecials<T>(T obj)
        {
            if (_builder != null)
            {
                // Special case for portable object during build.
                PortableUserObject portObj = obj as PortableUserObject;

                if (portObj != null)
                {
                    if (!WriteHandle(_stream.Position, portObj))
                        _builder.ProcessPortable(_stream, portObj);

                    return true;
                }

                // Special case for builder during build.
                PortableBuilderImpl portBuilder = obj as PortableBuilderImpl;

                if (portBuilder != null)
                {
                    if (!WriteHandle(_stream.Position, portBuilder))
                        _builder.ProcessBuilder(_stream, portBuilder);

                    return true;
                }
            }

            return false;
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

            _stream.WriteByte(PU.HdrHnd);

            // Handle is written as difference between position before header and handle position.
            _stream.WriteInt((int)(pos - hndPos));

            return true;
        }

        /// <summary>
        /// Perform action with detached semantics.
        /// </summary>
        /// <param name="a"></param>
        internal void WithDetach(Action<PortableWriterImpl> a)
        {
            if (_detaching)
                a(this);
            else
            {
                _detaching = true;

                PortableHandleDictionary<object, long> oldHnds = _hnds;
                _hnds = null;

                try
                {
                    a(this);
                }
                finally
                {
                    _detaching = false;

                    if (oldHnds != null)
                    {
                        // Merge newly recorded handles with old ones and restore old on the stack.
                        // Otherwise we can use current handles right away.
                        if (_hnds != null)
                            oldHnds.Merge(_hnds);

                        _hnds = oldHnds;
                    }
                }
            }
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
                return _marsh.GetDescriptor(type) != null || PortableSystemHandlers.GetWriteHandler(type) != null;
            }

            return true;
        }
        
        /// <summary>
        /// Write field ID.
        /// </summary>
        /// <param name="fieldName">Field name.</param>
        /// <param name="fieldTypeId">Field type ID.</param>
        private void WriteFieldId(string fieldName, byte fieldTypeId)
        {
            if (_curRawPos != 0)
                throw new PortableException("Cannot write named fields after raw data is written.");

            int action = _curStructAction++;

            int fieldId;

            if (_curStructUpdates == null)
            {
                fieldId = _curStruct.GetFieldId(fieldName, fieldTypeId, ref _curStructPath, action);

                if (fieldId == 0)
                    fieldId = GetNewFieldId(fieldName, fieldTypeId, action);
            }
            else
                fieldId = GetNewFieldId(fieldName, fieldTypeId, action);

            _stream.WriteInt(fieldId);
        }

        /// <summary>
        /// Get ID for the new field and save structure update.
        /// </summary>
        /// <param name="fieldName">Field name.</param>
        /// <param name="fieldTypeId">Field type ID.</param>
        /// <param name="action">Action index.</param>
        /// <returns>Field ID.</returns>
        private int GetNewFieldId(string fieldName, byte fieldTypeId, int action)
        {
            int fieldId = PU.FieldId(_curTypeId, fieldName, _curConverter, _curMapper);

            if (_curStructUpdates == null)
                _curStructUpdates = new List<PortableStructureUpdate>();

            _curStructUpdates.Add(new PortableStructureUpdate(fieldName, fieldId, fieldTypeId, action));

            return fieldId;
        }

        /// <summary>
        /// Skip field lenght and return position where it is to be written.
        /// </summary>
        /// <returns></returns>
        private int SkipFieldLength()
        {
            int pos = _stream.Position;

            _stream.Seek(4, SeekOrigin.Current);

            return pos;
        }

        /// <summary>
        /// Write field length.
        /// </summary>
        /// <param name="stream">Stream.</param>
        /// <param name="pos">Position where length should reside</param>
        private static void WriteFieldLength(IPortableStream stream, int pos)
        {
            // Length is is a difference between current position and previously recorder 
            // length placeholder position minus 4 bytes for the length itself.
            stream.WriteInt(pos, stream.Position - pos - 4);
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
