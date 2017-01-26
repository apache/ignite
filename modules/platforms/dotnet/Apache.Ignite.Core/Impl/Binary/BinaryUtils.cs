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
    using System.Diagnostics;
    using System.Diagnostics.CodeAnalysis;
    using System.Globalization;
    using System.IO;
    using System.Linq;
    using System.Reflection;
    using System.Runtime.InteropServices;
    using System.Text;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Impl.Binary.IO;
    using Apache.Ignite.Core.Impl.Common;

    /**
     * <summary>Utilities for binary serialization.</summary>
     */
    internal static class BinaryUtils
    {
        /** Header of NULL object. */
        public const byte HdrNull = 101;

        /** Header of object handle. */
        public const byte HdrHnd = 102;

        /** Header of object in fully serialized form. */
        public const byte HdrFull = 103;

        /** Protocol versnion. */
        public const byte ProtoVer = 1;

        /** Type: object. */
        public const byte TypeObject = HdrFull;

        /** Type: unsigned byte. */
        public const byte TypeByte = 1;

        /** Type: short. */
        public const byte TypeShort = 2;

        /** Type: int. */
        public const byte TypeInt = 3;

        /** Type: long. */
        public const byte TypeLong = 4;

        /** Type: float. */
        public const byte TypeFloat = 5;

        /** Type: double. */
        public const byte TypeDouble = 6;

        /** Type: char. */
        public const byte TypeChar = 7;

        /** Type: boolean. */
        public const byte TypeBool = 8;
        
        /** Type: decimal. */
        public const byte TypeDecimal = 30;

        /** Type: string. */
        public const byte TypeString = 9;

        /** Type: GUID. */
        public const byte TypeGuid = 10;

        /** Type: date. */
        public const byte TypeTimestamp = 33;

        /** Type: unsigned byte array. */
        public const byte TypeArrayByte = 12;

        /** Type: short array. */
        public const byte TypeArrayShort = 13;

        /** Type: int array. */
        public const byte TypeArrayInt = 14;

        /** Type: long array. */
        public const byte TypeArrayLong = 15;

        /** Type: float array. */
        public const byte TypeArrayFloat = 16;

        /** Type: double array. */
        public const byte TypeArrayDouble = 17;

        /** Type: char array. */
        public const byte TypeArrayChar = 18;

        /** Type: boolean array. */
        public const byte TypeArrayBool = 19;

        /** Type: decimal array. */
        public const byte TypeArrayDecimal = 31;

        /** Type: string array. */
        public const byte TypeArrayString = 20;

        /** Type: GUID array. */
        public const byte TypeArrayGuid = 21;

        /** Type: date array. */
        public const byte TypeArrayTimestamp = 34;

        /** Type: object array. */
        public const byte TypeArray = 23;

        /** Type: collection. */
        public const byte TypeCollection = 24;

        /** Type: map. */
        public const byte TypeDictionary = 25;
        
        /** Type: binary object. */
        public const byte TypeBinary = 27;

        /** Type: enum. */
        public const byte TypeEnum = 28;

        /** Type: enum array. */
        public const byte TypeArrayEnum = 29;
        
        /** Type: native job holder. */
        public const byte TypeNativeJobHolder = 77;

        /** Type: Ignite proxy. */
        public const byte TypeIgniteProxy = 74;

        /** Type: function wrapper. */
        public const byte TypeComputeOutFuncJob = 80;

        /** Type: function wrapper. */
        public const byte TypeComputeFuncJob = 81;

        /** Type: continuous query remote filter. */
        public const byte TypeContinuousQueryRemoteFilterHolder = 82;

        /** Type: Compute out func wrapper. */
        public const byte TypeComputeOutFuncWrapper = 83;

        /** Type: Compute func wrapper. */
        public const byte TypeComputeFuncWrapper = 85;

        /** Type: Compute job wrapper. */
        public const byte TypeComputeJobWrapper = 86;

        /** Type: Serializable wrapper. */
        public const byte TypeSerializableHolder = 87;

        /** Type: DateTime wrapper. */
        public const byte TypeDateTimeHolder = 93;

        /** Type: action wrapper. */
        public const byte TypeComputeActionJob = 88;

        /** Type: entry processor holder. */
        public const byte TypeCacheEntryProcessorHolder = 89;

        /** Type: entry predicate holder. */
        public const byte TypeCacheEntryPredicateHolder = 90;
        
        /** Type: message filter holder. */
        public const byte TypeMessageListenerHolder = 92;

        /** Type: stream receiver holder. */
        public const byte TypeStreamReceiverHolder = 94;

        /** Type: platform object proxy. */
        public const byte TypePlatformJavaObjectFactoryProxy = 99;

        /** Collection: custom. */
        public const byte CollectionCustom = 0;

        /** Collection: array list. */
        public const byte CollectionArrayList = 1;

        /** Collection: linked list. */
        public const byte CollectionLinkedList = 2;
        
        /** Map: custom. */
        public const byte MapCustom = 0;

        /** Map: hash map. */
        public const byte MapHashMap = 1;
        
        /** Byte "0". */
        public const byte ByteZero = 0;

        /** Byte "1". */
        public const byte ByteOne = 1;

        /** Indicates object array. */
        public const int ObjTypeId = -1;

        /** Length of array size. */
        public const int LengthArraySize = 4;

        /** Int type. */
        public static readonly Type TypInt = typeof(int);

        /** Collection type. */
        public static readonly Type TypCollection = typeof(ICollection);

        /** Dictionary type. */
        public static readonly Type TypDictionary = typeof(IDictionary);

        /** Ticks for Java epoch. */
        private static readonly long JavaDateTicks = new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc).Ticks;
        
        /** Bindig flags for static search. */
        private const BindingFlags BindFlagsStatic = BindingFlags.Static | BindingFlags.Public | BindingFlags.NonPublic;

        /** Default poratble marshaller. */
        private static readonly Marshaller Marsh = new Marshaller(null);

        /** Method: ReadArray. */
        public static readonly MethodInfo MtdhReadArray =
            typeof(BinaryUtils).GetMethod("ReadArray", BindFlagsStatic);

        /** Cached UTF8 encoding. */
        private static readonly Encoding Utf8 = Encoding.UTF8;

        /** Cached generic array read funcs. */
        private static readonly CopyOnWriteConcurrentDictionary<Type, Func<BinaryReader, bool, object>>
            ArrayReaders = new CopyOnWriteConcurrentDictionary<Type, Func<BinaryReader, bool, object>>();

        /** Flag indicating whether Guid struct is sequential in current runtime. */
        private static readonly bool IsGuidSequential = GetIsGuidSequential();

        /** Guid writer. */
        public static readonly Action<Guid, IBinaryStream> WriteGuid = IsGuidSequential
            ? (Action<Guid, IBinaryStream>)WriteGuidFast : WriteGuidSlow;

        /** Guid reader. */
        public static readonly Func<IBinaryStream, Guid?> ReadGuid = IsGuidSequential
            ? (Func<IBinaryStream, Guid?>)ReadGuidFast : ReadGuidSlow;

        /** String mode environment variable. */
        public const string IgniteBinaryMarshallerUseStringSerializationVer2 =
            "IGNITE_BINARY_MARSHALLER_USE_STRING_SERIALIZATION_VER_2";

        /** String mode. */
        public static readonly bool UseStringSerializationVer2 =
            (Environment.GetEnvironmentVariable(IgniteBinaryMarshallerUseStringSerializationVer2) ?? "false") == "true";

        /// <summary>
        /// Default marshaller.
        /// </summary>
        public static Marshaller Marshaller
        {
            get { return Marsh; }
        }

        /**
         * <summary>Write boolean array.</summary>
         * <param name="vals">Value.</param>
         * <param name="stream">Output stream.</param>
         */
        public static void WriteBooleanArray(bool[] vals, IBinaryStream stream)
        {
            stream.WriteInt(vals.Length);

            stream.WriteBoolArray(vals);
        }

        /**
         * <summary>Read boolean array.</summary>
         * <param name="stream">Output stream.</param>
         * <returns>Value.</returns>
         */
        public static bool[] ReadBooleanArray(IBinaryStream stream)
        {
            int len = stream.ReadInt();

            return stream.ReadBoolArray(len);
        }

        /**
         * <summary>Write byte array.</summary>
         * <param name="vals">Value.</param>
         * <param name="stream">Output stream.</param>
         * <returns>Length of written data.</returns>
         */
        public static void WriteByteArray(byte[] vals, IBinaryStream stream)
        {
            stream.WriteInt(vals.Length);

            stream.WriteByteArray(vals);
        }

        /**
         * <summary>Read byte array.</summary>
         * <param name="stream">Output stream.</param>
         * <returns>Value.</returns>
         */
        public static byte[] ReadByteArray(IBinaryStream stream)
        {
            return stream.ReadByteArray(stream.ReadInt());
        }

        /**
         * <summary>Read byte array.</summary>
         * <param name="stream">Output stream.</param>
         * <returns>Value.</returns>
         */
        public static unsafe sbyte[] ReadSbyteArray(IBinaryStream stream)
        {
            int len = stream.ReadInt();

            sbyte[] res = new sbyte[len];

            fixed (sbyte* res0 = res)
            {
                stream.Read((byte*) res0, len);
            }

            return res;
        }

        /**
         * <summary>Read byte array.</summary>
         * <param name="data">Data.</param>
         * <param name="pos">Position.</param>
         * <returns>Value.</returns>
         */
        public static byte[] ReadByteArray(byte[] data, int pos) {
            int len = ReadInt(data, pos);

            pos += 4;

            byte[] res = new byte[len];

            Buffer.BlockCopy(data, pos, res, 0, len);

            return res;
        }

        /**
         * <summary>Write short array.</summary>
         * <param name="vals">Value.</param>
         * <param name="stream">Output stream.</param>
         */
        public static void WriteShortArray(short[] vals, IBinaryStream stream)
        {
            stream.WriteInt(vals.Length);

            stream.WriteShortArray(vals);
        }

        /**
         * <summary>Read short array.</summary>
         * <param name="stream">Stream.</param>
         * <returns>Value.</returns>
         */
        public static unsafe ushort[] ReadUshortArray(IBinaryStream stream)
        {
            int len = stream.ReadInt();

            ushort[] res = new ushort[len];

            fixed (ushort* res0 = res)
            {
                stream.Read((byte*) res0, len * 2);
            }

            return res;
        }

        /**
         * <summary>Read short array.</summary>
         * <param name="stream">Stream.</param>
         * <returns>Value.</returns>
         */
        public static short[] ReadShortArray(IBinaryStream stream)
        {
            return stream.ReadShortArray(stream.ReadInt());
        }

        /**
         * <summary>Read int value.</summary>
         * <param name="data">Data array.</param>
         * <param name="pos">Position.</param>
         * <returns>Value.</returns>
         */
        public static int ReadInt(byte[] data, int pos) {
            int val = data[pos];

            val |= data[pos + 1] << 8;
            val |= data[pos + 2] << 16;
            val |= data[pos + 3] << 24;

            return val;
        }

        /**
         * <summary>Read long value.</summary>
         * <param name="data">Data array.</param>
         * <param name="pos">Position.</param>
         * <returns>Value.</returns>
         */
        public static long ReadLong(byte[] data, int pos) {
            long val = (long)(data[pos]) << 0;

            val |= (long)(data[pos + 1]) << 8;
            val |= (long)(data[pos + 2]) << 16;
            val |= (long)(data[pos + 3]) << 24;
            val |= (long)(data[pos + 4]) << 32;
            val |= (long)(data[pos + 5]) << 40;
            val |= (long)(data[pos + 6]) << 48;
            val |= (long)(data[pos + 7]) << 56;

            return val;
        }

        /**
         * <summary>Write int array.</summary>
         * <param name="vals">Value.</param>
         * <param name="stream">Output stream.</param>
         */
        public static void WriteIntArray(int[] vals, IBinaryStream stream)
        {
            stream.WriteInt(vals.Length);

            stream.WriteIntArray(vals);
        }

        /**
         * <summary>Read int array.</summary>
         * <param name="stream">Stream.</param>
         * <returns>Value.</returns>
         */
        public static int[] ReadIntArray(IBinaryStream stream)
        {
            return stream.ReadIntArray(stream.ReadInt());
        }

        /**
         * <summary>Read int array.</summary>
         * <param name="stream">Stream.</param>
         * <returns>Value.</returns>
         */
        public static unsafe uint[] ReadUintArray(IBinaryStream stream)
        {
            int len = stream.ReadInt();

            uint[] res = new uint[len];

            fixed (uint* res0 = res)
            {
                stream.Read((byte*) res0, len * 4);
            }

            return res;
        }

        /**
         * <summary>Write long array.</summary>
         * <param name="vals">Value.</param>
         * <param name="stream">Output stream.</param>
         */
        public static void WriteLongArray(long[] vals, IBinaryStream stream)
        {
            stream.WriteInt(vals.Length);

            stream.WriteLongArray(vals);
        }

        /**
         * <summary>Read long array.</summary>
         * <param name="stream">Stream.</param>
         * <returns>Value.</returns>
         */
        public static long[] ReadLongArray(IBinaryStream stream)
        {
            return stream.ReadLongArray(stream.ReadInt());
        }

        /**
         * <summary>Read ulong array.</summary>
         * <param name="stream">Stream.</param>
         * <returns>Value.</returns>
         */
        public static unsafe ulong[] ReadUlongArray(IBinaryStream stream)
        {
            int len = stream.ReadInt();

            ulong[] res = new ulong[len];

            fixed (ulong* res0 = res)
            {
                stream.Read((byte*) res0, len * 8);
            }

            return res;
        }

        /**
         * <summary>Write char array.</summary>
         * <param name="vals">Value.</param>
         * <param name="stream">Output stream.</param>
         */
        public static void WriteCharArray(char[] vals, IBinaryStream stream)
        {
            stream.WriteInt(vals.Length);

            stream.WriteCharArray(vals);
        }

        /**
         * <summary>Read char array.</summary>
         * <param name="stream">Stream.</param>
         * <returns>Value.</returns>
         */
        public static char[] ReadCharArray(IBinaryStream stream)
        {
            int len = stream.ReadInt();

            return stream.ReadCharArray(len);
        }

        /**
         * <summary>Write float array.</summary>
         * <param name="vals">Value.</param>
         * <param name="stream">Output stream.</param>
         */
        public static void WriteFloatArray(float[] vals, IBinaryStream stream)
        {
            stream.WriteInt(vals.Length);

            stream.WriteFloatArray(vals);
        }

        /**
         * <summary>Read float array.</summary>
         * <param name="stream">Stream.</param>
         * <returns>Value.</returns>
         */
        public static float[] ReadFloatArray(IBinaryStream stream)
        {
            int len = stream.ReadInt();

            return stream.ReadFloatArray(len);
        }

        /**
         * <summary>Write double array.</summary>
         * <param name="vals">Value.</param>
         * <param name="stream">Output stream.</param>
         */
        public static void WriteDoubleArray(double[] vals, IBinaryStream stream)
        {
            stream.WriteInt(vals.Length);

            stream.WriteDoubleArray(vals);
        }

        /**
         * <summary>Read double array.</summary>
         * <param name="stream">Stream.</param>
         * <returns>Value.</returns>
         */
        public static double[] ReadDoubleArray(IBinaryStream stream)
        {
            int len = stream.ReadInt();

            return stream.ReadDoubleArray(len);
        }

        /**
         * <summary>Write date.</summary>
         * <param name="val">Date.</param>
         * <param name="stream">Stream.</param>
         */
        public static void WriteTimestamp(DateTime val, IBinaryStream stream)
        {
            long high;
            int low;

            ToJavaDate(val, out high, out low);

            stream.WriteLong(high);
            stream.WriteInt(low);
        }

        /**
         * <summary>Read date.</summary>
         * <param name="stream">Stream.</param>
         * <returns>Date</returns>
         */
        public static DateTime? ReadTimestamp(IBinaryStream stream)
        {
            long high = stream.ReadLong();
            int low = stream.ReadInt();

            return new DateTime(JavaDateTicks + high * TimeSpan.TicksPerMillisecond + low / 100, DateTimeKind.Utc);
        }
        
        /// <summary>
        /// Write nullable date array.
        /// </summary>
        /// <param name="vals">Values.</param>
        /// <param name="stream">Stream.</param>
        public static void WriteTimestampArray(DateTime?[] vals, IBinaryStream stream)
        {
            stream.WriteInt(vals.Length);

            foreach (DateTime? val in vals)
            {
                if (val.HasValue)
                {
                    stream.WriteByte(TypeTimestamp);

                    WriteTimestamp(val.Value, stream);
                }
                else
                    stream.WriteByte(HdrNull);
            }
        }
        
        /**
         * <summary>Write string in UTF8 encoding.</summary>
         * <param name="val">String.</param>
         * <param name="stream">Stream.</param>
         */
        public static unsafe void WriteString(string val, IBinaryStream stream)
        {
            int charCnt = val.Length;

            fixed (char* chars = val)
            {
                int byteCnt = GetUtf8ByteCount(chars, charCnt);

                stream.WriteInt(byteCnt);

                stream.WriteString(chars, charCnt, byteCnt, Utf8);
            }
        }

        /// <summary>
        /// Converts string to UTF8 bytes.
        /// </summary>
        /// <param name="chars">Chars.</param>
        /// <param name="charCnt">Chars count.</param>
        /// <param name="byteCnt">Bytes count.</param>
        /// <param name="enc">Encoding.</param>
        /// <param name="data">Data.</param>
        /// <returns>Amount of bytes written.</returns>
        public static unsafe int StringToUtf8Bytes(char* chars, int charCnt, int byteCnt, Encoding enc, byte* data)
        {
            if (!UseStringSerializationVer2)
                return enc.GetBytes(chars, charCnt, data, byteCnt);

            int strLen = charCnt;
            // ReSharper disable TooWideLocalVariableScope (keep code similar to Java part)
            int c, cnt;
            // ReSharper restore TooWideLocalVariableScope

            int position = 0;

            for (cnt = 0; cnt < strLen; cnt++)
            {
                c = *(chars + cnt);

                if (c >= 0x0001 && c <= 0x007F)
                    *(data + position++) = (byte)c;
                else if (c > 0x07FF)
                {
                    *(data + position++) = (byte)(0xE0 | ((c >> 12) & 0x0F));
                    *(data + position++) = (byte)(0x80 | ((c >> 6) & 0x3F));
                    *(data + position++) = (byte)(0x80 | (c & 0x3F));
                }
                else
                {
                    *(data + position++) = (byte)(0xC0 | ((c >> 6) & 0x1F));
                    *(data + position++) = (byte)(0x80 | (c & 0x3F));
                }
            }

            return position;
        }

        /// <summary>
        /// Gets the UTF8 byte count.
        /// </summary>
        /// <param name="chars">The chars.</param>
        /// <param name="strLen">Length of the string.</param>
        /// <returns>UTF byte count.</returns>
        private static unsafe int GetUtf8ByteCount(char* chars, int strLen)
        {
            int utfLen = 0;
            int cnt;
            for (cnt = 0; cnt < strLen; cnt++)
            {
                var c = *(chars + cnt);

                // ASCII
                if (c >= 0x0001 && c <= 0x007F)
                    utfLen++;
                // Special symbols (surrogates)
                else if (c > 0x07FF)
                    utfLen += 3;
                // The rest of the symbols.
                else
                    utfLen += 2;
            }
            return utfLen;
        }

        /// <summary>
        /// Converts UTF8 bytes to string.
        /// </summary>
        /// <param name="arr">The bytes.</param>
        /// <returns>Resulting string.</returns>
        public static string Utf8BytesToString(byte[] arr)
        {
            if (!UseStringSerializationVer2)
                return Utf8.GetString(arr);

            int len = arr.Length, off = 0;
            int c, charArrCnt = 0, total = len;
            // ReSharper disable TooWideLocalVariableScope (keep code similar to Java part)
            int c2, c3;
            // ReSharper restore TooWideLocalVariableScope
            char[] res = new char[len];

            // try reading ascii
            while (off < total)
            {
                c = arr[off] & 0xff;

                if (c > 127)
                    break;

                off++;

                res[charArrCnt++] = (char)c;
            }

            // read other
            while (off < total)
            {
                c = arr[off] & 0xff;

                switch (c >> 4)
                {
                    case 0:
                    case 1:
                    case 2:
                    case 3:
                    case 4:
                    case 5:
                    case 6:
                    case 7:
                        /* 0xxxxxxx*/
                        off++;

                        res[charArrCnt++] = (char)c;

                        break;
                    case 12:
                    case 13:
                        /* 110x xxxx   10xx xxxx*/
                        off += 2;

                        if (off > total)
                            throw new BinaryObjectException("Malformed input: partial character at end");

                        c2 = arr[off - 1];

                        if ((c2 & 0xC0) != 0x80)
                            throw new BinaryObjectException("Malformed input around byte: " + off);

                        res[charArrCnt++] = (char)(((c & 0x1F) << 6) | (c2 & 0x3F));

                        break;
                    case 14:
                        /* 1110 xxxx  10xx xxxx  10xx xxxx */
                        off += 3;

                        if (off > total)
                            throw new BinaryObjectException("Malformed input: partial character at end");

                        c2 = arr[off - 2];

                        c3 = arr[off - 1];

                        if (((c2 & 0xC0) != 0x80) || ((c3 & 0xC0) != 0x80))
                            throw new BinaryObjectException("Malformed input around byte: " + (off - 1));

                        res[charArrCnt++] = (char)(((c & 0x0F) << 12) |
                            ((c2 & 0x3F) << 6) |
                            ((c3 & 0x3F) << 0));

                        break;
                    default:
                        /* 10xx xxxx,  1111 xxxx */
                        throw new BinaryObjectException("Malformed input around byte: " + off);
                }
            }

            return len == charArrCnt ? new string(res) : new string(res, 0, charArrCnt);
        }

        /**
         * <summary>Read string in UTF8 encoding.</summary>
         * <param name="stream">Stream.</param>
         * <returns>String.</returns>
         */
        public static string ReadString(IBinaryStream stream)
        {
            byte[] bytes = ReadByteArray(stream);

            return bytes != null ? Utf8BytesToString(bytes) : null;
        }

        /**
         * <summary>Write string array in UTF8 encoding.</summary>
         * <param name="vals">String array.</param>
         * <param name="stream">Stream.</param>
         */
        public static void WriteStringArray(string[] vals, IBinaryStream stream)
        {
            stream.WriteInt(vals.Length);

            foreach (string val in vals)
            {
                if (val != null)
                {
                    stream.WriteByte(TypeString);
                    WriteString(val, stream);
                }
                else
                    stream.WriteByte(HdrNull);
            }
        }

        /**
         * <summary>Read string array in UTF8 encoding.</summary>
         * <param name="stream">Stream.</param>
         * <returns>String array.</returns>
         */
        public static string[] ReadStringArray(IBinaryStream stream)
        {
            int len = stream.ReadInt();

            string[] vals = new string[len];

            for (int i = 0; i < len; i++)
                vals[i] = ReadString(stream);

            return vals;
        }

        /**
         * <summary>Write decimal value.</summary>
         * <param name="val">Decimal value.</param>
         * <param name="stream">Stream.</param>
         */
        public static void WriteDecimal(decimal val, IBinaryStream stream) 
        {
            // Vals are:
            // [0] = lo
            // [1] = mid
            // [2] = high
            // [3] = flags
            int[] vals = decimal.GetBits(val);
            
            // Get start index skipping leading zeros.
            int idx = vals[2] != 0 ? 2 : vals[1] != 0 ? 1 : vals[0] != 0 ? 0 : -1;
                        
            // Write scale and negative flag.
            int scale = (vals[3] & 0x00FF0000) >> 16; 

            stream.WriteInt(((vals[3] & 0x80000000) == 0x80000000) ? (int)((uint)scale | 0x80000000) : scale);

            if (idx == -1)
            {
                // Writing zero.
                stream.WriteInt(1);
                stream.WriteByte(0);
            }
            else
            {
                int len = (idx + 1) << 2;
                
                // Write data.
                for (int i = idx; i >= 0; i--)
                {
                    int curPart = vals[i];

                    int part24 = (curPart >> 24) & 0xFF;
                    int part16 = (curPart >> 16) & 0xFF;
                    int part8 = (curPart >> 8) & 0xFF;
                    int part0 = curPart & 0xFF;
                    
                    if (i == idx)
                    {
                        // Possibly skipping some values here.
                        if (part24 != 0)
                        {
                            if ((part24 & 0x80) == 0x80)
                            {
                                stream.WriteInt(len + 1);

                                stream.WriteByte(ByteZero);
                            }
                            else
                                stream.WriteInt(len);

                            stream.WriteByte((byte)part24);
                            stream.WriteByte((byte)part16);
                            stream.WriteByte((byte)part8);
                            stream.WriteByte((byte)part0);
                        }
                        else if (part16 != 0)
                        {
                            if ((part16 & 0x80) == 0x80)
                            {
                                stream.WriteInt(len);

                                stream.WriteByte(ByteZero);
                            }
                            else
                                stream.WriteInt(len - 1);

                            stream.WriteByte((byte)part16);
                            stream.WriteByte((byte)part8);
                            stream.WriteByte((byte)part0);
                        }
                        else if (part8 != 0)
                        {
                            if ((part8 & 0x80) == 0x80)
                            {
                                stream.WriteInt(len - 1);

                                stream.WriteByte(ByteZero);
                            }
                            else
                                stream.WriteInt(len - 2);

                            stream.WriteByte((byte)part8);
                            stream.WriteByte((byte)part0);
                        }
                        else
                        {
                            if ((part0 & 0x80) == 0x80)
                            {
                                stream.WriteInt(len - 2);

                                stream.WriteByte(ByteZero);
                            }
                            else
                                stream.WriteInt(len - 3);

                            stream.WriteByte((byte)part0);
                        }
                    }
                    else
                    {
                        stream.WriteByte((byte)part24);
                        stream.WriteByte((byte)part16);
                        stream.WriteByte((byte)part8);
                        stream.WriteByte((byte)part0);
                    }
                }
            }
        }

        /**
         * <summary>Read decimal value.</summary>
         * <param name="stream">Stream.</param>
         * <returns>Decimal value.</returns>
         */
        public static decimal? ReadDecimal(IBinaryStream stream)
        {
            int scale = stream.ReadInt();

            bool neg;

            if (scale < 0)
            {
                scale = scale & 0x7FFFFFFF;

                neg = true;
            }
            else
                neg = false;

            byte[] mag = ReadByteArray(stream);

            if (scale < 0 || scale > 28)
                throw new BinaryObjectException("Decimal value scale overflow (must be between 0 and 28): " + scale);

            if (mag.Length > 13)
                throw new BinaryObjectException("Decimal magnitude overflow (must be less than 96 bits): " + 
                    mag.Length * 8);

            if (mag.Length == 13 && mag[0] != 0)
                throw new BinaryObjectException("Decimal magnitude overflow (must be less than 96 bits): " +
                        mag.Length * 8);

            int hi = 0;
            int mid = 0;
            int lo = 0;

            int ctr = -1;

            for (int i = mag.Length - 12; i < mag.Length; i++)
            {
                if (++ctr == 4)
                {
                    mid = lo;
                    lo = 0;
                }
                else if (ctr == 8)
                {
                    hi = mid;
                    mid = lo;
                    lo = 0;
                }

                if (i >= 0)
                    lo = (lo << 8) + mag[i];
            }

            return new decimal(lo, mid, hi, neg, (byte)scale);
        }

        /**
         * <summary>Write decimal array.</summary>
         * <param name="vals">Decimal array.</param>
         * <param name="stream">Stream.</param>
         */
        public static void WriteDecimalArray(decimal?[] vals, IBinaryStream stream)
        {
            stream.WriteInt(vals.Length);

            foreach (var val in vals)
            {
                if (val.HasValue)
                {
                    stream.WriteByte(TypeDecimal);

                    WriteDecimal(val.Value, stream);
                }
                else
                    stream.WriteByte(HdrNull);
            }
        }

        /**
         * <summary>Read decimal array.</summary>
         * <param name="stream">Stream.</param>
         * <returns>Decimal array.</returns>
         */
        public static decimal?[] ReadDecimalArray(IBinaryStream stream)
        {
            int len = stream.ReadInt();

            var vals = new decimal?[len];

            for (int i = 0; i < len; i++)
                vals[i] = stream.ReadByte() == HdrNull ? null : ReadDecimal(stream);

            return vals;
        }

        /// <summary>
        /// Gets a value indicating whether <see cref="Guid"/> fields are stored sequentially in memory.
        /// </summary>
        /// <returns></returns>
        private static unsafe bool GetIsGuidSequential()
        {
            // Check that bitwise conversion returns correct result
            var guid = Guid.NewGuid();

            var bytes = guid.ToByteArray();

            var bytes0 = (byte*) &guid;

            for (var i = 0; i < bytes.Length; i++)
                if (bytes[i] != bytes0[i])
                    return false;

            return true;
        }

        /// <summary>
        /// Writes a guid with bitwise conversion, assuming that <see cref="Guid"/> 
        /// is laid out in memory sequentially and without gaps between fields.
        /// </summary>
        /// <param name="val">The value.</param>
        /// <param name="stream">The stream.</param>
        public static unsafe void WriteGuidFast(Guid val, IBinaryStream stream)
        {
            var jguid = new JavaGuid(val);

            var ptr = &jguid;

            stream.Write((byte*) ptr, 16);
        }

        /// <summary>
        /// Writes a guid byte by byte.
        /// </summary>
        /// <param name="val">The value.</param>
        /// <param name="stream">The stream.</param>
        public static unsafe void WriteGuidSlow(Guid val, IBinaryStream stream)
        {
            var bytes = val.ToByteArray();
            byte* jBytes = stackalloc byte[16];

            jBytes[0] = bytes[6]; // c1
            jBytes[1] = bytes[7]; // c2

            jBytes[2] = bytes[4]; // b1
            jBytes[3] = bytes[5]; // b2

            jBytes[4] = bytes[0]; // a1
            jBytes[5] = bytes[1]; // a2
            jBytes[6] = bytes[2]; // a3
            jBytes[7] = bytes[3]; // a4

            jBytes[8] = bytes[15]; // k
            jBytes[9] = bytes[14]; // j
            jBytes[10] = bytes[13]; // i
            jBytes[11] = bytes[12]; // h
            jBytes[12] = bytes[11]; // g
            jBytes[13] = bytes[10]; // f
            jBytes[14] = bytes[9]; // e
            jBytes[15] = bytes[8]; // d
            
            stream.Write(jBytes, 16);
        }

        /// <summary>
        /// Reads a guid with bitwise conversion, assuming that <see cref="Guid"/> 
        /// is laid out in memory sequentially and without gaps between fields.
        /// </summary>
        /// <param name="stream">The stream.</param>
        /// <returns>Guid.</returns>
        public static unsafe Guid? ReadGuidFast(IBinaryStream stream)
        {
            JavaGuid jguid;

            var ptr = (byte*) &jguid;

            stream.Read(ptr, 16);

            var dotnetGuid = new GuidAccessor(jguid);

            return *(Guid*) (&dotnetGuid);
        }

        /// <summary>
        /// Reads a guid byte by byte.
        /// </summary>
        /// <param name="stream">The stream.</param>
        /// <returns>Guid.</returns>
        public static unsafe Guid? ReadGuidSlow(IBinaryStream stream)
        {
            byte* jBytes = stackalloc byte[16];

            stream.Read(jBytes, 16);

            var bytes = new byte[16];

            bytes[0] = jBytes[4]; // a1
            bytes[1] = jBytes[5]; // a2
            bytes[2] = jBytes[6]; // a3
            bytes[3] = jBytes[7]; // a4

            bytes[4] = jBytes[2]; // b1
            bytes[5] = jBytes[3]; // b2

            bytes[6] = jBytes[0]; // c1
            bytes[7] = jBytes[1]; // c2

            bytes[8] = jBytes[15]; // d
            bytes[9] = jBytes[14]; // e
            bytes[10] = jBytes[13]; // f
            bytes[11] = jBytes[12]; // g
            bytes[12] = jBytes[11]; // h
            bytes[13] = jBytes[10]; // i
            bytes[14] = jBytes[9]; // j
            bytes[15] = jBytes[8]; // k

            return new Guid(bytes);
        }

        /// <summary>
        /// Write GUID array.
        /// </summary>
        /// <param name="vals">Values.</param>
        /// <param name="stream">Stream.</param>
        public static void WriteGuidArray(Guid?[] vals, IBinaryStream stream)
        {
            stream.WriteInt(vals.Length);

            foreach (Guid? val in vals)
            {
                if (val.HasValue)
                {
                    stream.WriteByte(TypeGuid);

                    WriteGuid(val.Value, stream);
                }
                else
                    stream.WriteByte(HdrNull);
            }
        }

        /**
         * <summary>Read GUID array.</summary>
         * <param name="stream">Stream.</param>
         * <returns>GUID array.</returns>
         */
        public static Guid?[] ReadGuidArray(IBinaryStream stream)
        {
            int len = stream.ReadInt();

            Guid?[] vals = new Guid?[len];

            for (int i = 0; i < len; i++)
                vals[i] = ReadGuid(stream);

            return vals;
        }

        /// <summary>
        /// Write array.
        /// </summary>
        /// <param name="val">Array.</param>
        /// <param name="ctx">Write context.</param>
        /// <param name="elementType">Type of the array element.</param>
        public static void WriteArray(Array val, BinaryWriter ctx, int elementType = ObjTypeId)
        {
            Debug.Assert(val != null && ctx != null);

            IBinaryStream stream = ctx.Stream;

            stream.WriteInt(elementType);

            stream.WriteInt(val.Length);

            for (int i = 0; i < val.Length; i++)
                ctx.Write(val.GetValue(i));
        }

        /// <summary>
        /// Read array.
        /// </summary>
        /// <param name="ctx">Read context.</param>
        /// <param name="typed">Typed flag.</param>
        /// <param name="elementType">Type of the element.</param>
        /// <returns>Array.</returns>
        public static object ReadTypedArray(BinaryReader ctx, bool typed, Type elementType)
        {
            Func<BinaryReader, bool, object> result;

            if (!ArrayReaders.TryGetValue(elementType, out result))
                result = ArrayReaders.GetOrAdd(elementType, t =>
                    DelegateConverter.CompileFunc<Func<BinaryReader, bool, object>>(null,
                        MtdhReadArray.MakeGenericMethod(t),
                        new[] {typeof (BinaryReader), typeof (bool)}, new[] {false, false, true}));

            return result(ctx, typed);
        }

        /// <summary>
        /// Read array.
        /// </summary>
        /// <param name="ctx">Read context.</param>
        /// <param name="typed">Typed flag.</param>
        /// <returns>Array.</returns>
        public static T[] ReadArray<T>(BinaryReader ctx, bool typed)
        {
            var stream = ctx.Stream;

            var pos = stream.Position;

            if (typed)
                stream.ReadInt();

            int len = stream.ReadInt();

            var vals = new T[len];

            ctx.AddHandle(pos - 1, vals);

            for (int i = 0; i < len; i++)
                vals[i] = ctx.Deserialize<T>();

            return vals;
        }

        /// <summary>
        /// Read timestamp array.
        /// </summary>
        /// <param name="stream">Stream.</param>
        /// <returns>Timestamp array.</returns>
        public static DateTime?[] ReadTimestampArray(IBinaryStream stream)
        {
            int len = stream.ReadInt();

            DateTime?[] vals = new DateTime?[len];

            for (int i = 0; i < len; i++)
                vals[i] = stream.ReadByte() == HdrNull ? null : ReadTimestamp(stream);

            return vals;
        }

        /**
         * <summary>Write collection.</summary>
         * <param name="val">Value.</param>
         * <param name="ctx">Write context.</param>
         */
        public static void WriteCollection(ICollection val, BinaryWriter ctx)
        {
            var valType = val.GetType();
            
            byte colType;

            if (valType.IsGenericType)
            {
                var genType = valType.GetGenericTypeDefinition();

                if (genType == typeof (List<>))
                    colType = CollectionArrayList;
                else if (genType == typeof (LinkedList<>))
                    colType = CollectionLinkedList;
                else
                    colType = CollectionCustom;
            }
            else
                colType = valType == typeof (ArrayList) ? CollectionArrayList : CollectionCustom;

            WriteCollection(val, ctx, colType);
        }

        /**
         * <summary>Write non-null collection with known type.</summary>
         * <param name="val">Value.</param>
         * <param name="ctx">Write context.</param>
         * <param name="colType">Collection type.</param>
         */
        public static void WriteCollection(ICollection val, BinaryWriter ctx, byte colType)
        {
            ctx.Stream.WriteInt(val.Count);

            ctx.Stream.WriteByte(colType);

            foreach (object elem in val)
                ctx.Write(elem);
        }

        /**
         * <summary>Read collection.</summary>
         * <param name="ctx">Context.</param>
         * <param name="factory">Factory delegate.</param>
         * <param name="adder">Adder delegate.</param>
         * <returns>Collection.</returns>
         */
        public static ICollection ReadCollection(BinaryReader ctx,
            Func<int, ICollection> factory, Action<ICollection, object> adder)
        {
            IBinaryStream stream = ctx.Stream;

            int pos = stream.Position;

            int len = stream.ReadInt();

            byte colType = ctx.Stream.ReadByte();

            ICollection res;

            if (factory == null)
            {
                if (colType == CollectionLinkedList)
                    res = new LinkedList<object>();
                else
                    res = new ArrayList(len);
            }
            else
                res = factory.Invoke(len);

            ctx.AddHandle(pos - 1, res);

            if (adder == null)
                adder = (col, elem) => ((ArrayList) col).Add(elem);

            for (int i = 0; i < len; i++)
                adder.Invoke(res, ctx.Deserialize<object>());

            return res;
        }

        /**
         * <summary>Write dictionary.</summary>
         * <param name="val">Value.</param>
         * <param name="ctx">Write context.</param>
         */
        public static void WriteDictionary(IDictionary val, BinaryWriter ctx)
        {
            var valType = val.GetType();

            byte dictType;

            if (valType.IsGenericType)
            {
                var genType = valType.GetGenericTypeDefinition();

                dictType = genType == typeof (Dictionary<,>) ? MapHashMap : MapCustom;
            }
            else
                dictType = valType == typeof (Hashtable) ? MapHashMap : MapCustom;

            WriteDictionary(val, ctx, dictType);
        }

        /**
         * <summary>Write non-null dictionary with known type.</summary>
         * <param name="val">Value.</param>
         * <param name="ctx">Write context.</param>
         * <param name="dictType">Dictionary type.</param>
         */
        public static void WriteDictionary(IDictionary val, BinaryWriter ctx, byte dictType)
        {
            ctx.Stream.WriteInt(val.Count);

            ctx.Stream.WriteByte(dictType);

            foreach (DictionaryEntry entry in val)
            {
                ctx.Write(entry.Key);
                ctx.Write(entry.Value);
            }
        }

        /**
         * <summary>Read dictionary.</summary>
         * <param name="ctx">Context.</param>
         * <param name="factory">Factory delegate.</param>
         * <returns>Dictionary.</returns>
         */
        public static IDictionary ReadDictionary(BinaryReader ctx, Func<int, IDictionary> factory)
        {
            IBinaryStream stream = ctx.Stream;

            int pos = stream.Position;

            int len = stream.ReadInt();

            // Skip dictionary type as we can do nothing with it here.
            ctx.Stream.ReadByte();

            var res = factory == null ? new Hashtable(len) : factory.Invoke(len);

            ctx.AddHandle(pos - 1, res);

            for (int i = 0; i < len; i++)
            {
                object key = ctx.Deserialize<object>();
                object val = ctx.Deserialize<object>();

                res[key] = val;
            }

            return res;
        }

        /**
         * <summary>Write binary object.</summary>
         * <param name="stream">Stream.</param>
         * <param name="val">Value.</param>
         */
        public static void WriteBinary(IBinaryStream stream, BinaryObject val)
        {
            WriteByteArray(val.Data, stream);

            stream.WriteInt(val.Offset);
        }

        /// <summary>
        /// Write enum.
        /// </summary>
        /// <param name="writer">Writer.</param>
        /// <param name="val">Value.</param>
        public static void WriteEnum<T>(BinaryWriter writer, T val)
        {
            writer.WriteInt(GetEnumTypeId(val.GetType(), writer.Marshaller));
            writer.WriteInt(TypeCaster<int>.Cast(val));
        }

        /// <summary>
        /// Gets the enum type identifier.
        /// </summary>
        /// <param name="enumType">The enum type.</param>
        /// <param name="marshaller">The marshaller.</param>
        /// <returns>Enum type id.</returns>
        public static int GetEnumTypeId(Type enumType, Marshaller marshaller)
        {
            if (Enum.GetUnderlyingType(enumType) == TypInt)
            {
                var desc = marshaller.GetDescriptor(enumType);

                return desc == null ? ObjTypeId : desc.TypeId;
            }

            throw new BinaryObjectException("Only Int32 underlying type is supported for enums: " +
                                            enumType.Name);
        }

        /// <summary>
        /// Gets the enum value by type id and int representation.
        /// </summary>
        /// <typeparam name="T">Result type.</typeparam>
        /// <param name="value">The value.</param>
        /// <param name="typeId">The type identifier.</param>
        /// <param name="marsh">The marshaller.</param>
        /// <returns>value in form of enum, if typeId is known; value in for of int, if typeId is -1.</returns>
        public static T GetEnumValue<T>(int value, int typeId, Marshaller marsh)
        {
            if (typeId == ObjTypeId)
                return TypeCaster<T>.Cast(value);

            // All enums are user types
            var desc = marsh.GetDescriptor(true, typeId);

            if (desc == null || desc.Type == null)
                throw new BinaryObjectException("Unknown enum type id: " + typeId);

            return (T)Enum.ToObject(desc.Type, value);
        }

        /**
         * <summary>Gets type key.</summary>
         * <param name="userType">User type flag.</param>
         * <param name="typeId">Type ID.</param>
         * <returns>Type key.</returns>
         */
        public static long TypeKey(bool userType, int typeId)
        {
            long res = typeId;

            if (userType)
                res |= (long)1 << 32;

            return res;
        }

        /**
         * <summary>Get string hash code.</summary>
         * <param name="val">Value.</param>
         * <returns>Hash code.</returns>
         */
        public static int GetStringHashCode(string val)
        {
            if (val == null)
                return 0;

            int hash = 0;

            unchecked
            {
                // ReSharper disable once LoopCanBeConvertedToQuery (performance)
                foreach (var c in val)
                    hash = 31 * hash + ('A' <= c && c <= 'Z' ? c | 0x20 : c);
            }

            return hash;
        }

        public static string CleanFieldName(string fieldName)
        {
            if (fieldName.StartsWith("<", StringComparison.Ordinal)
                && fieldName.EndsWith(">k__BackingField", StringComparison.Ordinal))
                return fieldName.Substring(1, fieldName.IndexOf(">", StringComparison.Ordinal) - 1);
            
            return fieldName;
        }

        /**
         * <summary>Convert type name.</summary>
         * <param name="typeName">Type name.</param>
         * <param name="converter">Converter.</param>
         * <returns>Converted name.</returns>
         */
        public static string ConvertTypeName(string typeName, IBinaryNameMapper converter)
        {
            var typeName0 = typeName;

            try
            {
                if (converter != null)
                    typeName = converter.GetTypeName(typeName);
            }
            catch (Exception e)
            {
                throw new BinaryObjectException("Failed to convert type name due to converter exception " +
                    "[typeName=" + typeName + ", converter=" + converter + ']', e);
            }

            if (typeName == null)
                throw new BinaryObjectException("Name converter returned null name for type [typeName=" +
                    typeName0 + ", converter=" + converter + "]");

            return typeName;
        }

        /**
         * <summary>Convert field name.</summary>
         * <param name="fieldName">Field name.</param>
         * <param name="converter">Converter.</param>
         * <returns>Converted name.</returns>
         */
        public static string ConvertFieldName(string fieldName, IBinaryNameMapper converter)
        {
            var fieldName0 = fieldName;

            try
            {
                if (converter != null)
                    fieldName = converter.GetFieldName(fieldName);
            }
            catch (Exception e)
            {
                throw new BinaryObjectException("Failed to convert field name due to converter exception " +
                    "[fieldName=" + fieldName + ", converter=" + converter + ']', e);
            }

            if (fieldName == null)
                throw new BinaryObjectException("Name converter returned null name for field [fieldName=" +
                    fieldName0 + ", converter=" + converter + "]");

            return fieldName;
        }

        /**
         * <summary>Extract simple type name.</summary>
         * <param name="typeName">Type name.</param>
         * <returns>Simple type name.</returns>
         */
        public static string SimpleTypeName(string typeName)
        {
            int idx = typeName.LastIndexOf('.');

            return idx < 0 ? typeName : typeName.Substring(idx + 1);
        }

        /**
         * <summary>Resolve type ID.</summary>
         * <param name="typeName">Type name.</param>
         * <param name="nameMapper">Name mapper.</param>
         * <param name="idMapper">ID mapper.</param>
         */
        public static int TypeId(string typeName, IBinaryNameMapper nameMapper,
            IBinaryIdMapper idMapper)
        {
            Debug.Assert(typeName != null);

            typeName = ConvertTypeName(typeName, nameMapper);

            int id = 0;

            if (idMapper != null)
            {
                try
                {
                    id = idMapper.GetTypeId(typeName);
                }
                catch (Exception e)
                {
                    throw new BinaryObjectException("Failed to resolve type ID due to ID mapper exception " +
                        "[typeName=" + typeName + ", idMapper=" + idMapper + ']', e);
                }
            }

            if (id == 0)
                id = GetStringHashCode(typeName);

            return id;
        }

        /// <summary>
        /// Gets the name of the type.
        /// </summary>
        /// <param name="type">The type.</param>
        /// <returns>
        /// Simple type name for non-generic types; simple type name with appended generic arguments for generic types.
        /// </returns>
        public static string GetTypeName(Type type)
        {
            if (!type.IsGenericType)
                return type.Name;

            var args = type.GetGenericArguments().Select(GetTypeName).Aggregate((x, y) => x + "," + y);

            return string.Format(CultureInfo.InvariantCulture, "{0}[{1}]", type.Name, args);
        }

        /**
         * <summary>Resolve field ID.</summary>
         * <param name="typeId">Type ID.</param>
         * <param name="fieldName">Field name.</param>
         * <param name="nameMapper">Name mapper.</param>
         * <param name="idMapper">ID mapper.</param>
         */
        public static int FieldId(int typeId, string fieldName, IBinaryNameMapper nameMapper,
            IBinaryIdMapper idMapper)
        {
            Debug.Assert(typeId != 0);
            Debug.Assert(fieldName != null);

            fieldName = ConvertFieldName(fieldName, nameMapper);

            int id = 0;

            if (idMapper != null)
            {
                try
                {
                    id = idMapper.GetFieldId(typeId, fieldName);
                }
                catch (Exception e)
                {
                    throw new BinaryObjectException("Failed to resolve field ID due to ID mapper exception " +
                        "[typeId=" + typeId + ", fieldName=" + fieldName + ", idMapper=" + idMapper + ']', e);
                }
            }

            if (id == 0)
                id = GetStringHashCode(fieldName);

            if (id == 0)
                throw new BinaryObjectException("Field ID is zero (please provide ID mapper or change field name) " + 
                    "[typeId=" + typeId + ", fieldName=" + fieldName + ", idMapper=" + idMapper + ']');

            return id;
        }

        /// <summary>
        /// Compare contents of two byte array chunks.
        /// </summary>
        /// <param name="arr1">Array 1.</param>
        /// <param name="offset1">Offset 1.</param>
        /// <param name="len1">Length 1.</param>
        /// <param name="arr2">Array 2.</param>
        /// <param name="offset2">Offset 2.</param>
        /// <param name="len2">Length 2.</param>
        /// <returns>True if array chunks are equal.</returns>
        public static bool CompareArrays(byte[] arr1, int offset1, int len1, byte[] arr2, int offset2, int len2)
        {
            if (len1 == len2)
            {
                for (int i = 0; i < len1; i++)
                {
                    if (arr1[offset1 + i] != arr2[offset2 + i])
                        return false;
                }

                return true;
            }
            return false;
        }

        /// <summary>
        /// Writes invocation result.
        /// </summary>
        /// <param name="writer">Writer.</param>
        /// <param name="success">Success flag.</param>
        /// <param name="res">Result.</param>
        [SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes")]
        public static void WriteInvocationResult(BinaryWriter writer, bool success, object res)
        {
            var pos = writer.Stream.Position;

            try
            {
                if (success)
                    writer.WriteBoolean(true);
                else
                {
                    writer.WriteBoolean(false); // Call failed.
                    writer.WriteBoolean(true); // Exception serialized sucessfully.
                }

                writer.Write(res);
            }
            catch (Exception marshErr)
            {
                // Failed to serialize result, fallback to plain string.
                writer.Stream.Seek(pos, SeekOrigin.Begin);

                writer.WriteBoolean(false); // Call failed.
                writer.WriteBoolean(false); // Cannot serialize result or exception.

                if (success)
                {
                    writer.WriteString("Call completed successfully, but result serialization failed [resultType=" +
                        res.GetType().Name + ", serializationErrMsg=" + marshErr.Message + ']');
                }
                else
                {
                    writer.WriteString("Call completed with error, but error serialization failed [errType=" +
                        res.GetType().Name + ", serializationErrMsg=" + marshErr.Message + ']');
                }
            }
        }

        /// <summary>
        /// Reads invocation result.
        /// </summary>
        /// <param name="reader">Reader.</param>
        /// <param name="err">Error.</param>
        /// <returns>Result.</returns>
        public static object ReadInvocationResult(BinaryReader reader, out object err)
        {
            err = null;

            if (reader.ReadBoolean())
                return reader.ReadObject<object>();

            err = reader.ReadBoolean()
                ? reader.ReadObject<object>()
                : ExceptionUtils.GetException(reader.Marshaller.Ignite, reader.ReadString(), reader.ReadString(),
                                              reader.ReadString());

            return null;
        }

        /// <summary>
        /// Validate protocol version.
        /// </summary>
        /// <param name="version">The version.</param>
        public static void ValidateProtocolVersion(byte version)
        {
            if (version != ProtoVer)
                throw new BinaryObjectException("Unsupported protocol version: " + version);
        }

        /**
         * <summary>Convert date to Java ticks.</summary>
         * <param name="date">Date</param>
         * <param name="high">High part (milliseconds).</param>
         * <param name="low">Low part (nanoseconds)</param>
         */
        private static void ToJavaDate(DateTime date, out long high, out int low)
        {
            if (date.Kind != DateTimeKind.Utc)
                throw new InvalidOperationException(
                    "DateTime is not UTC. Only UTC DateTime can be used for interop with other platforms.");

            long diff = date.Ticks - JavaDateTicks;

            high = diff / TimeSpan.TicksPerMillisecond;

            low = (int)(diff % TimeSpan.TicksPerMillisecond) * 100; 
        }

        /// <summary>
        /// Read additional configuration from the stream.
        /// </summary>
        /// <param name="reader">Reader.</param>
        /// <param name="assemblies">Assemblies.</param>
        /// <param name="cfg">Configuration.</param>
        public static void ReadConfiguration(BinaryReader reader, out ICollection<string> assemblies, out BinaryConfiguration cfg)
        {
            if (reader.ReadBoolean())
            {
                int assemblyCnt = reader.ReadInt();

                assemblies = new List<string>(assemblyCnt);

                for (int i = 0; i < assemblyCnt; i++)
                    assemblies.Add(reader.ReadObject<string>());
            }
            else
                assemblies = null;

            if (reader.ReadBoolean())
            {
                cfg = new BinaryConfiguration();

                // Read binary types in full form.
                if (reader.ReadBoolean())
                {
                    int typesCnt = reader.ReadInt();

                    cfg.TypeConfigurations = new List<BinaryTypeConfiguration>();

                    for (int i = 0; i < typesCnt; i++)
                    {
                        cfg.TypeConfigurations.Add(new BinaryTypeConfiguration
                        {
                            TypeName = reader.ReadString(),
                            NameMapper = CreateInstance<IBinaryNameMapper>(reader),
                            IdMapper = CreateInstance<IBinaryIdMapper>(reader),
                            Serializer = CreateInstance<IBinarySerializer>(reader),
                            AffinityKeyFieldName = reader.ReadString(),
                            KeepDeserialized = reader.ReadObject<bool?>(),
                            IsEnum = reader.ReadBoolean()
                        });
                    }
                }

                // Read binary types in compact form.
                if (reader.ReadBoolean())
                {
                    int typesCnt = reader.ReadInt();

                    cfg.Types = new List<string>(typesCnt);

                    for (int i = 0; i < typesCnt; i++)
                        cfg.Types.Add(reader.ReadString());
                }

                // Read the rest.
                cfg.DefaultNameMapper = CreateInstance<IBinaryNameMapper>(reader);
                cfg.DefaultIdMapper = CreateInstance<IBinaryIdMapper>(reader);
                cfg.DefaultSerializer = CreateInstance<IBinarySerializer>(reader);
                cfg.DefaultKeepDeserialized = reader.ReadBoolean();
            }
            else
                cfg = null;
        }

        /// <summary>
        /// Gets the unsupported type exception.
        /// </summary>
        public static BinaryObjectException GetUnsupportedTypeException(Type type, object obj)
        {
            return new BinaryObjectException(string.Format(
                "Unsupported object type [type={0}, object={1}].\nSpecified type " +
                "can not be serialized by Ignite: it is neither [Serializable], " +
                "nor registered in IgniteConfiguration.BinaryConfiguration." +
                "\nSee https://apacheignite-net.readme.io/docs/serialization for more details.", type, obj));
        }

        /// <summary>
        /// Reinterprets int bits as a float.
        /// </summary>
        public static unsafe float IntToFloatBits(int val)
        {
            return *(float*) &val;
        }

        /// <summary>
        /// Reinterprets long bits as a double.
        /// </summary>
        public static unsafe double LongToDoubleBits(long val)
        {
            return *(double*) &val;
        }

        /// <summary>
        /// Creates and instance from the type name in reader.
        /// </summary>
        private static T CreateInstance<T>(BinaryReader reader)
        {
            var typeName = reader.ReadString();

            if (typeName == null)
                return default(T);

            return IgniteUtils.CreateInstance<T>(typeName);
        }

        /// <summary>
        /// Reverses the byte order of an unsigned long.
        /// </summary>
        private static ulong ReverseByteOrder(ulong l)
        {
            // Fastest way would be to use bswap processor instruction.
            return ((l >> 56) & 0x00000000000000FF) | ((l >> 40) & 0x000000000000FF00) |
                   ((l >> 24) & 0x0000000000FF0000) | ((l >> 8) & 0x00000000FF000000) |
                   ((l << 8) & 0x000000FF00000000) | ((l << 24) & 0x0000FF0000000000) |
                   ((l << 40) & 0x00FF000000000000) | ((l << 56) & 0xFF00000000000000);
        }

        /// <summary>
        /// Struct with .Net-style Guid memory layout.
        /// </summary>
        [StructLayout(LayoutKind.Sequential, Pack = 0)]
        private struct GuidAccessor
        {
            public readonly ulong ABC;
            public readonly ulong DEFGHIJK;

            /// <summary>
            /// Initializes a new instance of the <see cref="GuidAccessor"/> struct.
            /// </summary>
            /// <param name="val">The value.</param>
            public GuidAccessor(JavaGuid val)
            {
                var l = val.CBA;

                if (BitConverter.IsLittleEndian)
                    ABC = ((l >> 32) & 0x00000000FFFFFFFF) | ((l << 48) & 0xFFFF000000000000) |
                          ((l << 16) & 0x0000FFFF00000000);
                else
                    ABC = ((l << 32) & 0xFFFFFFFF00000000) | ((l >> 48) & 0x000000000000FFFF) |
                          ((l >> 16) & 0x00000000FFFF0000);

                // This is valid in any endianness (symmetrical)
                DEFGHIJK = ReverseByteOrder(val.KJIHGFED);
            }
        }

        /// <summary>
        /// Struct with Java-style Guid memory layout.
        /// </summary>
        [StructLayout(LayoutKind.Explicit)]
        private struct JavaGuid
        {
            [FieldOffset(0)] public readonly ulong CBA;
            [FieldOffset(8)] public readonly ulong KJIHGFED;
            [SuppressMessage("Microsoft.Performance", "CA1823:AvoidUnusedPrivateFields")]
            [FieldOffset(0)] public unsafe fixed byte Bytes [16];

            /// <summary>
            /// Initializes a new instance of the <see cref="JavaGuid"/> struct.
            /// </summary>
            /// <param name="val">The value.</param>
            public unsafe JavaGuid(Guid val)
            {
                // .Net returns bytes in the following order: _a(4), _b(2), _c(2), _d, _e, _f, _g, _h, _i, _j, _k.
                // And _a, _b and _c are always in little endian format irrespective of system configuration.
                // To be compliant with Java we rearrange them as follows: _c, _b_, a_, _k, _j, _i, _h, _g, _f, _e, _d.
                var accessor = *((GuidAccessor*)&val);

                var l = accessor.ABC;

                if (BitConverter.IsLittleEndian)
                    CBA = ((l << 32) & 0xFFFFFFFF00000000) | ((l >> 48) & 0x000000000000FFFF) |
                          ((l >> 16) & 0x00000000FFFF0000);
                else
                    CBA = ((l >> 32) & 0x00000000FFFFFFFF) | ((l << 48) & 0xFFFF000000000000) |
                          ((l << 16) & 0x0000FFFF00000000);

                // This is valid in any endianness (symmetrical)
                KJIHGFED = ReverseByteOrder(accessor.DEFGHIJK);
            }
        }
    }
}
