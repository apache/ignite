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
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.IO;
    using System.Reflection;
    using System.Runtime.Serialization.Formatters.Binary;
    using System.Text;
    using Apache.Ignite.Core.Impl.Common;
    using Apache.Ignite.Core.Impl.Portable.IO;
    using Apache.Ignite.Core.Portable;

    /**
     * <summary>Utilities for portable serialization.</summary>
     */
    static class PortableUtils
    {
        /** Cache empty dictionary. */
        public static readonly IDictionary<int, int> EmptyFields = new Dictionary<int, int>();

        /** Header of NULL object. */
        public const byte HdrNull = 101;

        /** Header of object handle. */
        public const byte HdrHnd = 102;

        /** Header of object in fully serialized form. */
        public const byte HdrFull = 103;
        
        /** Full header length. */
        public const int FullHdrLen = 18;

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
        public const byte TypeDate = 11;

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
        public const byte TypeArrayDate = 22;

        /** Type: object array. */
        public const byte TypeArray = 23;

        /** Type: collection. */
        public const byte TypeCollection = 24;

        /** Type: map. */
        public const byte TypeDictionary = 25;

        /** Type: map entry. */
        public const byte TypeMapEntry = 26;

        /** Type: portable object. */
        public const byte TypePortable = 27;

        /** Type: enum. */
        public const byte TypeEnum = 28;

        /** Type: enum array. */
        public const byte TypeArrayEnum = 29;
        
        /** Type: native job holder. */
        public const byte TypeNativeJobHolder = 77;

        /** Type: native job result holder. */
        public const byte TypePortableJobResHolder = 76;

        /** Type: .Net configuration. */
        public const byte TypeDotNetCfg = 202;

        /** Type: .Net portable configuration. */
        public const byte TypeDotNetPortableCfg = 203;

        /** Type: .Net portable type configuration. */
        public const byte TypeDotNetPortableTypCfg = 204;

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

        /** Type: Compute job wrapper. */
        public const byte TypeSerializableHolder = 87;

        /** Type: action wrapper. */
        public const byte TypeComputeActionJob = 88;

        /** Type: entry processor holder. */
        public const byte TypeCacheEntryProcessorHolder = 89;

        /** Type: entry predicate holder. */
        public const byte TypeCacheEntryPredicateHolder = 90;
        
        /** Type: product license. */
        public const byte TypeProductLicense = 78;

        /** Type: message filter holder. */
        public const byte TypeMessageFilterHolder = 92;

        /** Type: message filter holder. */
        public const byte TypePortableOrSerializableHolder = 93;

        /** Type: stream receiver holder. */
        public const byte TypeStreamReceiverHolder = 94;

        /** Collection: custom. */
        public const byte CollectionCustom = 0;

        /** Collection: array list. */
        public const byte CollectionArrayList = 1;

        /** Collection: linked list. */
        public const byte CollectionLinkedList = 2;

        /** Collection: hash set. */
        public const byte CollectionHashSet = 3;

        /** Collection: hash set. */
        public const byte CollectionLinkedHashSet = 4;

        /** Collection: sorted set. */
        public const byte CollectionSortedSet = 5;

        /** Collection: concurrent bag. */
        public const byte CollectionConcurrentBag = 6;

        /** Map: custom. */
        public const byte MapCustom = 0;

        /** Map: hash map. */
        public const byte MapHashMap = 1;

        /** Map: linked hash map. */
        public const byte MapLinkedHashMap = 2;

        /** Map: sorted map. */
        public const byte MapSortedMap = 3;

        /** Map: concurrent hash map. */
        public const byte MapConcurrentHashMap = 4;

        /** Byte "0". */
        public const byte ByteZero = 0;

        /** Byte "1". */
        public const byte ByteOne = 1;

        /** Indicates object array. */
        public const int ObjTypeId = -1;

        /** Int type. */
        public static readonly Type TypInt = typeof(int);

        /** Collection type. */
        public static readonly Type TypCollection = typeof(ICollection);

        /** Dictionary type. */
        public static readonly Type TypDictionary = typeof(IDictionary);

        /** Generic collection type. */
        public static readonly Type TypGenericCollection = typeof(ICollection<>);

        /** Generic dictionary type. */
        public static readonly Type TypGenericDictionary = typeof(IDictionary<,>);

        /** Ticks for Java epoch. */
        private static readonly long JavaDateTicks = new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc).Ticks;
        
        /** Bindig flags for static search. */
        private static BindingFlags _bindFlagsStatic = 
            BindingFlags.Static | BindingFlags.Public | BindingFlags.NonPublic;

        /** Default poratble marshaller. */
        private static readonly PortableMarshaller Marsh = new PortableMarshaller(null);

        /** Method: WriteGenericCollection. */
        public static readonly MethodInfo MtdhWriteGenericCollection =
            typeof(PortableUtils).GetMethod("WriteGenericCollection", _bindFlagsStatic);

        /** Method: ReadGenericCollection. */
        public static readonly MethodInfo MtdhReadGenericCollection =
            typeof(PortableUtils).GetMethod("ReadGenericCollection", _bindFlagsStatic);

        /** Method: WriteGenericDictionary. */
        public static readonly MethodInfo MtdhWriteGenericDictionary =
            typeof(PortableUtils).GetMethod("WriteGenericDictionary", _bindFlagsStatic);

        /** Method: ReadGenericDictionary. */
        public static readonly MethodInfo MtdhReadGenericDictionary =
            typeof(PortableUtils).GetMethod("ReadGenericDictionary", _bindFlagsStatic);

        /** Method: ReadGenericArray. */
        public static readonly MethodInfo MtdhReadGenericArray =
            typeof(PortableUtils).GetMethod("ReadGenericArray", _bindFlagsStatic);

        /** Cached UTF8 encoding. */
        private static readonly Encoding Utf8 = Encoding.UTF8;

        /** Cached generic array read funcs. */
        private static readonly CopyOnWriteConcurrentDictionary<Type, Func<PortableReaderImpl, bool, object>>
            ArrayReaders = new CopyOnWriteConcurrentDictionary<Type, Func<PortableReaderImpl, bool, object>>();

        /// <summary>
        /// Default marshaller.
        /// </summary>
        public static PortableMarshaller Marshaller
        {
            get { return Marsh; }
        }

        /**
         * <summary>Write boolean array.</summary>
         * <param name="vals">Value.</param>
         * <param name="stream">Output stream.</param>
         */
        public static void WriteBooleanArray(bool[] vals, IPortableStream stream)
        {
            stream.WriteInt(vals.Length);

            stream.WriteBoolArray(vals);
        }

        /**
         * <summary>Read boolean array.</summary>
         * <param name="stream">Output stream.</param>
         * <returns>Value.</returns>
         */
        public static bool[] ReadBooleanArray(IPortableStream stream)
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
        public static void WriteByteArray(byte[] vals, IPortableStream stream)
        {
            stream.WriteInt(vals.Length);

            stream.WriteByteArray(vals);
        }

        /**
         * <summary>Read byte array.</summary>
         * <param name="stream">Output stream.</param>
         * <returns>Value.</returns>
         */
        public static byte[] ReadByteArray(IPortableStream stream)
        {
            return stream.ReadByteArray(stream.ReadInt());
        }

        /**
         * <summary>Read byte array.</summary>
         * <param name="stream">Output stream.</param>
         * <returns>Value.</returns>
         */
        public static unsafe sbyte[] ReadSbyteArray(IPortableStream stream)
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
        public static void WriteShortArray(short[] vals, IPortableStream stream)
        {
            stream.WriteInt(vals.Length);

            stream.WriteShortArray(vals);
        }

        /**
         * <summary>Read short array.</summary>
         * <param name="stream">Stream.</param>
         * <returns>Value.</returns>
         */
        public static unsafe ushort[] ReadUshortArray(IPortableStream stream)
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
        public static short[] ReadShortArray(IPortableStream stream)
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
        public static void WriteIntArray(int[] vals, IPortableStream stream)
        {
            stream.WriteInt(vals.Length);

            stream.WriteIntArray(vals);
        }

        /**
         * <summary>Read int array.</summary>
         * <param name="stream">Stream.</param>
         * <returns>Value.</returns>
         */
        public static int[] ReadIntArray(IPortableStream stream)
        {
            return stream.ReadIntArray(stream.ReadInt());
        }

        /**
         * <summary>Read int array.</summary>
         * <param name="stream">Stream.</param>
         * <returns>Value.</returns>
         */
        public static unsafe uint[] ReadUintArray(IPortableStream stream)
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
        public static void WriteLongArray(long[] vals, IPortableStream stream)
        {
            stream.WriteInt(vals.Length);

            stream.WriteLongArray(vals);
        }

        /**
         * <summary>Read long array.</summary>
         * <param name="stream">Stream.</param>
         * <returns>Value.</returns>
         */
        public static long[] ReadLongArray(IPortableStream stream)
        {
            return stream.ReadLongArray(stream.ReadInt());
        }

        /**
         * <summary>Read ulong array.</summary>
         * <param name="stream">Stream.</param>
         * <returns>Value.</returns>
         */
        public static unsafe ulong[] ReadUlongArray(IPortableStream stream)
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
        public static void WriteCharArray(char[] vals, IPortableStream stream)
        {
            stream.WriteInt(vals.Length);

            stream.WriteCharArray(vals);
        }

        /**
         * <summary>Read char array.</summary>
         * <param name="stream">Stream.</param>
         * <returns>Value.</returns>
         */
        public static char[] ReadCharArray(IPortableStream stream)
        {
            int len = stream.ReadInt();

            return stream.ReadCharArray(len);
        }

        /**
         * <summary>Write float array.</summary>
         * <param name="vals">Value.</param>
         * <param name="stream">Output stream.</param>
         */
        public static void WriteFloatArray(float[] vals, IPortableStream stream)
        {
            stream.WriteInt(vals.Length);

            stream.WriteFloatArray(vals);
        }

        /**
         * <summary>Read float array.</summary>
         * <param name="stream">Stream.</param>
         * <returns>Value.</returns>
         */
        public static float[] ReadFloatArray(IPortableStream stream)
        {
            int len = stream.ReadInt();

            return stream.ReadFloatArray(len);
        }

        /**
         * <summary>Write double array.</summary>
         * <param name="vals">Value.</param>
         * <param name="stream">Output stream.</param>
         */
        public static void WriteDoubleArray(double[] vals, IPortableStream stream)
        {
            stream.WriteInt(vals.Length);

            stream.WriteDoubleArray(vals);
        }

        /**
         * <summary>Read double array.</summary>
         * <param name="stream">Stream.</param>
         * <returns>Value.</returns>
         */
        public static double[] ReadDoubleArray(IPortableStream stream)
        {
            int len = stream.ReadInt();

            return stream.ReadDoubleArray(len);
        }

        /**
         * <summary>Write date.</summary>
         * <param name="val">Date.</param>
         * <param name="stream">Stream.</param>
         */
        public static void WriteDate(DateTime? val, IPortableStream stream)
        {
            long high;
            int low;

            Debug.Assert(val.HasValue);
            ToJavaDate(val.Value, out high, out low);

            stream.WriteLong(high);
            stream.WriteInt(low);
        }

        /**
         * <summary>Read date.</summary>
         * <param name="stream">Stream.</param>
         * <param name="local">Local flag.</param>
         * <returns>Date</returns>
         */
        public static DateTime? ReadDate(IPortableStream stream, bool local)
        {
            long high = stream.ReadLong();
            int low = stream.ReadInt();

            return ToDotNetDate(high, low, local);
        }

        /**
         * <summary>Write date array.</summary>
         * <param name="vals">Date array.</param>
         * <param name="stream">Stream.</param>
         */
        public static void WriteDateArray(DateTime?[] vals, IPortableStream stream)
        {
            stream.WriteInt(vals.Length);

            foreach (DateTime? val in vals)
            {
                if (val.HasValue)
                    PortableSystemHandlers.WriteHndDateTyped(stream, val);
                else
                    stream.WriteByte(HdrNull);
            }
        }
        
        /**
         * <summary>Write string in UTF8 encoding.</summary>
         * <param name="val">String.</param>
         * <param name="stream">Stream.</param>
         */
        public static unsafe void WriteString(string val, IPortableStream stream)
        {
            stream.WriteBool(true);

            int charCnt = val.Length;

            fixed (char* chars = val)
            {
                int byteCnt = Utf8.GetByteCount(chars, charCnt);

                stream.WriteInt(byteCnt);

                stream.WriteString(chars, charCnt, byteCnt, Utf8);
            }
        }

        /**
         * <summary>Read string in UTF8 encoding.</summary>
         * <param name="stream">Stream.</param>
         * <returns>String.</returns>
         */
        public static string ReadString(IPortableStream stream)
        {
            if (stream.ReadBool())
            {
                byte[] bytes = ReadByteArray(stream);

                return bytes != null ? Utf8.GetString(bytes) : null;
            }
            
            char[] chars = ReadCharArray(stream);

            return new string(chars);
        }

        /**
         * <summary>Write string array in UTF8 encoding.</summary>
         * <param name="vals">String array.</param>
         * <param name="stream">Stream.</param>
         */
        public static void WriteStringArray(string[] vals, IPortableStream stream)
        {
            stream.WriteInt(vals.Length);

            foreach (string val in vals)
            {
                if (val != null)
                    PortableSystemHandlers.WriteHndStringTyped(stream, val); 
                else
                    stream.WriteByte(HdrNull);
            }
        }

        /**
         * <summary>Read string array in UTF8 encoding.</summary>
         * <param name="stream">Stream.</param>
         * <returns>String array.</returns>
         */
        public static string[] ReadStringArray(IPortableStream stream)
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
        public static void WriteDecimal(decimal val, IPortableStream stream) 
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
        public static decimal ReadDecimal(IPortableStream stream)
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
                throw new PortableException("Decimal value scale overflow (must be between 0 and 28): " + scale);

            if (mag.Length > 13)
                throw new PortableException("Decimal magnitude overflow (must be less than 96 bits): " + 
                    mag.Length * 8);

            if (mag.Length == 13 && mag[0] != 0)
                throw new PortableException("Decimal magnitude overflow (must be less than 96 bits): " +
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
        public static void WriteDecimalArray(decimal[] vals, IPortableStream stream)
        {
            stream.WriteInt(vals.Length);

            foreach (decimal val in vals)
                WriteDecimal(val, stream);
        }

        /**
         * <summary>Read decimal array.</summary>
         * <param name="stream">Stream.</param>
         * <returns>Decimal array.</returns>
         */
        public static decimal[] ReadDecimalArray(IPortableStream stream)
        {
            int len = stream.ReadInt();

            decimal[] vals = new decimal[len];

            for (int i = 0; i < len; i++)
                vals[i] = ReadDecimal(stream);

            return vals;
        }

        /**
         * <summary>Write GUID.</summary>
         * <param name="val">GUID.</param>
         * <param name="stream">Stream.</param>
         */
        public static unsafe void WriteGuid(Guid? val, IPortableStream stream)
        {
            Debug.Assert(val.HasValue);
            byte[] bytes = val.Value.ToByteArray();

            // .Net returns bytes in the following order: _a(4), _b(2), _c(2), _d, _e, _g, _h, _i, _j, _k.
            // And _a, _b and _c are always in little endian format irrespective of system configuration.
            // To be compliant with Java we rearrange them as follows: _c, _b_, a_, _k, _j, _i, _h, _g, _e, _d.
            fixed (byte* bytes0 = bytes)
            {
                stream.Write(bytes0 + 6, 2); // _c
                stream.Write(bytes0 + 4, 2); // _a
                stream.Write(bytes0, 4);     // _a
            }

            stream.WriteByte(bytes[15]); // _k
            stream.WriteByte(bytes[14]); // _j
            stream.WriteByte(bytes[13]); // _i
            stream.WriteByte(bytes[12]); // _h

            stream.WriteByte(bytes[11]); // _g
            stream.WriteByte(bytes[10]); // _f
            stream.WriteByte(bytes[9]);  // _e
            stream.WriteByte(bytes[8]);  // _d
        }

        /**
         * <summary>Read GUID.</summary>
         * <param name="stream">Stream.</param>
         * <returns>GUID</returns>
         */
        public static unsafe Guid? ReadGuid(IPortableStream stream)
        {
            byte[] bytes = new byte[16];

            // Perform conversion opposite to what write does.
            fixed (byte* bytes0 = bytes)
            {
                stream.Read(bytes0 + 6, 2);      // _c
                stream.Read(bytes0 + 4, 2);      // _b
                stream.Read(bytes0, 4);          // _a
            }

            bytes[15] = stream.ReadByte();  // _k
            bytes[14] = stream.ReadByte();  // _j
            bytes[13] = stream.ReadByte();  // _i
            bytes[12] = stream.ReadByte();  // _h

            bytes[11] = stream.ReadByte();  // _g
            bytes[10] = stream.ReadByte();  // _f
            bytes[9] = stream.ReadByte();   // _e
            bytes[8] = stream.ReadByte();   // _d

            return new Guid(bytes);
        }

        /**
         * <summary>Read GUID.</summary>
         * <param name="data">Data array.</param>
         * <param name="pos">Position.</param>
         * <returns>GUID</returns>
         */
        public static Guid ReadGuid(byte[] data, int pos) {
            byte[] bytes = new byte[16];

            // Perform conversion opposite to what write does.
            bytes[6] = data[pos];  // _c
            bytes[7] = data[pos + 1];

            bytes[4] = data[pos + 2];  // _b
            bytes[5] = data[pos + 3];

            bytes[0] = data[pos + 4];  // _a
            bytes[1] = data[pos + 5];
            bytes[2] = data[pos + 6];
            bytes[3] = data[pos + 7];

            bytes[15] = data[pos + 8];  // _k
            bytes[14] = data[pos + 9];  // _j
            bytes[13] = data[pos + 10];  // _i
            bytes[12] = data[pos + 11];  // _h

            bytes[11] = data[pos + 12];  // _g
            bytes[10] = data[pos + 13];  // _f
            bytes[9] = data[pos + 14];   // _e
            bytes[8] = data[pos + 15];   // _d

            return new Guid(bytes);
        }

        /**
         * <summary>Write GUID array.</summary>
         * <param name="vals">GUID array.</param>
         * <param name="stream">Stream.</param>
         */
        public static void WriteGuidArray(Guid?[] vals, IPortableStream stream)
        {
            stream.WriteInt(vals.Length);

            foreach (Guid? val in vals)
            {
                if (val.HasValue)
                    PortableSystemHandlers.WriteHndGuidTyped(stream, val);
                else
                    stream.WriteByte(HdrNull);
            }
        }

        /**
         * <summary>Read GUID array.</summary>
         * <param name="stream">Stream.</param>
         * <returns>GUID array.</returns>
         */
        public static Guid?[] ReadGuidArray(IPortableStream stream)
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
        /// <param name="typed">Typed flag.</param>
        public static void WriteArray(Array val, PortableWriterImpl ctx, bool typed)
        {
            IPortableStream stream = ctx.Stream;

            if (typed)
                stream.WriteInt(ObjTypeId);

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
        public static object ReadArray(PortableReaderImpl ctx, bool typed, Type elementType)
        {
            Func<PortableReaderImpl, bool, object> result;

            if (!ArrayReaders.TryGetValue(elementType, out result))
                result = ArrayReaders.GetOrAdd(elementType, t =>
                    DelegateConverter.CompileFunc<Func<PortableReaderImpl, bool, object>>(null,
                        MtdhReadGenericArray.MakeGenericMethod(t),
                        new[] {typeof (PortableReaderImpl), typeof (bool)}, new[] {false, false, true}));

            return result(ctx, typed);
        }

        /// <summary>
        /// Read array.
        /// </summary>
        /// <param name="ctx">Read context.</param>
        /// <param name="typed">Typed flag.</param>
        /// <returns>Array.</returns>
        public static T[] ReadGenericArray<T>(PortableReaderImpl ctx, bool typed)
        {
            IPortableStream stream = ctx.Stream;

            if (typed)
                stream.ReadInt();

            int len = stream.ReadInt();

            var vals = new T[len];

            for (int i = 0; i < len; i++)
                vals[i] = ctx.Deserialize<T>();

            return vals;
        }

        /**
         * <summary>Read DateTime array.</summary>
         * <param name="stream">Stream.</param>
         * <param name="local">Local flag.</param>
         * <returns>Array.</returns>
         */
        public static DateTime?[] ReadDateArray(IPortableStream stream, bool local)
        {
            int len = stream.ReadInt();

            DateTime?[] vals = new DateTime?[len];

            for (int i = 0; i < len; i++)
                vals[i] = stream.ReadByte() == HdrNull ? null : ReadDate(stream, local);

            return vals;
        }

        /**
         * <summary>Write collection.</summary>
         * <param name="val">Value.</param>
         * <param name="ctx">Write context.</param>
         */
        public static void WriteCollection(ICollection val, PortableWriterImpl ctx)
        {
            byte colType = val.GetType() == typeof(ArrayList) ? CollectionArrayList : CollectionCustom;

            WriteTypedCollection(val, ctx, colType);
        }

        /**
         * <summary>Write non-null collection with known type.</summary>
         * <param name="val">Value.</param>
         * <param name="ctx">Write context.</param>
         * <param name="colType">Collection type.</param>
         */
        public static void WriteTypedCollection(ICollection val, PortableWriterImpl ctx, byte colType)
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
        public static ICollection ReadCollection(PortableReaderImpl ctx,
            PortableCollectionFactory factory, PortableCollectionAdder adder)
        {
            if (factory == null)
                factory = PortableSystemHandlers.CreateArrayList;

            if (adder == null)
                adder = PortableSystemHandlers.AddToArrayList;

            IPortableStream stream = ctx.Stream;

            int len = stream.ReadInt();

            ctx.Stream.Seek(1, SeekOrigin.Current);

            ICollection res = factory.Invoke(len);

            for (int i = 0; i < len; i++)
                adder.Invoke(res, ctx.Deserialize<object>());

            return res;
        }

        /**
         * <summary>Write generic collection.</summary>
         * <param name="val">Value.</param>
         * <param name="ctx">Write context.</param>
         */
        public static void WriteGenericCollection<T>(ICollection<T> val, PortableWriterImpl ctx)
        {
            Type type = val.GetType().GetGenericTypeDefinition();

            byte colType;

            if (type == typeof(List<>))
                colType = CollectionArrayList;
            else if (type == typeof(LinkedList<>))
                colType = CollectionLinkedList;
            else if (type == typeof(HashSet<>))
                colType = CollectionHashSet;
            else if (type == typeof(SortedSet<>))
                colType = CollectionSortedSet;
            else
                colType = CollectionCustom;

            WriteTypedGenericCollection(val, ctx, colType);
        }

        /**
         * <summary>Write generic non-null collection with known type.</summary>
         * <param name="val">Value.</param>
         * <param name="ctx">Write context.</param>
         * <param name="colType">Collection type.</param>
         */
        public static void WriteTypedGenericCollection<T>(ICollection<T> val, PortableWriterImpl ctx,
            byte colType)
        {
            ctx.Stream.WriteInt(val.Count);

            ctx.Stream.WriteByte(colType);

            foreach (T elem in val)
                ctx.Write(elem);
        }

        /**
         * <summary>Read generic collection.</summary>
         * <param name="ctx">Context.</param>
         * <param name="factory">Factory delegate.</param>
         * <returns>Collection.</returns>
         */
        public static ICollection<T> ReadGenericCollection<T>(PortableReaderImpl ctx,
            PortableGenericCollectionFactory<T> factory)
        {
            int len = ctx.Stream.ReadInt();

            if (len >= 0)
            {
                byte colType = ctx.Stream.ReadByte();

                if (factory == null)
                {
                    // Need to detect factory automatically.
                    if (colType == CollectionLinkedList)
                        factory = PortableSystemHandlers.CreateLinkedList<T>;
                    else if (colType == CollectionHashSet)
                        factory = PortableSystemHandlers.CreateHashSet<T>;
                    else if (colType == CollectionSortedSet)
                        factory = PortableSystemHandlers.CreateSortedSet<T>;
                    else
                        factory = PortableSystemHandlers.CreateList<T>;
                }

                ICollection<T> res = factory.Invoke(len);

                for (int i = 0; i < len; i++)
                    res.Add(ctx.Deserialize<T>());

                return res;
            }
            return null;
        }

        /**
         * <summary>Write dictionary.</summary>
         * <param name="val">Value.</param>
         * <param name="ctx">Write context.</param>
         */
        public static void WriteDictionary(IDictionary val, PortableWriterImpl ctx)
        {
            byte dictType = val.GetType() == typeof(Hashtable) ? MapHashMap : MapCustom;

            WriteTypedDictionary(val, ctx, dictType);
        }

        /**
         * <summary>Write non-null dictionary with known type.</summary>
         * <param name="val">Value.</param>
         * <param name="ctx">Write context.</param>
         * <param name="dictType">Dictionary type.</param>
         */
        public static void WriteTypedDictionary(IDictionary val, PortableWriterImpl ctx, byte dictType)
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
        public static IDictionary ReadDictionary(PortableReaderImpl ctx,
            PortableDictionaryFactory factory)
        {
            if (factory == null)
                factory = PortableSystemHandlers.CreateHashtable;

            IPortableStream stream = ctx.Stream;

            int len = stream.ReadInt();

            ctx.Stream.Seek(1, SeekOrigin.Current);

            IDictionary res = factory.Invoke(len);

            for (int i = 0; i < len; i++)
            {
                object key = ctx.Deserialize<object>();
                object val = ctx.Deserialize<object>();

                res[key] = val;
            }

            return res;
        }

        /**
         * <summary>Write generic dictionary.</summary>
         * <param name="val">Value.</param>
         * <param name="ctx">Write context.</param>
         */
        public static void WriteGenericDictionary<TK, TV>(IDictionary<TK, TV> val, PortableWriterImpl ctx)
        {
            Type type = val.GetType().GetGenericTypeDefinition();

            byte dictType;

            if (type == typeof(Dictionary<,>))
                dictType = MapHashMap;
            else if (type == typeof(SortedDictionary<,>))
                dictType = MapSortedMap;
            else if (type == typeof(ConcurrentDictionary<,>))
                dictType = MapConcurrentHashMap;
            else
                dictType = MapCustom;

            WriteTypedGenericDictionary(val, ctx, dictType);
        }

        /**
         * <summary>Write generic non-null dictionary with known type.</summary>
         * <param name="val">Value.</param>
         * <param name="ctx">Write context.</param>
         * <param name="dictType">Dictionary type.</param>
         */
        public static void WriteTypedGenericDictionary<TK, TV>(IDictionary<TK, TV> val,
            PortableWriterImpl ctx, byte dictType)
        {
            ctx.Stream.WriteInt(val.Count);

            ctx.Stream.WriteByte(dictType);

            foreach (KeyValuePair<TK, TV> entry in val)
            {
                ctx.Write(entry.Key);
                ctx.Write(entry.Value);
            }
        }

        /**
         * <summary>Read generic dictionary.</summary>
         * <param name="ctx">Context.</param>
         * <param name="factory">Factory delegate.</param>
         * <returns>Collection.</returns>
         */
        public static IDictionary<TK, TV> ReadGenericDictionary<TK, TV>(PortableReaderImpl ctx,
            PortableGenericDictionaryFactory<TK, TV> factory)
        {
            int len = ctx.Stream.ReadInt();

            if (len >= 0)
            {
                byte colType = ctx.Stream.ReadByte();

                if (factory == null)
                {
                    if (colType == MapSortedMap)
                        factory = PortableSystemHandlers.CreateSortedDictionary<TK, TV>;
                    else if (colType == MapConcurrentHashMap)
                        factory = PortableSystemHandlers.CreateConcurrentDictionary<TK, TV>;
                    else
                        factory = PortableSystemHandlers.CreateDictionary<TK, TV>;
                }

                IDictionary<TK, TV> res = factory.Invoke(len);

                for (int i = 0; i < len; i++)
                {
                    TK key = ctx.Deserialize<TK>();
                    TV val = ctx.Deserialize<TV>();

                    res[key] = val;
                }

                return res;
            }
            return null;
        }

        /**
         * <summary>Write map entry.</summary>
         * <param name="ctx">Write context.</param>
         * <param name="val">Value.</param>
         */
        public static void WriteMapEntry(PortableWriterImpl ctx, DictionaryEntry val)
        {
            ctx.Write(val.Key);
            ctx.Write(val.Value);
        }

        /**
         * <summary>Read map entry.</summary>
         * <param name="ctx">Context.</param>
         * <returns>Map entry.</returns>
         */
        public static DictionaryEntry ReadMapEntry(PortableReaderImpl ctx)
        {
            object key = ctx.Deserialize<object>();
            object val = ctx.Deserialize<object>();

            return new DictionaryEntry(key, val);
        }

        /**
         * <summary>Write portable object.</summary>
         * <param name="stream">Stream.</param>
         * <param name="val">Value.</param>
         */
        public static void WritePortable(IPortableStream stream, PortableUserObject val)
        {
            WriteByteArray(val.Data, stream);

            stream.WriteInt(val.Offset);
        }

        /// <summary>
        /// Write enum.
        /// </summary>
        /// <param name="stream">Stream.</param>
        /// <param name="val">Value.</param>
        public static void WriteEnum(IPortableStream stream, Enum val)
        {
            if (Enum.GetUnderlyingType(val.GetType()) == TypInt)
            {
                stream.WriteInt(ObjTypeId);
                stream.WriteInt(Convert.ToInt32(val));
            }
            else
                throw new PortableException("Only Int32 underlying type is supported for enums: " +
                    val.GetType().Name);
        }

        /// <summary>
        /// Read enum.
        /// </summary>
        /// <param name="stream">Stream.</param>
        /// <returns>Enumeration.</returns>
        public static T ReadEnum<T>(IPortableStream stream)
        {
            if (!typeof(T).IsEnum || Enum.GetUnderlyingType(typeof(T)) == TypInt)
            {
                stream.ReadInt();

                return TypeCaster<T>.Cast(stream.ReadInt());
            }

            throw new PortableException("Only Int32 underlying type is supported for enums: " +
                                        typeof (T).Name);
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
        public static int StringHashCode(string val)
        {
            if (val == null)
                return 0;
            int hash = 0;

            for (int i = 0; i < val.Length; i++)
            {
                char c = val[i];

                if ('A' <= c && c <= 'Z')
                    c = (char)(c | 0x20);

                hash = 31 * hash + c;
            }

            return hash;
        }

        public static string CleanFieldName(string fieldName)
        {
            if (fieldName.StartsWith("<") && fieldName.EndsWith(">k__BackingField"))
                return fieldName.Substring(1, fieldName.IndexOf(">", StringComparison.Ordinal) - 1);
            
            return fieldName;
        }

        /**
         * <summary>Check whether this is predefined type.</summary>
         * <param name="hdr">Header.</param>
         * <returns>True is this is one of predefined types with special semantics.</returns>
         */
        public static bool IsPredefinedType(byte hdr)
        {
            switch (hdr)
            {
                case TypeByte:
                case TypeShort:
                case TypeInt:
                case TypeLong:
                case TypeFloat:
                case TypeDouble:
                case TypeChar:
                case TypeBool:
                case TypeDecimal:
                case TypeString:
                case TypeGuid:
                case TypeDate:
                case TypeEnum:
                case TypeArrayByte:
                case TypeArrayShort:
                case TypeArrayInt:
                case TypeArrayLong:
                case TypeArrayFloat:
                case TypeArrayDouble:
                case TypeArrayChar:
                case TypeArrayBool:
                case TypeArrayDecimal:
                case TypeArrayString:
                case TypeArrayGuid:
                case TypeArrayDate:
                case TypeArrayEnum:
                case TypeArray:
                case TypeCollection:
                case TypeDictionary:
                case TypeMapEntry:
                case TypePortable:
                    return true;
                default:
                    return false;
            }
        }

        /**
         * <summary>Convert type name.</summary>
         * <param name="typeName">Type name.</param>
         * <param name="converter">Converter.</param>
         * <returns>Converted name.</returns>
         */
        public static string ConvertTypeName(string typeName, IPortableNameMapper converter)
        {
            var typeName0 = typeName;

            try
            {
                if (converter != null)
                    typeName = converter.TypeName(typeName);
            }
            catch (Exception e)
            {
                throw new PortableException("Failed to convert type name due to converter exception " +
                    "[typeName=" + typeName + ", converter=" + converter + ']', e);
            }

            if (typeName == null)
                throw new PortableException("Name converter returned null name for type [typeName=" +
                    typeName0 + ", converter=" + converter + "]");

            return typeName;
        }

        /**
         * <summary>Convert field name.</summary>
         * <param name="fieldName">Field name.</param>
         * <param name="converter">Converter.</param>
         * <returns>Converted name.</returns>
         */
        public static string ConvertFieldName(string fieldName, IPortableNameMapper converter)
        {
            var fieldName0 = fieldName;

            try
            {
                if (converter != null)
                    fieldName = converter.FieldName(fieldName);
            }
            catch (Exception e)
            {
                throw new PortableException("Failed to convert field name due to converter exception " +
                    "[fieldName=" + fieldName + ", converter=" + converter + ']', e);
            }

            if (fieldName == null)
                throw new PortableException("Name converter returned null name for field [fieldName=" +
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
        public static int TypeId(string typeName, IPortableNameMapper nameMapper,
            IPortableIdMapper idMapper)
        {
            Debug.Assert(typeName != null);

            typeName = ConvertTypeName(typeName, nameMapper);

            int id = 0;

            if (idMapper != null)
            {
                try
                {
                    id = idMapper.TypeId(typeName);
                }
                catch (Exception e)
                {
                    throw new PortableException("Failed to resolve type ID due to ID mapper exception " +
                        "[typeName=" + typeName + ", idMapper=" + idMapper + ']', e);
                }
            }

            if (id == 0)
                id = StringHashCode(typeName);

            return id;
        }

        /**
         * <summary>Resolve field ID.</summary>
         * <param name="typeId">Type ID.</param>
         * <param name="fieldName">Field name.</param>
         * <param name="nameMapper">Name mapper.</param>
         * <param name="idMapper">ID mapper.</param>
         */
        public static int FieldId(int typeId, string fieldName, IPortableNameMapper nameMapper,
            IPortableIdMapper idMapper)
        {
            Debug.Assert(typeId != 0);
            Debug.Assert(fieldName != null);

            fieldName = ConvertFieldName(fieldName, nameMapper);

            int id = 0;

            if (idMapper != null)
            {
                try
                {
                    id = idMapper.FieldId(typeId, fieldName);
                }
                catch (Exception e)
                {
                    throw new PortableException("Failed to resolve field ID due to ID mapper exception " +
                        "[typeId=" + typeId + ", fieldName=" + fieldName + ", idMapper=" + idMapper + ']', e);
                }
            }

            if (id == 0)
                id = StringHashCode(fieldName);

            return id;
        }

        /**
         * <summary>Get fields map for the given object.</summary>
         * <param name="stream">Stream.</param>
         * <param name="typeId">Type ID.</param>
         * <param name="rawDataOffset">Raw data offset.</param>
         * <returns>Dictionary with field ID as key and field position as value.</returns>
         */
        public static IDictionary<int, int> ObjectFields(IPortableStream stream, int typeId, int rawDataOffset)
        {
            int endPos = stream.Position + rawDataOffset - 18;

            // First loop detects amount of fields in the object.
            int retPos = stream.Position;
            int cnt = 0;

            while (stream.Position < endPos)
            {
                cnt++;

                stream.Seek(4, SeekOrigin.Current);
                int len = stream.ReadInt();

                stream.Seek(stream.Position + len, SeekOrigin.Begin);
            }

            if (cnt == 0)
                return EmptyFields;

            stream.Seek(retPos, SeekOrigin.Begin);

            IDictionary<int, int> fields = new Dictionary<int, int>(cnt);

            // Second loop populates fields.
            while (stream.Position < endPos)
            {
                int id = stream.ReadInt();
                int len = stream.ReadInt();

                if (fields.ContainsKey(id))
                    throw new PortableException("Object contains duplicate field IDs [typeId=" +
                        typeId + ", fieldId=" + id + ']');

                fields[id] = stream.Position; // Add field ID and length.

                stream.Seek(stream.Position + len, SeekOrigin.Begin);
            }

            return fields;
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
        /// Write object which is not necessary portable.
        /// </summary>
        /// <param name="writer">Writer.</param>
        /// <param name="obj">Object.</param>
        public static void WritePortableOrSerializable<T>(PortableWriterImpl writer, T obj)
        {
            if (writer.IsPortable(obj))
            {
                writer.WriteBoolean(true);

                writer.WriteObject(obj);
            }
            else
            {
                writer.WriteBoolean(false);

                WriteSerializable(writer, obj);
            }
        }

        /// <summary>
        /// Writes a serializable object.
        /// </summary>
        /// <param name="writer">Writer.</param>
        /// <param name="obj">Object.</param>
        public static void WriteSerializable<T>(PortableWriterImpl writer, T obj)
        {
            new BinaryFormatter().Serialize(new PortableStreamAdapter(writer.Stream), obj);
        }

        /// <summary>
        /// Read object which is not necessary portable.
        /// </summary>
        /// <param name="reader">Reader.</param>
        /// <returns>Object.</returns>
        public static T ReadPortableOrSerializable<T>(PortableReaderImpl reader)
        {
            return reader.ReadBoolean()
                ? reader.ReadObject<T>()
                : ReadSerializable<T>(reader);
        }

        /// <summary>
        /// Reads a serializable object.
        /// </summary>
        /// <param name="reader">Reader.</param>
        /// <returns>Object.</returns>
        public static T ReadSerializable<T>(PortableReaderImpl reader)
        {
            return (T) new BinaryFormatter().Deserialize(new PortableStreamAdapter(reader.Stream), null);
        }

        /// <summary>
        /// Writes wrapped invocation result.
        /// </summary>
        /// <param name="writer">Writer.</param>
        /// <param name="success">Success flag.</param>
        /// <param name="res">Result.</param>
        public static void WriteWrappedInvocationResult(PortableWriterImpl writer, bool success, object res)
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

                writer.Write(new PortableResultWrapper(res));
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
        /// Writes invocation result.
        /// </summary>
        /// <param name="writer">Writer.</param>
        /// <param name="success">Success flag.</param>
        /// <param name="res">Result.</param>
        public static void WriteInvocationResult(PortableWriterImpl writer, bool success, object res)
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
        /// Reads wrapped invocation result.
        /// </summary>
        /// <param name="reader">Reader.</param>
        /// <param name="err">Error.</param>
        /// <returns>Result.</returns>
        public static object ReadWrappedInvocationResult(PortableReaderImpl reader, out object err)
        {
            err = null;

            if (reader.ReadBoolean())
                return reader.ReadObject<PortableResultWrapper>().Result;

            if (reader.ReadBoolean())
                err = (Exception) reader.ReadObject<PortableResultWrapper>().Result;
            else
                err = ExceptionUtils.GetException(reader.ReadString(), reader.ReadString());

            return null;
        }

        /// <summary>
        /// Reads invocation result.
        /// </summary>
        /// <param name="reader">Reader.</param>
        /// <param name="err">Error.</param>
        /// <returns>Result.</returns>
        public static object ReadInvocationResult(PortableReaderImpl reader, out object err)
        {
            err = null;

            if (reader.ReadBoolean())
                return reader.ReadObject<object>();

            if (reader.ReadBoolean())
                err = reader.ReadObject<object>();
            else
                err = ExceptionUtils.GetException(reader.ReadString(), reader.ReadString());

            return null;
        }

        /**
         * <summary>Convert date to Java ticks.</summary>
         * <param name="date">Date</param>
         * <param name="high">High part (milliseconds).</param>
         * <param name="low">Low part (nanoseconds)</param>
         */
        private static void ToJavaDate(DateTime date, out long high, out int low)
        {
            long diff = date.ToUniversalTime().Ticks - JavaDateTicks;

            high = diff / TimeSpan.TicksPerMillisecond;

            low = (int)(diff % TimeSpan.TicksPerMillisecond) * 100; 
        }

        /**
         * <summary>Convert Java ticks to date.</summary>
         * <param name="high">High part (milliseconds).</param>
         * <param name="low">Low part (nanoseconds).</param>
         * <param name="local">Whether the time should be treaten as local.</param>
         * <returns>Date.</returns>
         */
        private static DateTime ToDotNetDate(long high, int low, bool local)
        {
            DateTime res = 
                new DateTime(JavaDateTicks + high * TimeSpan.TicksPerMillisecond + low / 100, DateTimeKind.Utc);

            return local ? res.ToLocalTime() : res;
        }
    }
}
