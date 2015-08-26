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
    using Apache.Ignite.Core.Impl.Common;
    using Apache.Ignite.Core.Impl.Portable.IO;

    /**
     * <summary>Write delegate.</summary>
     * <param name="ctx">Write context.</param>
     * <param name="obj">Object to write.</param>
     */
    public delegate void PortableSystemWriteDelegate(PortableWriterImpl ctx, object obj);

    /// <summary>
    /// Typed write delegate.
    /// </summary>
    /// <param name="stream">Stream.</param>
    /// <param name="obj">Object to write.</param>
    internal delegate void PortableSystemTypedWriteDelegate<T>(IPortableStream stream, T obj);

    /**
     * <summary>Collection of predefined handlers for various system types.</summary>
     */
    internal static class PortableSystemHandlers
    {
        /** Write handlers. */
        private static readonly Dictionary<Type, PortableSystemWriteDelegate> WRITE_HANDLERS =
            new Dictionary<Type, PortableSystemWriteDelegate>();

        /** Read handlers. */
        private static readonly IPortableSystemReader[] READ_HANDLERS = new IPortableSystemReader[255];

        /** Typed write handler: boolean. */
        public static readonly PortableSystemTypedWriteDelegate<bool> WRITE_HND_BOOL_TYPED = WriteBoolTyped;

        /** Typed write handler: byte. */
        public static readonly PortableSystemTypedWriteDelegate<byte> WRITE_HND_BYTE_TYPED = WriteByteTyped;

        /** Typed write handler: short. */
        public static readonly PortableSystemTypedWriteDelegate<short> WRITE_HND_SHORT_TYPED = WriteShortTyped;

        /** Typed write handler: char. */
        public static readonly PortableSystemTypedWriteDelegate<char> WRITE_HND_CHAR_TYPED = WriteCharTyped;

        /** Typed write handler: int. */
        public static readonly PortableSystemTypedWriteDelegate<int> WRITE_HND_INT_TYPED = WriteIntTyped;

        /** Typed write handler: long. */
        public static readonly PortableSystemTypedWriteDelegate<long> WRITE_HND_LONG_TYPED = WriteLongTyped;

        /** Typed write handler: float. */
        public static readonly PortableSystemTypedWriteDelegate<float> WRITE_HND_FLOAT_TYPED = WriteFloatTyped;

        /** Typed write handler: double. */
        public static readonly PortableSystemTypedWriteDelegate<double> WRITE_HND_DOUBLE_TYPED = WriteDoubleTyped;

        /** Typed write handler: decimal. */
        public static readonly PortableSystemTypedWriteDelegate<decimal> WRITE_HND_DECIMAL_TYPED = WriteDecimalTyped;

        /** Typed write handler: Date. */
        public static readonly PortableSystemTypedWriteDelegate<DateTime?> WRITE_HND_DATE_TYPED = WriteDateTyped;

        /** Typed write handler: string. */
        public static readonly PortableSystemTypedWriteDelegate<string> WRITE_HND_STRING_TYPED = WriteStringTyped;

        /** Typed write handler: Guid. */
        public static readonly PortableSystemTypedWriteDelegate<Guid?> WRITE_HND_GUID_TYPED = WriteGuidTyped;

        /** Typed write handler: Portable. */
        public static readonly PortableSystemTypedWriteDelegate<PortableUserObject> WRITE_HND_PORTABLE_TYPED = WritePortableTyped;

        /** Typed write handler: boolean array. */
        public static readonly PortableSystemTypedWriteDelegate<bool[]> WRITE_HND_BOOL_ARRAY_TYPED = WriteBoolArrayTyped;

        /** Typed write handler: byte array. */
        public static readonly PortableSystemTypedWriteDelegate<byte[]> WRITE_HND_BYTE_ARRAY_TYPED = WriteByteArrayTyped;

        /** Typed write handler: short array. */
        public static readonly PortableSystemTypedWriteDelegate<short[]> WRITE_HND_SHORT_ARRAY_TYPED = WriteShortArrayTyped;

        /** Typed write handler: char array. */
        public static readonly PortableSystemTypedWriteDelegate<char[]> WRITE_HND_CHAR_ARRAY_TYPED = WriteCharArrayTyped;

        /** Typed write handler: int array. */
        public static readonly PortableSystemTypedWriteDelegate<int[]> WRITE_HND_INT_ARRAY_TYPED = WriteIntArrayTyped;

        /** Typed write handler: long array. */
        public static readonly PortableSystemTypedWriteDelegate<long[]> WRITE_HND_LONG_ARRAY_TYPED = WriteLongArrayTyped;

        /** Typed write handler: float array. */
        public static readonly PortableSystemTypedWriteDelegate<float[]> WRITE_HND_FLOAT_ARRAY_TYPED = WriteFloatArrayTyped;

        /** Typed write handler: double array. */
        public static readonly PortableSystemTypedWriteDelegate<double[]> WRITE_HND_DOUBLE_ARRAY_TYPED = WriteDoubleArrayTyped;

        /** Typed write handler: decimal array. */
        public static readonly PortableSystemTypedWriteDelegate<decimal[]> WRITE_HND_DECIMAL_ARRAY_TYPED = WriteDecimalArrayTyped;

        /** Typed write handler: Date array. */
        public static readonly PortableSystemTypedWriteDelegate<DateTime?[]> WRITE_HND_DATE_ARRAY_TYPED = WriteDateArrayTyped;

        /** Typed write handler: string array. */
        public static readonly PortableSystemTypedWriteDelegate<string[]> WRITE_HND_STRING_ARRAY_TYPED = WriteStringArrayTyped;

        /** Typed write handler: Guid array. */
        public static readonly PortableSystemTypedWriteDelegate<Guid?[]> WRITE_HND_GUID_ARRAY_TYPED = WriteGuidArrayTyped;

        /** Write handler: boolean. */
        public static readonly PortableSystemWriteDelegate WRITE_HND_BOOL = WriteBool;

        /** Write handler: sbyte. */
        public static readonly PortableSystemWriteDelegate WRITE_HND_SBYTE = WriteSbyte;

        /** Write handler: byte. */
        public static readonly PortableSystemWriteDelegate WRITE_HND_BYTE = WriteByte;

        /** Write handler: short. */
        public static readonly PortableSystemWriteDelegate WRITE_HND_SHORT = WriteShort;

        /** Write handler: ushort. */
        public static readonly PortableSystemWriteDelegate WRITE_HND_USHORT = WriteUshort;

        /** Write handler: char. */
        public static readonly PortableSystemWriteDelegate WRITE_HND_CHAR = WriteChar;

        /** Write handler: int. */
        public static readonly PortableSystemWriteDelegate WRITE_HND_INT = WriteInt;

        /** Write handler: uint. */
        public static readonly PortableSystemWriteDelegate WRITE_HND_UINT = WriteUint;

        /** Write handler: long. */
        public static readonly PortableSystemWriteDelegate WRITE_HND_LONG = WriteLong;

        /** Write handler: ulong. */
        public static readonly PortableSystemWriteDelegate WRITE_HND_ULONG = WriteUlong;

        /** Write handler: float. */
        public static readonly PortableSystemWriteDelegate WRITE_HND_FLOAT = WriteFloat;

        /** Write handler: double. */
        public static readonly PortableSystemWriteDelegate WRITE_HND_DOUBLE = WriteDouble;

        /** Write handler: decimal. */
        public static readonly PortableSystemWriteDelegate WRITE_HND_DECIMAL = WriteDecimal;

        /** Write handler: Date. */
        public static readonly PortableSystemWriteDelegate WRITE_HND_DATE = WriteDate;

        /** Write handler: string. */
        public static readonly PortableSystemWriteDelegate WRITE_HND_STRING = WriteString;

        /** Write handler: Guid. */
        public static readonly PortableSystemWriteDelegate WRITE_HND_GUID = WriteGuid;

        /** Write handler: Portable. */
        public static readonly PortableSystemWriteDelegate WRITE_HND_PORTABLE = WritePortable;

        /** Write handler: Enum. */
        public static readonly PortableSystemWriteDelegate WRITE_HND_ENUM = WriteEnum;

        /** Write handler: boolean array. */
        public static readonly PortableSystemWriteDelegate WRITE_HND_BOOL_ARRAY = WriteBoolArray;

        /** Write handler: sbyte array. */
        public static readonly PortableSystemWriteDelegate WRITE_HND_SBYTE_ARRAY = WriteSbyteArray;

        /** Write handler: byte array. */
        public static readonly PortableSystemWriteDelegate WRITE_HND_BYTE_ARRAY = WriteByteArray;

        /** Write handler: short array. */
        public static readonly PortableSystemWriteDelegate WRITE_HND_SHORT_ARRAY = WriteShortArray;

        /** Write handler: ushort array. */
        public static readonly PortableSystemWriteDelegate WRITE_HND_USHORT_ARRAY = WriteUshortArray;

        /** Write handler: char array. */
        public static readonly PortableSystemWriteDelegate WRITE_HND_CHAR_ARRAY = WriteCharArray;

        /** Write handler: int array. */
        public static readonly PortableSystemWriteDelegate WRITE_HND_INT_ARRAY = WriteIntArray;

        /** Write handler: uint array. */
        public static readonly PortableSystemWriteDelegate WRITE_HND_UINT_ARRAY = WriteUintArray;

        /** Write handler: long array. */
        public static readonly PortableSystemWriteDelegate WRITE_HND_LONG_ARRAY = WriteLongArray;

        /** Write handler: ulong array. */
        public static readonly PortableSystemWriteDelegate WRITE_HND_ULONG_ARRAY = WriteUlongArray;

        /** Write handler: float array. */
        public static readonly PortableSystemWriteDelegate WRITE_HND_FLOAT_ARRAY = WriteFloatArray;

        /** Write handler: double array. */
        public static readonly PortableSystemWriteDelegate WRITE_HND_DOUBLE_ARRAY = WriteDoubleArray;

        /** Write handler: decimal array. */
        public static readonly PortableSystemWriteDelegate WRITE_HND_DECIMAL_ARRAY = WriteDecimalArray;

        /** Write handler: date array. */
        public static readonly PortableSystemWriteDelegate WRITE_HND_DATE_ARRAY = WriteDateArray;

        /** Write handler: string array. */
        public static readonly PortableSystemWriteDelegate WRITE_HND_STRING_ARRAY = WriteStringArray;

        /** Write handler: Guid array. */
        public static readonly PortableSystemWriteDelegate WRITE_HND_GUID_ARRAY = WriteGuidArray;

        /** Write handler: Enum array. */
        public static readonly PortableSystemWriteDelegate WRITE_HND_ENUM_ARRAY = WriteEnumArray;

        /** Write handler: object array. */
        public static readonly PortableSystemWriteDelegate WRITE_HND_ARRAY = WriteArray;

        /** Write handler: collection. */
        public static readonly PortableSystemWriteDelegate WRITE_HND_COLLECTION = WriteCollection;

        /** Write handler: dictionary. */
        public static readonly PortableSystemWriteDelegate WRITE_HND_DICTIONARY = WriteDictionary;

        /** Write handler: generic collection. */
        public static readonly PortableSystemWriteDelegate WRITE_HND_GENERIC_COLLECTION =
            WriteGenericCollection;

        /** Write handler: generic dictionary. */
        public static readonly PortableSystemWriteDelegate WRITE_HND_GENERIC_DICTIONARY =
            WriteGenericDictionary;

        /**
         * <summary>Static initializer.</summary>
         */
        static PortableSystemHandlers()
        {
            // 1. Primitives.

            READ_HANDLERS[PortableUtils.TYPE_BOOL] = new PortableSystemReader<bool>(s => s.ReadBool());

            WRITE_HANDLERS[typeof(sbyte)] = WRITE_HND_SBYTE;
            READ_HANDLERS[PortableUtils.TYPE_BYTE] = new PortableSystemReader<byte>(s => s.ReadByte());

            WRITE_HANDLERS[typeof(ushort)] = WRITE_HND_USHORT;
            READ_HANDLERS[PortableUtils.TYPE_SHORT] = new PortableSystemReader<short>(s => s.ReadShort());

            READ_HANDLERS[PortableUtils.TYPE_CHAR] = new PortableSystemReader<char>(s => s.ReadChar());

            WRITE_HANDLERS[typeof(uint)] = WRITE_HND_UINT;
            READ_HANDLERS[PortableUtils.TYPE_INT] = new PortableSystemReader<int>(s => s.ReadInt());

            WRITE_HANDLERS[typeof(ulong)] = WRITE_HND_ULONG;
            READ_HANDLERS[PortableUtils.TYPE_LONG] = new PortableSystemReader<long>(s => s.ReadLong());

            READ_HANDLERS[PortableUtils.TYPE_FLOAT] = new PortableSystemReader<float>(s => s.ReadFloat());

            READ_HANDLERS[PortableUtils.TYPE_DOUBLE] = new PortableSystemReader<double>(s => s.ReadDouble());

            READ_HANDLERS[PortableUtils.TYPE_DECIMAL] = new PortableSystemReader<decimal>(PortableUtils.ReadDecimal);

            // 2. Date.
            READ_HANDLERS[PortableUtils.TYPE_DATE] =
                new PortableSystemReader<DateTime?>(s => PortableUtils.ReadDate(s, false));

            // 3. String.
            READ_HANDLERS[PortableUtils.TYPE_STRING] = new PortableSystemReader<string>(PortableUtils.ReadString);

            // 4. Guid.
            READ_HANDLERS[PortableUtils.TYPE_GUID] = new PortableSystemReader<Guid?>(PortableUtils.ReadGuid);

            // 5. Primitive arrays.
            READ_HANDLERS[PortableUtils.TYPE_ARRAY_BOOL] = new PortableSystemReader<bool[]>(PortableUtils.ReadBooleanArray);

            WRITE_HANDLERS[typeof(sbyte[])] = WRITE_HND_SBYTE_ARRAY;
            READ_HANDLERS[PortableUtils.TYPE_ARRAY_BYTE] =
                new PortableSystemDualReader<byte[], sbyte[]>(PortableUtils.ReadByteArray, PortableUtils.ReadSbyteArray);

            WRITE_HANDLERS[typeof(ushort[])] = WRITE_HND_USHORT_ARRAY;
            READ_HANDLERS[PortableUtils.TYPE_ARRAY_SHORT] =
                new PortableSystemDualReader<short[], ushort[]>(PortableUtils.ReadShortArray,
                    PortableUtils.ReadUshortArray);

            READ_HANDLERS[PortableUtils.TYPE_ARRAY_CHAR] = 
                new PortableSystemReader<char[]>(PortableUtils.ReadCharArray);

            WRITE_HANDLERS[typeof(uint[])] = WRITE_HND_UINT_ARRAY;
            READ_HANDLERS[PortableUtils.TYPE_ARRAY_INT] =
                new PortableSystemDualReader<int[], uint[]>(PortableUtils.ReadIntArray, PortableUtils.ReadUintArray);


            WRITE_HANDLERS[typeof(ulong[])] = WRITE_HND_ULONG_ARRAY;
            READ_HANDLERS[PortableUtils.TYPE_ARRAY_LONG] =
                new PortableSystemDualReader<long[], ulong[]>(PortableUtils.ReadLongArray, 
                    PortableUtils.ReadUlongArray);

            READ_HANDLERS[PortableUtils.TYPE_ARRAY_FLOAT] =
                new PortableSystemReader<float[]>(PortableUtils.ReadFloatArray);

            READ_HANDLERS[PortableUtils.TYPE_ARRAY_DOUBLE] =
                new PortableSystemReader<double[]>(PortableUtils.ReadDoubleArray);

            READ_HANDLERS[PortableUtils.TYPE_ARRAY_DECIMAL] =
                new PortableSystemReader<decimal[]>(PortableUtils.ReadDecimalArray);

            // 6. Date array.
            READ_HANDLERS[PortableUtils.TYPE_ARRAY_DATE] =
                new PortableSystemReader<DateTime?[]>(s => PortableUtils.ReadDateArray(s, false));

            // 7. String array.
            READ_HANDLERS[PortableUtils.TYPE_ARRAY_STRING] = new PortableSystemGenericArrayReader<string>();

            // 8. Guid array.
            READ_HANDLERS[PortableUtils.TYPE_ARRAY_GUID] = new PortableSystemGenericArrayReader<Guid?>();

            // 9. Array.
            READ_HANDLERS[PortableUtils.TYPE_ARRAY] = new PortableSystemReader(ReadArray);

            // 10. Predefined collections.
            WRITE_HANDLERS[typeof(ArrayList)] = WriteArrayList;

            // 11. Predefined dictionaries.
            WRITE_HANDLERS[typeof(Hashtable)] = WriteHashtable;

            // 12. Arbitrary collection.
            READ_HANDLERS[PortableUtils.TYPE_COLLECTION] = new PortableSystemReader(ReadCollection);

            // 13. Arbitrary dictionary.
            READ_HANDLERS[PortableUtils.TYPE_DICTIONARY] = new PortableSystemReader(ReadDictionary);

            // 14. Map entry.
            WRITE_HANDLERS[typeof(DictionaryEntry)] = WriteMapEntry;
            READ_HANDLERS[PortableUtils.TYPE_MAP_ENTRY] = new PortableSystemReader(ReadMapEntry);

            // 15. Portable.
            WRITE_HANDLERS[typeof(PortableUserObject)] = WritePortable;

            // 16. Enum.
            READ_HANDLERS[PortableUtils.TYPE_ENUM] = new PortableSystemReader<int>(PortableUtils.ReadEnum<int>);
            READ_HANDLERS[PortableUtils.TYPE_ARRAY_ENUM] = new PortableSystemReader(ReadEnumArray);
        }

        /**
         * <summary>Get write handler for type.</summary>
         * <param name="type">Type.</param>
         * <returns>Handler or null if cannot be hanled in special way.</returns>
         */
        public static PortableSystemWriteDelegate WriteHandler(Type type)
        {
            PortableSystemWriteDelegate handler;

            if (WRITE_HANDLERS.TryGetValue(type, out handler))
                return handler;

            // 1. Array?
            if (type.IsArray)
            {
                if (type.GetElementType().IsEnum)
                    return WriteEnumArray;
                return WriteArray;
            }

            // 2. Enum?
            if (type.IsEnum)
                return WriteEnum;

            // 3. Collection?
            PortableCollectionInfo info = PortableCollectionInfo.Info(type);

            if (info.IsAny)
                return info.WriteHandler;

            // No special handler found.
            return null;
        }

        /// <summary>
        /// Reads an object of predefined type.
        /// </summary>
        public static T ReadSystemType<T>(byte typeId, IPortableReaderEx ctx)
        {
            var handler = READ_HANDLERS[typeId];

            Debug.Assert(handler != null, "Cannot find predefined read handler: " + typeId);
            
            return handler.Read<T>(ctx);
        }

        /**
         * <summary>Write boolean.</summary>
         */
        private static void WriteBool(IPortableWriterEx ctx, object obj)
        {
            WriteBoolTyped(ctx.Stream, (bool)obj);
        }

        /**
         * <summary>Write boolean.</summary>
         */
        private static void WriteBoolTyped(IPortableStream stream, bool obj)
        {
            stream.WriteByte(PortableUtils.TYPE_BOOL);

            stream.WriteBool(obj);
        }

        /**
         * <summary>Write sbyte.</summary>
         */
        private static unsafe void WriteSbyte(IPortableWriterEx ctx, object obj)
        {
            sbyte val = (sbyte)obj;

            ctx.Stream.WriteByte(PortableUtils.TYPE_BYTE);
            ctx.Stream.WriteByte(*(byte*)&val);
        }

        /**
         * <summary>Write byte.</summary>
         */
        private static void WriteByte(IPortableWriterEx ctx, object obj)
        {
            WriteByteTyped(ctx.Stream, (byte)obj);
        }

        /**
         * <summary>Write byte.</summary>
         */
        private static void WriteByteTyped(IPortableStream stream, byte obj)
        {
            stream.WriteByte(PortableUtils.TYPE_BYTE);
            stream.WriteByte(obj);
        }

        /**
         * <summary>Write short.</summary>
         */
        private static void WriteShort(IPortableWriterEx ctx, object obj)
        {
            WriteShortTyped(ctx.Stream, (short)obj);
        }

        /**
         * <summary>Write short.</summary>
         */
        private static void WriteShortTyped(IPortableStream stream, short obj)
        {
            stream.WriteByte(PortableUtils.TYPE_SHORT);

            stream.WriteShort(obj);
        }

        /**
         * <summary>Write ushort.</summary>
         */
        private static unsafe void WriteUshort(IPortableWriterEx ctx, object obj)
        {
            ushort val = (ushort)obj;

            ctx.Stream.WriteByte(PortableUtils.TYPE_SHORT);

            ctx.Stream.WriteShort(*(short*)&val);
        }

        /**
         * <summary>Write char.</summary>
         */
        private static void WriteChar(IPortableWriterEx ctx, object obj)
        {
            WriteCharTyped(ctx.Stream, (char)obj);
        }

        /**
         * <summary>Write char.</summary>
         */
        private static void WriteCharTyped(IPortableStream stream, char obj)
        {
            stream.WriteByte(PortableUtils.TYPE_CHAR);

            stream.WriteChar(obj);
        }

        /**
         * <summary>Write int.</summary>
         */
        private static void WriteInt(IPortableWriterEx ctx, object obj)
        {
            WriteIntTyped(ctx.Stream, (int)obj);
        }

        /**
         * <summary>Write int.</summary>
         */
        private static void WriteIntTyped(IPortableStream stream, int obj)
        {
            stream.WriteByte(PortableUtils.TYPE_INT);
            stream.WriteInt(obj);
        }

        /**
         * <summary>Write uint.</summary>
         */
        private static unsafe void WriteUint(IPortableWriterEx ctx, object obj)
        {
            uint val = (uint)obj;

            ctx.Stream.WriteByte(PortableUtils.TYPE_INT);
            ctx.Stream.WriteInt(*(int*)&val);
        }

        /**
         * <summary>Write long.</summary>
         */
        private static void WriteLong(IPortableWriterEx ctx, object obj)
        {
            WriteLongTyped(ctx.Stream, (long)obj);
        }

        /**
         * <summary>Write long.</summary>
         */
        private static void WriteLongTyped(IPortableStream stream, long obj)
        {
            stream.WriteByte(PortableUtils.TYPE_LONG);
            stream.WriteLong(obj);
        }

        /**
         * <summary>Write ulong.</summary>
         */
        private static unsafe void WriteUlong(IPortableWriterEx ctx, object obj)
        {
            ulong val = (ulong)obj;

            ctx.Stream.WriteByte(PortableUtils.TYPE_LONG);
            ctx.Stream.WriteLong(*(long*)&val);
        }

        /**
         * <summary>Write float.</summary>
         */
        private static void WriteFloat(IPortableWriterEx ctx, object obj)
        {
            WriteFloatTyped(ctx.Stream, (float)obj);
        }

        /**
         * <summary>Write float.</summary>
         */
        private static void WriteFloatTyped(IPortableStream stream, float obj)
        {
            stream.WriteByte(PortableUtils.TYPE_FLOAT);
            stream.WriteFloat(obj);
        }

        /**
         * <summary>Write double.</summary>
         */
        private static void WriteDouble(IPortableWriterEx ctx, object obj)
        {
            WriteDoubleTyped(ctx.Stream, (double)obj);
        }

        /**
         * <summary>Write double.</summary>
         */
        private static void WriteDoubleTyped(IPortableStream stream, double obj)
        {
            stream.WriteByte(PortableUtils.TYPE_DOUBLE);
            stream.WriteDouble(obj);
        }

        /**
         * <summary>Write decimal.</summary>
         */
        private static void WriteDecimal(IPortableWriterEx ctx, object obj)
        {
            WriteDecimalTyped(ctx.Stream, (decimal)obj);
        }

        /**
         * <summary>Write double.</summary>
         */
        private static void WriteDecimalTyped(IPortableStream stream, decimal obj)
        {
            stream.WriteByte(PortableUtils.TYPE_DECIMAL);

            PortableUtils.WriteDecimal(obj, stream);
        }

        /**
         * <summary>Write date.</summary>
         */
        private static void WriteDate(IPortableWriterEx ctx, object obj)
        {
            WriteDateTyped(ctx.Stream, (DateTime?)obj);
        }

        /**
         * <summary>Write double.</summary>
         */
        private static void WriteDateTyped(IPortableStream stream, DateTime? obj)
        {
            stream.WriteByte(PortableUtils.TYPE_DATE);

            PortableUtils.WriteDate(obj, stream);
        }

        /**
         * <summary>Write string.</summary>
         */
        private static void WriteString(IPortableWriterEx ctx, object obj)
        {
            WriteStringTyped(ctx.Stream, (string)obj);
        }

        /**
         * <summary>Write string.</summary>
         */
        private static void WriteStringTyped(IPortableStream stream, string obj)
        {
            stream.WriteByte(PortableUtils.TYPE_STRING);

            PortableUtils.WriteString(obj, stream);
        }

        /**
         * <summary>Write Guid.</summary>
         */
        private static void WriteGuid(IPortableWriterEx ctx, object obj)
        {
            WriteGuidTyped(ctx.Stream, (Guid?)obj);
        }

        /**
         * <summary>Write Guid.</summary>
         */
        private static void WriteGuidTyped(IPortableStream stream, Guid? obj)
        {
            stream.WriteByte(PortableUtils.TYPE_GUID);

            PortableUtils.WriteGuid(obj, stream);
        }

        /**
         * <summary>Write bool array.</summary>
         */
        private static void WriteBoolArray(IPortableWriterEx ctx, object obj)
        {
            WriteBoolArrayTyped(ctx.Stream, (bool[])obj);
        }

        /**
         * <summary>Write bool array.</summary>
         */
        private static void WriteBoolArrayTyped(IPortableStream stream, bool[] obj)
        {
            stream.WriteByte(PortableUtils.TYPE_ARRAY_BOOL);

            PortableUtils.WriteBooleanArray(obj, stream);
        }

        /**
         * <summary>Write byte array.</summary>
         */
        private static void WriteByteArray(IPortableWriterEx ctx, object obj)
        {
            WriteByteArrayTyped(ctx.Stream, (byte[])obj);
        }

        /**
         * <summary>Write byte array.</summary>
         */
        private static void WriteByteArrayTyped(IPortableStream stream, byte[] obj)
        {
            stream.WriteByte(PortableUtils.TYPE_ARRAY_BYTE);

            PortableUtils.WriteByteArray(obj, stream);
        }

        /**
         * <summary>Write sbyte array.</summary>
         */
        private static void WriteSbyteArray(IPortableWriterEx ctx, object obj)
        {
            ctx.Stream.WriteByte(PortableUtils.TYPE_ARRAY_BYTE);

            PortableUtils.WriteByteArray((byte[])(Array)obj, ctx.Stream);
        }

        /**
         * <summary>Write short array.</summary>
         */
        private static void WriteShortArray(IPortableWriterEx ctx, object obj)
        {
            WriteShortArrayTyped(ctx.Stream, (short[])obj);
        }

        /**
         * <summary>Write short array.</summary>
         */
        private static void WriteShortArrayTyped(IPortableStream stream, short[] obj)
        {
            stream.WriteByte(PortableUtils.TYPE_ARRAY_SHORT);

            PortableUtils.WriteShortArray(obj, stream);
        }

        /**
         * <summary>Write ushort array.</summary>
         */
        private static void WriteUshortArray(IPortableWriterEx ctx, object obj)
        {
            ctx.Stream.WriteByte(PortableUtils.TYPE_ARRAY_SHORT);

            PortableUtils.WriteShortArray((short[])(Array)obj, ctx.Stream);
        }

        /**
         * <summary>Write char array.</summary>
         */
        private static void WriteCharArray(IPortableWriterEx ctx, object obj)
        {
            WriteCharArrayTyped(ctx.Stream, (char[])obj);
        }

        /**
         * <summary>Write char array.</summary>
         */
        private static void WriteCharArrayTyped(IPortableStream stream, char[] obj)
        {
            stream.WriteByte(PortableUtils.TYPE_ARRAY_CHAR);

            PortableUtils.WriteCharArray(obj, stream);
        }

        /**
         * <summary>Write int array.</summary>
         */
        private static void WriteIntArray(IPortableWriterEx ctx, object obj)
        {
            WriteIntArrayTyped(ctx.Stream, (int[])obj);
        }

        /**
         * <summary>Write int array.</summary>
         */
        private static void WriteIntArrayTyped(IPortableStream stream, int[] obj)
        {
            stream.WriteByte(PortableUtils.TYPE_ARRAY_INT);

            PortableUtils.WriteIntArray(obj, stream);
        }

        /**
         * <summary>Write uint array.</summary>
         */
        private static void WriteUintArray(IPortableWriterEx ctx, object obj)
        {
            ctx.Stream.WriteByte(PortableUtils.TYPE_ARRAY_INT);

            PortableUtils.WriteIntArray((int[])(Array)obj, ctx.Stream);
        }

        /**
         * <summary>Write long array.</summary>
         */
        private static void WriteLongArray(IPortableWriterEx ctx, object obj)
        {
            WriteLongArrayTyped(ctx.Stream, (long[])obj);
        }

        /**
         * <summary>Write long array.</summary>
         */
        private static void WriteLongArrayTyped(IPortableStream stream, long[] obj)
        {
            stream.WriteByte(PortableUtils.TYPE_ARRAY_LONG);

            PortableUtils.WriteLongArray(obj, stream);
        }

        /**
         * <summary>Write ulong array.</summary>
         */
        private static void WriteUlongArray(IPortableWriterEx ctx, object obj)
        {
            ctx.Stream.WriteByte(PortableUtils.TYPE_ARRAY_LONG);

            PortableUtils.WriteLongArray((long[])(Array)obj, ctx.Stream);
        }

        /**
         * <summary>Write float array.</summary>
         */
        private static void WriteFloatArray(IPortableWriterEx ctx, object obj)
        {
            WriteFloatArrayTyped(ctx.Stream, (float[])obj);
        }

        /**
         * <summary>Write float array.</summary>
         */
        private static void WriteFloatArrayTyped(IPortableStream stream, float[] obj)
        {
            stream.WriteByte(PortableUtils.TYPE_ARRAY_FLOAT);

            PortableUtils.WriteFloatArray(obj, stream);
        }

        /**
         * <summary>Write double array.</summary>
         */
        private static void WriteDoubleArray(IPortableWriterEx ctx, object obj)
        {
            WriteDoubleArrayTyped(ctx.Stream, (double[])obj);
        }

        /**
         * <summary>Write double array.</summary>
         */
        private static void WriteDoubleArrayTyped(IPortableStream stream, double[] obj)
        {
            stream.WriteByte(PortableUtils.TYPE_ARRAY_DOUBLE);

            PortableUtils.WriteDoubleArray(obj, stream);
        }

        /**
         * <summary>Write decimal array.</summary>
         */
        private static void WriteDecimalArray(IPortableWriterEx ctx, object obj)
        {
            WriteDecimalArrayTyped(ctx.Stream, (decimal[])obj);
        }

        /**
         * <summary>Write double array.</summary>
         */
        private static void WriteDecimalArrayTyped(IPortableStream stream, decimal[] obj)
        {
            stream.WriteByte(PortableUtils.TYPE_ARRAY_DECIMAL);

            PortableUtils.WriteDecimalArray(obj, stream);
        }

        /**
         * <summary>Write date array.</summary>
         */
        private static void WriteDateArray(IPortableWriterEx ctx, object obj)
        {
            WriteDateArrayTyped(ctx.Stream, (DateTime?[])obj);
        }

        /**
         * <summary>Write date array.</summary>
         */
        private static void WriteDateArrayTyped(IPortableStream stream, DateTime?[] obj)
        {
            stream.WriteByte(PortableUtils.TYPE_ARRAY_DATE);

            PortableUtils.WriteDateArray(obj, stream);
        }

        /**
         * <summary>Write string array.</summary>
         */
        private static void WriteStringArray(IPortableWriterEx ctx, object obj)
        {
            WriteStringArrayTyped(ctx.Stream, (string[])obj);
        }

        /**
         * <summary>Write string array.</summary>
         */
        private static void WriteStringArrayTyped(IPortableStream stream, string[] obj)
        {
            stream.WriteByte(PortableUtils.TYPE_ARRAY_STRING);

            PortableUtils.WriteStringArray(obj, stream);
        }

        /**
         * <summary>Write Guid array.</summary>
         */
        private static void WriteGuidArray(IPortableWriterEx ctx, object obj)
        {
            WriteGuidArrayTyped(ctx.Stream, (Guid?[])obj);
        }

        /**
         * <summary>Write Guid array.</summary>
         */
        private static void WriteGuidArrayTyped(IPortableStream stream, Guid?[] obj)
        {
            stream.WriteByte(PortableUtils.TYPE_ARRAY_GUID);

            PortableUtils.WriteGuidArray(obj, stream);
        }

        /**
         * <summary>Write enum array.</summary>
         */
        private static void WriteEnumArray(IPortableWriterEx ctx, object obj)
        {
            ctx.Stream.WriteByte(PortableUtils.TYPE_ARRAY_ENUM);

            PortableUtils.WriteArray((Array)obj, ctx, true);
        }

        /**
         * <summary>Write array.</summary>
         */
        private static void WriteArray(IPortableWriterEx ctx, object obj)
        {
            ctx.Stream.WriteByte(PortableUtils.TYPE_ARRAY);

            PortableUtils.WriteArray((Array)obj, ctx, true);
        }

        /**
         * <summary>Write collection.</summary>
         */
        private static void WriteCollection(IPortableWriterEx ctx, object obj)
        {
            ctx.Stream.WriteByte(PortableUtils.TYPE_COLLECTION);

            PortableUtils.WriteCollection((ICollection)obj, ctx);
        }

        /**
         * <summary>Write generic collection.</summary>
         */
        private static void WriteGenericCollection(IPortableWriterEx ctx, object obj)
        {
            PortableCollectionInfo info = PortableCollectionInfo.Info(obj.GetType());

            Debug.Assert(info.IsGenericCollection, "Not generic collection: " + obj.GetType().FullName);

            ctx.Stream.WriteByte(PortableUtils.TYPE_COLLECTION);

            info.WriteGeneric(ctx, obj);
        }

        /**
         * <summary>Write dictionary.</summary>
         */
        private static void WriteDictionary(IPortableWriterEx ctx, object obj)
        {
            ctx.Stream.WriteByte(PortableUtils.TYPE_DICTIONARY);

            PortableUtils.WriteDictionary((IDictionary)obj, ctx);
        }

        /**
         * <summary>Write generic dictionary.</summary>
         */
        private static void WriteGenericDictionary(IPortableWriterEx ctx, object obj)
        {
            PortableCollectionInfo info = PortableCollectionInfo.Info(obj.GetType());

            Debug.Assert(info.IsGenericDictionary, "Not generic dictionary: " + obj.GetType().FullName);

            ctx.Stream.WriteByte(PortableUtils.TYPE_DICTIONARY);

            info.WriteGeneric(ctx, obj);
        }

        /**
         * <summary>Write ArrayList.</summary>
         */
        private static void WriteArrayList(IPortableWriterEx ctx, object obj)
        {
            ctx.Stream.WriteByte(PortableUtils.TYPE_COLLECTION);

            PortableUtils.WriteTypedCollection((ICollection)obj, ctx, PortableUtils.COLLECTION_ARRAY_LIST);
        }

        /**
         * <summary>Write Hashtable.</summary>
         */
        private static void WriteHashtable(IPortableWriterEx ctx, object obj)
        {
            ctx.Stream.WriteByte(PortableUtils.TYPE_DICTIONARY);

            PortableUtils.WriteTypedDictionary((IDictionary)obj, ctx, PortableUtils.MAP_HASH_MAP);
        }

        /**
         * <summary>Write map entry.</summary>
         */
        private static void WriteMapEntry(IPortableWriterEx ctx, object obj)
        {
            ctx.Stream.WriteByte(PortableUtils.TYPE_MAP_ENTRY);

            PortableUtils.WriteMapEntry(ctx, (DictionaryEntry)obj);
        }

        /**
         * <summary>Write portable object.</summary>
         */
        private static void WritePortable(IPortableWriterEx ctx, object obj)
        {
            ctx.Stream.WriteByte(PortableUtils.TYPE_PORTABLE);

            PortableUtils.WritePortable(ctx.Stream, (PortableUserObject)obj);
        }

        /**
         * <summary>Write portable object.</summary>
         */
        private static void WritePortableTyped(IPortableStream stream, PortableUserObject obj)
        {
            stream.WriteByte(PortableUtils.TYPE_PORTABLE);

            PortableUtils.WritePortable(stream, obj);
        }

        /// <summary>
        /// Write enum.
        /// </summary>
        private static void WriteEnum(IPortableWriterEx ctx, object obj)
        {
            ctx.Stream.WriteByte(PortableUtils.TYPE_ENUM);

            PortableUtils.WriteEnum(ctx.Stream, (Enum)obj);
        }

        /**
         * <summary>Read enum array.</summary>
         */
        private static object ReadEnumArray(IPortableReaderEx ctx, Type type)
        {
            return PortableUtils.ReadArray(ctx, true, type.GetElementType());
        }

        /**
         * <summary>Read array.</summary>
         */
        private static object ReadArray(IPortableReaderEx ctx, Type type)
        {
            var elemType = type.IsArray ? type.GetElementType() : typeof(object);

            return PortableUtils.ReadArray(ctx, true, elemType);
        }

        /**
         * <summary>Read collection.</summary>
         */
        private static object ReadCollection(IPortableReaderEx ctx, Type type)
        {
            PortableCollectionInfo info = PortableCollectionInfo.Info(type);

            return info.IsGenericCollection 
                ? info.ReadGeneric(ctx)
                : PortableUtils.ReadCollection(ctx, null, null);
        }

        /**
         * <summary>Read dictionary.</summary>
         */
        private static object ReadDictionary(IPortableReaderEx ctx, Type type)
        {
            PortableCollectionInfo info = PortableCollectionInfo.Info(type);

            return info.IsGenericDictionary
                ? info.ReadGeneric(ctx)
                : PortableUtils.ReadDictionary(ctx, null);
        }

        /**
         * <summary>Read map entry.</summary>
         */
        private static object ReadMapEntry(IPortableReaderEx ctx, Type type)
        {
            return PortableUtils.ReadMapEntry(ctx);
        }

        /**
         * <summary>Create new ArrayList.</summary>
         * <param name="len">Length.</param>
         * <returns>ArrayList.</returns>
         */
        public static ICollection CreateArrayList(int len)
        {
            return new ArrayList(len);
        }

        /**
         * <summary>Add element to array list.</summary>
         * <param name="col">Array list.</param>
         * <param name="elem">Element.</param>
         */
        public static void AddToArrayList(ICollection col, object elem)
        {
            ((ArrayList) col).Add(elem);
        }

        /**
         * <summary>Create new List.</summary>
         * <param name="len">Length.</param>
         * <returns>List.</returns>
         */
        public static ICollection<T> CreateList<T>(int len)
        {
            return new List<T>(len);
        }

        /**
         * <summary>Create new LinkedList.</summary>
         * <param name="len">Length.</param>
         * <returns>LinkedList.</returns>
         */
        public static ICollection<T> CreateLinkedList<T>(int len)
        {
            return new LinkedList<T>();
        }

        /**
         * <summary>Create new HashSet.</summary>
         * <param name="len">Length.</param>
         * <returns>HashSet.</returns>
         */
        public static ICollection<T> CreateHashSet<T>(int len)
        {
            return new HashSet<T>();
        }

        /**
         * <summary>Create new SortedSet.</summary>
         * <param name="len">Length.</param>
         * <returns>SortedSet.</returns>
         */
        public static ICollection<T> CreateSortedSet<T>(int len)
        {
            return new SortedSet<T>();
        }

        /**
         * <summary>Create new Hashtable.</summary>
         * <param name="len">Length.</param>
         * <returns>Hashtable.</returns>
         */
        public static IDictionary CreateHashtable(int len)
        {
            return new Hashtable(len);
        }

        /**
         * <summary>Create new Dictionary.</summary>
         * <param name="len">Length.</param>
         * <returns>Dictionary.</returns>
         */
        public static IDictionary<K, V> CreateDictionary<K, V>(int len)
        {
            return new Dictionary<K, V>(len);
        }

        /**
         * <summary>Create new SortedDictionary.</summary>
         * <param name="len">Length.</param>
         * <returns>SortedDictionary.</returns>
         */
        public static IDictionary<K, V> CreateSortedDictionary<K, V>(int len)
        {
            return new SortedDictionary<K, V>();
        }

        /**
         * <summary>Create new ConcurrentDictionary.</summary>
         * <param name="len">Length.</param>
         * <returns>ConcurrentDictionary.</returns>
         */
        public static IDictionary<K, V> CreateConcurrentDictionary<K, V>(int len)
        {
            return new ConcurrentDictionary<K, V>(Environment.ProcessorCount, len);
        }


        /**
         * <summary>Read delegate.</summary>
         * <param name="ctx">Read context.</param>
         * <param name="type">Type.</param>
         */
        private delegate object PortableSystemReadDelegate(IPortableReaderEx ctx, Type type);

        /// <summary>
        /// System type reader.
        /// </summary>
        private interface IPortableSystemReader
        {
            /// <summary>
            /// Reads a value of specified type from reader.
            /// </summary>
            T Read<T>(IPortableReaderEx ctx);
        }

        /// <summary>
        /// System type generic reader.
        /// </summary>
        private interface IPortableSystemReader<out T>
        {
            /// <summary>
            /// Reads a value of specified type from reader.
            /// </summary>
            T Read(IPortableReaderEx ctx);
        }

        /// <summary>
        /// Default reader with boxing.
        /// </summary>
        private class PortableSystemReader : IPortableSystemReader
        {
            /** */
            private readonly PortableSystemReadDelegate readDelegate;

            /// <summary>
            /// Initializes a new instance of the <see cref="PortableSystemReader"/> class.
            /// </summary>
            /// <param name="readDelegate">The read delegate.</param>
            public PortableSystemReader(PortableSystemReadDelegate readDelegate)
            {
                Debug.Assert(readDelegate != null);

                this.readDelegate = readDelegate;
            }

            /** <inheritdoc /> */
            public T Read<T>(IPortableReaderEx ctx)
            {
                return (T)readDelegate(ctx, typeof(T));
            }
        }

        /// <summary>
        /// Reader without boxing.
        /// </summary>
        private class PortableSystemReader<T> : IPortableSystemReader
        {
            /** */
            private readonly Func<IPortableStream, T> readDelegate;

            /// <summary>
            /// Initializes a new instance of the <see cref="PortableSystemReader{T}"/> class.
            /// </summary>
            /// <param name="readDelegate">The read delegate.</param>
            public PortableSystemReader(Func<IPortableStream, T> readDelegate)
            {
                Debug.Assert(readDelegate != null);

                this.readDelegate = readDelegate;
            }

            /** <inheritdoc /> */
            public TResult Read<TResult>(IPortableReaderEx ctx)
            {
                return TypeCaster<TResult>.Cast(readDelegate(ctx.Stream));
            }
        }

        /// <summary>
        /// Reader without boxing.
        /// </summary>
        private class PortableSystemGenericArrayReader<T> : IPortableSystemReader
        {
            public TResult Read<TResult>(IPortableReaderEx ctx)
            {
                return TypeCaster<TResult>.Cast(PortableUtils.ReadGenericArray<T>(ctx, false));
            }
        }

        /// <summary>
        /// Reader with selection based on requested type.
        /// </summary>
        private class PortableSystemDualReader<T1, T2> : IPortableSystemReader, IPortableSystemReader<T2>
        {
            /** */
            private readonly Func<IPortableStream, T1> readDelegate1;

            /** */
            private readonly Func<IPortableStream, T2> readDelegate2;

            /// <summary>
            /// Initializes a new instance of the <see cref="PortableSystemDualReader{T1, T2}"/> class.
            /// </summary>
            /// <param name="readDelegate1">The read delegate1.</param>
            /// <param name="readDelegate2">The read delegate2.</param>
            public PortableSystemDualReader(Func<IPortableStream, T1> readDelegate1, Func<IPortableStream, T2> readDelegate2)
            {
                Debug.Assert(readDelegate1 != null);
                Debug.Assert(readDelegate2 != null);

                this.readDelegate1 = readDelegate1;
                this.readDelegate2 = readDelegate2;
            }

            /** <inheritdoc /> */
            T2 IPortableSystemReader<T2>.Read(IPortableReaderEx ctx)
            {
                return readDelegate2(ctx.Stream);
            }

            /** <inheritdoc /> */
            public T Read<T>(IPortableReaderEx ctx)
            {
                // Can't use "as" because of variance. 
                // For example, IPortableSystemReader<byte[]> can be cast to IPortableSystemReader<sbyte[]>, which
                // will cause incorrect behavior.
                if (typeof (T) == typeof (T2))  
                    return ((IPortableSystemReader<T>) this).Read(ctx);

                return TypeCaster<T>.Cast(readDelegate1(ctx.Stream));
            }
        }
    }
}
