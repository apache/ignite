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

    /// <summary>
    /// Write delegate.
    /// </summary>
    /// <param name="writer">Write context.</param>
    /// <param name="obj">Object to write.</param>
    internal delegate void PortableSystemWriteDelegate(PortableWriterImpl writer, object obj);

    /// <summary>
    /// Typed write delegate.
    /// </summary>
    /// <param name="stream">Stream.</param>
    /// <param name="obj">Object to write.</param>
    // ReSharper disable once TypeParameterCanBeVariant
    // Generic variance in a delegate causes performance hit
    internal delegate void PortableSystemTypedWriteDelegate<T>(IPortableStream stream, T obj);

    /**
     * <summary>Collection of predefined handlers for various system types.</summary>
     */
    internal static class PortableSystemHandlers
    {
        /** Write handlers. */
        private static readonly Dictionary<Type, PortableSystemWriteDelegate> WriteHandlers =
            new Dictionary<Type, PortableSystemWriteDelegate>();

        /** Read handlers. */
        private static readonly IPortableSystemReader[] ReadHandlers = new IPortableSystemReader[255];

        /** Typed write handler: boolean. */
        public static readonly PortableSystemTypedWriteDelegate<bool> WriteHndBoolTyped = WriteBoolTyped;

        /** Typed write handler: byte. */
        public static readonly PortableSystemTypedWriteDelegate<byte> WriteHndByteTyped = WriteByteTyped;

        /** Typed write handler: short. */
        public static readonly PortableSystemTypedWriteDelegate<short> WriteHndShortTyped = WriteShortTyped;

        /** Typed write handler: char. */
        public static readonly PortableSystemTypedWriteDelegate<char> WriteHndCharTyped = WriteCharTyped;

        /** Typed write handler: int. */
        public static readonly PortableSystemTypedWriteDelegate<int> WriteHndIntTyped = WriteIntTyped;

        /** Typed write handler: long. */
        public static readonly PortableSystemTypedWriteDelegate<long> WriteHndLongTyped = WriteLongTyped;

        /** Typed write handler: float. */
        public static readonly PortableSystemTypedWriteDelegate<float> WriteHndFloatTyped = WriteFloatTyped;

        /** Typed write handler: double. */
        public static readonly PortableSystemTypedWriteDelegate<double> WriteHndDoubleTyped = WriteDoubleTyped;

        /** Typed write handler: decimal. */
        public static readonly PortableSystemTypedWriteDelegate<decimal> WriteHndDecimalTyped = WriteDecimalTyped;

        /** Typed write handler: Date. */
        public static readonly PortableSystemTypedWriteDelegate<DateTime?> WriteHndDateTyped = WriteDateTyped;

        /** Typed write handler: string. */
        public static readonly PortableSystemTypedWriteDelegate<string> WriteHndStringTyped = WriteStringTyped;

        /** Typed write handler: Guid. */
        public static readonly PortableSystemTypedWriteDelegate<Guid?> WriteHndGuidTyped = WriteGuidTyped;

        /** Typed write handler: Portable. */
        public static readonly PortableSystemTypedWriteDelegate<PortableUserObject> WriteHndPortableTyped = WritePortableTyped;

        /** Typed write handler: boolean array. */
        public static readonly PortableSystemTypedWriteDelegate<bool[]> WriteHndBoolArrayTyped = WriteBoolArrayTyped;

        /** Typed write handler: byte array. */
        public static readonly PortableSystemTypedWriteDelegate<byte[]> WriteHndByteArrayTyped = WriteByteArrayTyped;

        /** Typed write handler: short array. */
        public static readonly PortableSystemTypedWriteDelegate<short[]> WriteHndShortArrayTyped = WriteShortArrayTyped;

        /** Typed write handler: char array. */
        public static readonly PortableSystemTypedWriteDelegate<char[]> WriteHndCharArrayTyped = WriteCharArrayTyped;

        /** Typed write handler: int array. */
        public static readonly PortableSystemTypedWriteDelegate<int[]> WriteHndIntArrayTyped = WriteIntArrayTyped;

        /** Typed write handler: long array. */
        public static readonly PortableSystemTypedWriteDelegate<long[]> WriteHndLongArrayTyped = WriteLongArrayTyped;

        /** Typed write handler: float array. */
        public static readonly PortableSystemTypedWriteDelegate<float[]> WriteHndFloatArrayTyped = WriteFloatArrayTyped;

        /** Typed write handler: double array. */
        public static readonly PortableSystemTypedWriteDelegate<double[]> WriteHndDoubleArrayTyped = WriteDoubleArrayTyped;

        /** Typed write handler: decimal array. */
        public static readonly PortableSystemTypedWriteDelegate<decimal[]> WriteHndDecimalArrayTyped = WriteDecimalArrayTyped;

        /** Typed write handler: Date array. */
        public static readonly PortableSystemTypedWriteDelegate<DateTime?[]> WriteHndDateArrayTyped = WriteDateArrayTyped;

        /** Typed write handler: string array. */
        public static readonly PortableSystemTypedWriteDelegate<string[]> WriteHndStringArrayTyped = WriteStringArrayTyped;

        /** Typed write handler: Guid array. */
        public static readonly PortableSystemTypedWriteDelegate<Guid?[]> WriteHndGuidArrayTyped = WriteGuidArrayTyped;

        /** Write handler: boolean. */
        public static readonly PortableSystemWriteDelegate WriteHndBool = WriteBool;

        /** Write handler: sbyte. */
        public static readonly PortableSystemWriteDelegate WriteHndSbyte = WriteSbyte;

        /** Write handler: byte. */
        public static readonly PortableSystemWriteDelegate WriteHndByte = WriteByte;

        /** Write handler: short. */
        public static readonly PortableSystemWriteDelegate WriteHndShort = WriteShort;

        /** Write handler: ushort. */
        public static readonly PortableSystemWriteDelegate WriteHndUshort = WriteUshort;

        /** Write handler: char. */
        public static readonly PortableSystemWriteDelegate WriteHndChar = WriteChar;

        /** Write handler: int. */
        public static readonly PortableSystemWriteDelegate WriteHndInt = WriteInt;

        /** Write handler: uint. */
        public static readonly PortableSystemWriteDelegate WriteHndUint = WriteUint;

        /** Write handler: long. */
        public static readonly PortableSystemWriteDelegate WriteHndLong = WriteLong;

        /** Write handler: ulong. */
        public static readonly PortableSystemWriteDelegate WriteHndUlong = WriteUlong;

        /** Write handler: float. */
        public static readonly PortableSystemWriteDelegate WriteHndFloat = WriteFloat;

        /** Write handler: double. */
        public static readonly PortableSystemWriteDelegate WriteHndDouble = WriteDouble;

        /** Write handler: decimal. */
        public static readonly PortableSystemWriteDelegate WriteHndDecimal = WriteDecimal;

        /** Write handler: Date. */
        public static readonly PortableSystemWriteDelegate WriteHndDate = WriteDate;

        /** Write handler: string. */
        public static readonly PortableSystemWriteDelegate WriteHndString = WriteString;

        /** Write handler: Guid. */
        public static readonly PortableSystemWriteDelegate WriteHndGuid = WriteGuid;

        /** Write handler: Portable. */
        public static readonly PortableSystemWriteDelegate WriteHndPortable = WritePortable;

        /** Write handler: Enum. */
        public static readonly PortableSystemWriteDelegate WriteHndEnum = WriteEnum;

        /** Write handler: boolean array. */
        public static readonly PortableSystemWriteDelegate WriteHndBoolArray = WriteBoolArray;

        /** Write handler: sbyte array. */
        public static readonly PortableSystemWriteDelegate WriteHndSbyteArray = WriteSbyteArray;

        /** Write handler: byte array. */
        public static readonly PortableSystemWriteDelegate WriteHndByteArray = WriteByteArray;

        /** Write handler: short array. */
        public static readonly PortableSystemWriteDelegate WriteHndShortArray = WriteShortArray;

        /** Write handler: ushort array. */
        public static readonly PortableSystemWriteDelegate WriteHndUshortArray = WriteUshortArray;

        /** Write handler: char array. */
        public static readonly PortableSystemWriteDelegate WriteHndCharArray = WriteCharArray;

        /** Write handler: int array. */
        public static readonly PortableSystemWriteDelegate WriteHndIntArray = WriteIntArray;

        /** Write handler: uint array. */
        public static readonly PortableSystemWriteDelegate WriteHndUintArray = WriteUintArray;

        /** Write handler: long array. */
        public static readonly PortableSystemWriteDelegate WriteHndLongArray = WriteLongArray;

        /** Write handler: ulong array. */
        public static readonly PortableSystemWriteDelegate WriteHndUlongArray = WriteUlongArray;

        /** Write handler: float array. */
        public static readonly PortableSystemWriteDelegate WriteHndFloatArray = WriteFloatArray;

        /** Write handler: double array. */
        public static readonly PortableSystemWriteDelegate WriteHndDoubleArray = WriteDoubleArray;

        /** Write handler: decimal array. */
        public static readonly PortableSystemWriteDelegate WriteHndDecimalArray = WriteDecimalArray;

        /** Write handler: date array. */
        public static readonly PortableSystemWriteDelegate WriteHndDateArray = WriteDateArray;

        /** Write handler: string array. */
        public static readonly PortableSystemWriteDelegate WriteHndStringArray = WriteStringArray;

        /** Write handler: Guid array. */
        public static readonly PortableSystemWriteDelegate WriteHndGuidArray = WriteGuidArray;

        /** Write handler: Enum array. */
        public static readonly PortableSystemWriteDelegate WriteHndEnumArray = WriteEnumArray;

        /** Write handler: object array. */
        public static readonly PortableSystemWriteDelegate WriteHndArray = WriteArray;

        /** Write handler: collection. */
        public static readonly PortableSystemWriteDelegate WriteHndCollection = WriteCollection;

        /** Write handler: dictionary. */
        public static readonly PortableSystemWriteDelegate WriteHndDictionary = WriteDictionary;

        /** Write handler: generic collection. */
        public static readonly PortableSystemWriteDelegate WriteHndGenericCollection =
            WriteGenericCollection;

        /** Write handler: generic dictionary. */
        public static readonly PortableSystemWriteDelegate WriteHndGenericDictionary =
            WriteGenericDictionary;

        /**
         * <summary>Static initializer.</summary>
         */
        static PortableSystemHandlers()
        {
            // 1. Primitives.

            ReadHandlers[PortableUtils.TypeBool] = new PortableSystemReader<bool>(s => s.ReadBool());

            WriteHandlers[typeof(sbyte)] = WriteHndSbyte;
            ReadHandlers[PortableUtils.TypeByte] = new PortableSystemReader<byte>(s => s.ReadByte());

            WriteHandlers[typeof(ushort)] = WriteHndUshort;
            ReadHandlers[PortableUtils.TypeShort] = new PortableSystemReader<short>(s => s.ReadShort());

            ReadHandlers[PortableUtils.TypeChar] = new PortableSystemReader<char>(s => s.ReadChar());

            WriteHandlers[typeof(uint)] = WriteHndUint;
            ReadHandlers[PortableUtils.TypeInt] = new PortableSystemReader<int>(s => s.ReadInt());

            WriteHandlers[typeof(ulong)] = WriteHndUlong;
            ReadHandlers[PortableUtils.TypeLong] = new PortableSystemReader<long>(s => s.ReadLong());

            ReadHandlers[PortableUtils.TypeFloat] = new PortableSystemReader<float>(s => s.ReadFloat());

            ReadHandlers[PortableUtils.TypeDouble] = new PortableSystemReader<double>(s => s.ReadDouble());

            ReadHandlers[PortableUtils.TypeDecimal] = new PortableSystemReader<decimal>(PortableUtils.ReadDecimal);

            // 2. Date.
            ReadHandlers[PortableUtils.TypeDate] =
                new PortableSystemReader<DateTime?>(s => PortableUtils.ReadDate(s, false));

            // 3. String.
            ReadHandlers[PortableUtils.TypeString] = new PortableSystemReader<string>(PortableUtils.ReadString);

            // 4. Guid.
            ReadHandlers[PortableUtils.TypeGuid] = new PortableSystemReader<Guid?>(PortableUtils.ReadGuid);

            // 5. Primitive arrays.
            ReadHandlers[PortableUtils.TypeArrayBool] = new PortableSystemReader<bool[]>(PortableUtils.ReadBooleanArray);

            WriteHandlers[typeof(sbyte[])] = WriteHndSbyteArray;
            ReadHandlers[PortableUtils.TypeArrayByte] =
                new PortableSystemDualReader<byte[], sbyte[]>(PortableUtils.ReadByteArray, PortableUtils.ReadSbyteArray);

            WriteHandlers[typeof(ushort[])] = WriteHndUshortArray;
            ReadHandlers[PortableUtils.TypeArrayShort] =
                new PortableSystemDualReader<short[], ushort[]>(PortableUtils.ReadShortArray,
                    PortableUtils.ReadUshortArray);

            ReadHandlers[PortableUtils.TypeArrayChar] = 
                new PortableSystemReader<char[]>(PortableUtils.ReadCharArray);

            WriteHandlers[typeof(uint[])] = WriteHndUintArray;
            ReadHandlers[PortableUtils.TypeArrayInt] =
                new PortableSystemDualReader<int[], uint[]>(PortableUtils.ReadIntArray, PortableUtils.ReadUintArray);


            WriteHandlers[typeof(ulong[])] = WriteHndUlongArray;
            ReadHandlers[PortableUtils.TypeArrayLong] =
                new PortableSystemDualReader<long[], ulong[]>(PortableUtils.ReadLongArray, 
                    PortableUtils.ReadUlongArray);

            ReadHandlers[PortableUtils.TypeArrayFloat] =
                new PortableSystemReader<float[]>(PortableUtils.ReadFloatArray);

            ReadHandlers[PortableUtils.TypeArrayDouble] =
                new PortableSystemReader<double[]>(PortableUtils.ReadDoubleArray);

            ReadHandlers[PortableUtils.TypeArrayDecimal] =
                new PortableSystemReader<decimal[]>(PortableUtils.ReadDecimalArray);

            // 6. Date array.
            ReadHandlers[PortableUtils.TypeArrayDate] =
                new PortableSystemReader<DateTime?[]>(s => PortableUtils.ReadDateArray(s, false));

            // 7. String array.
            ReadHandlers[PortableUtils.TypeArrayString] = new PortableSystemGenericArrayReader<string>();

            // 8. Guid array.
            ReadHandlers[PortableUtils.TypeArrayGuid] = new PortableSystemGenericArrayReader<Guid?>();

            // 9. Array.
            ReadHandlers[PortableUtils.TypeArray] = new PortableSystemReader(ReadArray);

            // 10. Predefined collections.
            WriteHandlers[typeof(ArrayList)] = WriteArrayList;

            // 11. Predefined dictionaries.
            WriteHandlers[typeof(Hashtable)] = WriteHashtable;

            // 12. Arbitrary collection.
            ReadHandlers[PortableUtils.TypeCollection] = new PortableSystemReader(ReadCollection);

            // 13. Arbitrary dictionary.
            ReadHandlers[PortableUtils.TypeDictionary] = new PortableSystemReader(ReadDictionary);

            // 14. Map entry.
            WriteHandlers[typeof(DictionaryEntry)] = WriteMapEntry;
            ReadHandlers[PortableUtils.TypeMapEntry] = new PortableSystemReader(ReadMapEntry);

            // 15. Portable.
            WriteHandlers[typeof(PortableUserObject)] = WritePortable;

            // 16. Enum.
            ReadHandlers[PortableUtils.TypeEnum] = new PortableSystemReader<int>(PortableUtils.ReadEnum<int>);
            ReadHandlers[PortableUtils.TypeArrayEnum] = new PortableSystemReader(ReadEnumArray);
        }

        /**
         * <summary>Get write handler for type.</summary>
         * <param name="type">Type.</param>
         * <returns>Handler or null if cannot be hanled in special way.</returns>
         */
        public static PortableSystemWriteDelegate WriteHandler(Type type)
        {
            PortableSystemWriteDelegate handler;

            if (WriteHandlers.TryGetValue(type, out handler))
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
        public static T ReadSystemType<T>(byte typeId, PortableReaderImpl ctx)
        {
            var handler = ReadHandlers[typeId];

            Debug.Assert(handler != null, "Cannot find predefined read handler: " + typeId);
            
            return handler.Read<T>(ctx);
        }

        /**
         * <summary>Write boolean.</summary>
         */
        private static void WriteBool(PortableWriterImpl ctx, object obj)
        {
            WriteBoolTyped(ctx.Stream, (bool)obj);
        }

        /**
         * <summary>Write boolean.</summary>
         */
        private static void WriteBoolTyped(IPortableStream stream, bool obj)
        {
            stream.WriteByte(PortableUtils.TypeBool);

            stream.WriteBool(obj);
        }

        /**
         * <summary>Write sbyte.</summary>
         */
        private static unsafe void WriteSbyte(PortableWriterImpl ctx, object obj)
        {
            sbyte val = (sbyte)obj;

            ctx.Stream.WriteByte(PortableUtils.TypeByte);
            ctx.Stream.WriteByte(*(byte*)&val);
        }

        /**
         * <summary>Write byte.</summary>
         */
        private static void WriteByte(PortableWriterImpl ctx, object obj)
        {
            WriteByteTyped(ctx.Stream, (byte)obj);
        }

        /**
         * <summary>Write byte.</summary>
         */
        private static void WriteByteTyped(IPortableStream stream, byte obj)
        {
            stream.WriteByte(PortableUtils.TypeByte);
            stream.WriteByte(obj);
        }

        /**
         * <summary>Write short.</summary>
         */
        private static void WriteShort(PortableWriterImpl ctx, object obj)
        {
            WriteShortTyped(ctx.Stream, (short)obj);
        }

        /**
         * <summary>Write short.</summary>
         */
        private static void WriteShortTyped(IPortableStream stream, short obj)
        {
            stream.WriteByte(PortableUtils.TypeShort);

            stream.WriteShort(obj);
        }

        /**
         * <summary>Write ushort.</summary>
         */
        private static unsafe void WriteUshort(PortableWriterImpl ctx, object obj)
        {
            ushort val = (ushort)obj;

            ctx.Stream.WriteByte(PortableUtils.TypeShort);

            ctx.Stream.WriteShort(*(short*)&val);
        }

        /**
         * <summary>Write char.</summary>
         */
        private static void WriteChar(PortableWriterImpl ctx, object obj)
        {
            WriteCharTyped(ctx.Stream, (char)obj);
        }

        /**
         * <summary>Write char.</summary>
         */
        private static void WriteCharTyped(IPortableStream stream, char obj)
        {
            stream.WriteByte(PortableUtils.TypeChar);

            stream.WriteChar(obj);
        }

        /**
         * <summary>Write int.</summary>
         */
        private static void WriteInt(PortableWriterImpl ctx, object obj)
        {
            WriteIntTyped(ctx.Stream, (int)obj);
        }

        /**
         * <summary>Write int.</summary>
         */
        private static void WriteIntTyped(IPortableStream stream, int obj)
        {
            stream.WriteByte(PortableUtils.TypeInt);
            stream.WriteInt(obj);
        }

        /**
         * <summary>Write uint.</summary>
         */
        private static unsafe void WriteUint(PortableWriterImpl ctx, object obj)
        {
            uint val = (uint)obj;

            ctx.Stream.WriteByte(PortableUtils.TypeInt);
            ctx.Stream.WriteInt(*(int*)&val);
        }

        /**
         * <summary>Write long.</summary>
         */
        private static void WriteLong(PortableWriterImpl ctx, object obj)
        {
            WriteLongTyped(ctx.Stream, (long)obj);
        }

        /**
         * <summary>Write long.</summary>
         */
        private static void WriteLongTyped(IPortableStream stream, long obj)
        {
            stream.WriteByte(PortableUtils.TypeLong);
            stream.WriteLong(obj);
        }

        /**
         * <summary>Write ulong.</summary>
         */
        private static unsafe void WriteUlong(PortableWriterImpl ctx, object obj)
        {
            ulong val = (ulong)obj;

            ctx.Stream.WriteByte(PortableUtils.TypeLong);
            ctx.Stream.WriteLong(*(long*)&val);
        }

        /**
         * <summary>Write float.</summary>
         */
        private static void WriteFloat(PortableWriterImpl ctx, object obj)
        {
            WriteFloatTyped(ctx.Stream, (float)obj);
        }

        /**
         * <summary>Write float.</summary>
         */
        private static void WriteFloatTyped(IPortableStream stream, float obj)
        {
            stream.WriteByte(PortableUtils.TypeFloat);
            stream.WriteFloat(obj);
        }

        /**
         * <summary>Write double.</summary>
         */
        private static void WriteDouble(PortableWriterImpl ctx, object obj)
        {
            WriteDoubleTyped(ctx.Stream, (double)obj);
        }

        /**
         * <summary>Write double.</summary>
         */
        private static void WriteDoubleTyped(IPortableStream stream, double obj)
        {
            stream.WriteByte(PortableUtils.TypeDouble);
            stream.WriteDouble(obj);
        }

        /**
         * <summary>Write decimal.</summary>
         */
        private static void WriteDecimal(PortableWriterImpl ctx, object obj)
        {
            WriteDecimalTyped(ctx.Stream, (decimal)obj);
        }

        /**
         * <summary>Write double.</summary>
         */
        private static void WriteDecimalTyped(IPortableStream stream, decimal obj)
        {
            stream.WriteByte(PortableUtils.TypeDecimal);

            PortableUtils.WriteDecimal(obj, stream);
        }

        /**
         * <summary>Write date.</summary>
         */
        private static void WriteDate(PortableWriterImpl ctx, object obj)
        {
            WriteDateTyped(ctx.Stream, (DateTime?)obj);
        }

        /**
         * <summary>Write double.</summary>
         */
        private static void WriteDateTyped(IPortableStream stream, DateTime? obj)
        {
            stream.WriteByte(PortableUtils.TypeDate);

            PortableUtils.WriteDate(obj, stream);
        }

        /**
         * <summary>Write string.</summary>
         */
        private static void WriteString(PortableWriterImpl ctx, object obj)
        {
            WriteStringTyped(ctx.Stream, (string)obj);
        }

        /**
         * <summary>Write string.</summary>
         */
        private static void WriteStringTyped(IPortableStream stream, string obj)
        {
            stream.WriteByte(PortableUtils.TypeString);

            PortableUtils.WriteString(obj, stream);
        }

        /**
         * <summary>Write Guid.</summary>
         */
        private static void WriteGuid(PortableWriterImpl ctx, object obj)
        {
            WriteGuidTyped(ctx.Stream, (Guid?)obj);
        }

        /**
         * <summary>Write Guid.</summary>
         */
        private static void WriteGuidTyped(IPortableStream stream, Guid? obj)
        {
            stream.WriteByte(PortableUtils.TypeGuid);

            PortableUtils.WriteGuid(obj, stream);
        }

        /**
         * <summary>Write bool array.</summary>
         */
        private static void WriteBoolArray(PortableWriterImpl ctx, object obj)
        {
            WriteBoolArrayTyped(ctx.Stream, (bool[])obj);
        }

        /**
         * <summary>Write bool array.</summary>
         */
        private static void WriteBoolArrayTyped(IPortableStream stream, bool[] obj)
        {
            stream.WriteByte(PortableUtils.TypeArrayBool);

            PortableUtils.WriteBooleanArray(obj, stream);
        }

        /**
         * <summary>Write byte array.</summary>
         */
        private static void WriteByteArray(PortableWriterImpl ctx, object obj)
        {
            WriteByteArrayTyped(ctx.Stream, (byte[])obj);
        }

        /**
         * <summary>Write byte array.</summary>
         */
        private static void WriteByteArrayTyped(IPortableStream stream, byte[] obj)
        {
            stream.WriteByte(PortableUtils.TypeArrayByte);

            PortableUtils.WriteByteArray(obj, stream);
        }

        /**
         * <summary>Write sbyte array.</summary>
         */
        private static void WriteSbyteArray(PortableWriterImpl ctx, object obj)
        {
            ctx.Stream.WriteByte(PortableUtils.TypeArrayByte);

            PortableUtils.WriteByteArray((byte[])(Array)obj, ctx.Stream);
        }

        /**
         * <summary>Write short array.</summary>
         */
        private static void WriteShortArray(PortableWriterImpl ctx, object obj)
        {
            WriteShortArrayTyped(ctx.Stream, (short[])obj);
        }

        /**
         * <summary>Write short array.</summary>
         */
        private static void WriteShortArrayTyped(IPortableStream stream, short[] obj)
        {
            stream.WriteByte(PortableUtils.TypeArrayShort);

            PortableUtils.WriteShortArray(obj, stream);
        }

        /**
         * <summary>Write ushort array.</summary>
         */
        private static void WriteUshortArray(PortableWriterImpl ctx, object obj)
        {
            ctx.Stream.WriteByte(PortableUtils.TypeArrayShort);

            PortableUtils.WriteShortArray((short[])(Array)obj, ctx.Stream);
        }

        /**
         * <summary>Write char array.</summary>
         */
        private static void WriteCharArray(PortableWriterImpl ctx, object obj)
        {
            WriteCharArrayTyped(ctx.Stream, (char[])obj);
        }

        /**
         * <summary>Write char array.</summary>
         */
        private static void WriteCharArrayTyped(IPortableStream stream, char[] obj)
        {
            stream.WriteByte(PortableUtils.TypeArrayChar);

            PortableUtils.WriteCharArray(obj, stream);
        }

        /**
         * <summary>Write int array.</summary>
         */
        private static void WriteIntArray(PortableWriterImpl ctx, object obj)
        {
            WriteIntArrayTyped(ctx.Stream, (int[])obj);
        }

        /**
         * <summary>Write int array.</summary>
         */
        private static void WriteIntArrayTyped(IPortableStream stream, int[] obj)
        {
            stream.WriteByte(PortableUtils.TypeArrayInt);

            PortableUtils.WriteIntArray(obj, stream);
        }

        /**
         * <summary>Write uint array.</summary>
         */
        private static void WriteUintArray(PortableWriterImpl ctx, object obj)
        {
            ctx.Stream.WriteByte(PortableUtils.TypeArrayInt);

            PortableUtils.WriteIntArray((int[])(Array)obj, ctx.Stream);
        }

        /**
         * <summary>Write long array.</summary>
         */
        private static void WriteLongArray(PortableWriterImpl ctx, object obj)
        {
            WriteLongArrayTyped(ctx.Stream, (long[])obj);
        }

        /**
         * <summary>Write long array.</summary>
         */
        private static void WriteLongArrayTyped(IPortableStream stream, long[] obj)
        {
            stream.WriteByte(PortableUtils.TypeArrayLong);

            PortableUtils.WriteLongArray(obj, stream);
        }

        /**
         * <summary>Write ulong array.</summary>
         */
        private static void WriteUlongArray(PortableWriterImpl ctx, object obj)
        {
            ctx.Stream.WriteByte(PortableUtils.TypeArrayLong);

            PortableUtils.WriteLongArray((long[])(Array)obj, ctx.Stream);
        }

        /**
         * <summary>Write float array.</summary>
         */
        private static void WriteFloatArray(PortableWriterImpl ctx, object obj)
        {
            WriteFloatArrayTyped(ctx.Stream, (float[])obj);
        }

        /**
         * <summary>Write float array.</summary>
         */
        private static void WriteFloatArrayTyped(IPortableStream stream, float[] obj)
        {
            stream.WriteByte(PortableUtils.TypeArrayFloat);

            PortableUtils.WriteFloatArray(obj, stream);
        }

        /**
         * <summary>Write double array.</summary>
         */
        private static void WriteDoubleArray(PortableWriterImpl ctx, object obj)
        {
            WriteDoubleArrayTyped(ctx.Stream, (double[])obj);
        }

        /**
         * <summary>Write double array.</summary>
         */
        private static void WriteDoubleArrayTyped(IPortableStream stream, double[] obj)
        {
            stream.WriteByte(PortableUtils.TypeArrayDouble);

            PortableUtils.WriteDoubleArray(obj, stream);
        }

        /**
         * <summary>Write decimal array.</summary>
         */
        private static void WriteDecimalArray(PortableWriterImpl ctx, object obj)
        {
            WriteDecimalArrayTyped(ctx.Stream, (decimal[])obj);
        }

        /**
         * <summary>Write double array.</summary>
         */
        private static void WriteDecimalArrayTyped(IPortableStream stream, decimal[] obj)
        {
            stream.WriteByte(PortableUtils.TypeArrayDecimal);

            PortableUtils.WriteDecimalArray(obj, stream);
        }

        /**
         * <summary>Write date array.</summary>
         */
        private static void WriteDateArray(PortableWriterImpl ctx, object obj)
        {
            WriteDateArrayTyped(ctx.Stream, (DateTime?[])obj);
        }

        /**
         * <summary>Write date array.</summary>
         */
        private static void WriteDateArrayTyped(IPortableStream stream, DateTime?[] obj)
        {
            stream.WriteByte(PortableUtils.TypeArrayDate);

            PortableUtils.WriteDateArray(obj, stream);
        }

        /**
         * <summary>Write string array.</summary>
         */
        private static void WriteStringArray(PortableWriterImpl ctx, object obj)
        {
            WriteStringArrayTyped(ctx.Stream, (string[])obj);
        }

        /**
         * <summary>Write string array.</summary>
         */
        private static void WriteStringArrayTyped(IPortableStream stream, string[] obj)
        {
            stream.WriteByte(PortableUtils.TypeArrayString);

            PortableUtils.WriteStringArray(obj, stream);
        }

        /**
         * <summary>Write Guid array.</summary>
         */
        private static void WriteGuidArray(PortableWriterImpl ctx, object obj)
        {
            WriteGuidArrayTyped(ctx.Stream, (Guid?[])obj);
        }

        /**
         * <summary>Write Guid array.</summary>
         */
        private static void WriteGuidArrayTyped(IPortableStream stream, Guid?[] obj)
        {
            stream.WriteByte(PortableUtils.TypeArrayGuid);

            PortableUtils.WriteGuidArray(obj, stream);
        }

        /**
         * <summary>Write enum array.</summary>
         */
        private static void WriteEnumArray(PortableWriterImpl ctx, object obj)
        {
            ctx.Stream.WriteByte(PortableUtils.TypeArrayEnum);

            PortableUtils.WriteArray((Array)obj, ctx, true);
        }

        /**
         * <summary>Write array.</summary>
         */
        private static void WriteArray(PortableWriterImpl ctx, object obj)
        {
            ctx.Stream.WriteByte(PortableUtils.TypeArray);

            PortableUtils.WriteArray((Array)obj, ctx, true);
        }

        /**
         * <summary>Write collection.</summary>
         */
        private static void WriteCollection(PortableWriterImpl ctx, object obj)
        {
            ctx.Stream.WriteByte(PortableUtils.TypeCollection);

            PortableUtils.WriteCollection((ICollection)obj, ctx);
        }

        /**
         * <summary>Write generic collection.</summary>
         */
        private static void WriteGenericCollection(PortableWriterImpl ctx, object obj)
        {
            PortableCollectionInfo info = PortableCollectionInfo.Info(obj.GetType());

            Debug.Assert(info.IsGenericCollection, "Not generic collection: " + obj.GetType().FullName);

            ctx.Stream.WriteByte(PortableUtils.TypeCollection);

            info.WriteGeneric(ctx, obj);
        }

        /**
         * <summary>Write dictionary.</summary>
         */
        private static void WriteDictionary(PortableWriterImpl ctx, object obj)
        {
            ctx.Stream.WriteByte(PortableUtils.TypeDictionary);

            PortableUtils.WriteDictionary((IDictionary)obj, ctx);
        }

        /**
         * <summary>Write generic dictionary.</summary>
         */
        private static void WriteGenericDictionary(PortableWriterImpl ctx, object obj)
        {
            PortableCollectionInfo info = PortableCollectionInfo.Info(obj.GetType());

            Debug.Assert(info.IsGenericDictionary, "Not generic dictionary: " + obj.GetType().FullName);

            ctx.Stream.WriteByte(PortableUtils.TypeDictionary);

            info.WriteGeneric(ctx, obj);
        }

        /**
         * <summary>Write ArrayList.</summary>
         */
        private static void WriteArrayList(PortableWriterImpl ctx, object obj)
        {
            ctx.Stream.WriteByte(PortableUtils.TypeCollection);

            PortableUtils.WriteTypedCollection((ICollection)obj, ctx, PortableUtils.CollectionArrayList);
        }

        /**
         * <summary>Write Hashtable.</summary>
         */
        private static void WriteHashtable(PortableWriterImpl ctx, object obj)
        {
            ctx.Stream.WriteByte(PortableUtils.TypeDictionary);

            PortableUtils.WriteTypedDictionary((IDictionary)obj, ctx, PortableUtils.MapHashMap);
        }

        /**
         * <summary>Write map entry.</summary>
         */
        private static void WriteMapEntry(PortableWriterImpl ctx, object obj)
        {
            ctx.Stream.WriteByte(PortableUtils.TypeMapEntry);

            PortableUtils.WriteMapEntry(ctx, (DictionaryEntry)obj);
        }

        /**
         * <summary>Write portable object.</summary>
         */
        private static void WritePortable(PortableWriterImpl ctx, object obj)
        {
            ctx.Stream.WriteByte(PortableUtils.TypePortable);

            PortableUtils.WritePortable(ctx.Stream, (PortableUserObject)obj);
        }

        /**
         * <summary>Write portable object.</summary>
         */
        private static void WritePortableTyped(IPortableStream stream, PortableUserObject obj)
        {
            stream.WriteByte(PortableUtils.TypePortable);

            PortableUtils.WritePortable(stream, obj);
        }

        /// <summary>
        /// Write enum.
        /// </summary>
        private static void WriteEnum(PortableWriterImpl ctx, object obj)
        {
            ctx.Stream.WriteByte(PortableUtils.TypeEnum);

            PortableUtils.WriteEnum(ctx.Stream, (Enum)obj);
        }

        /**
         * <summary>Read enum array.</summary>
         */
        private static object ReadEnumArray(PortableReaderImpl ctx, Type type)
        {
            return PortableUtils.ReadArray(ctx, true, type.GetElementType());
        }

        /**
         * <summary>Read array.</summary>
         */
        private static object ReadArray(PortableReaderImpl ctx, Type type)
        {
            var elemType = type.IsArray ? type.GetElementType() : typeof(object);

            return PortableUtils.ReadArray(ctx, true, elemType);
        }

        /**
         * <summary>Read collection.</summary>
         */
        private static object ReadCollection(PortableReaderImpl ctx, Type type)
        {
            PortableCollectionInfo info = PortableCollectionInfo.Info(type);

            return info.IsGenericCollection 
                ? info.ReadGeneric(ctx)
                : PortableUtils.ReadCollection(ctx, null, null);
        }

        /**
         * <summary>Read dictionary.</summary>
         */
        private static object ReadDictionary(PortableReaderImpl ctx, Type type)
        {
            PortableCollectionInfo info = PortableCollectionInfo.Info(type);

            return info.IsGenericDictionary
                ? info.ReadGeneric(ctx)
                : PortableUtils.ReadDictionary(ctx, null);
        }

        /**
         * <summary>Read map entry.</summary>
         */
        private static object ReadMapEntry(PortableReaderImpl ctx, Type type)
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
        public static IDictionary<TK, TV> CreateDictionary<TK, TV>(int len)
        {
            return new Dictionary<TK, TV>(len);
        }

        /**
         * <summary>Create new SortedDictionary.</summary>
         * <param name="len">Length.</param>
         * <returns>SortedDictionary.</returns>
         */
        public static IDictionary<TK, TV> CreateSortedDictionary<TK, TV>(int len)
        {
            return new SortedDictionary<TK, TV>();
        }

        /**
         * <summary>Create new ConcurrentDictionary.</summary>
         * <param name="len">Length.</param>
         * <returns>ConcurrentDictionary.</returns>
         */
        public static IDictionary<TK, TV> CreateConcurrentDictionary<TK, TV>(int len)
        {
            return new ConcurrentDictionary<TK, TV>(Environment.ProcessorCount, len);
        }


        /**
         * <summary>Read delegate.</summary>
         * <param name="ctx">Read context.</param>
         * <param name="type">Type.</param>
         */
        private delegate object PortableSystemReadDelegate(PortableReaderImpl ctx, Type type);

        /// <summary>
        /// System type reader.
        /// </summary>
        private interface IPortableSystemReader
        {
            /// <summary>
            /// Reads a value of specified type from reader.
            /// </summary>
            T Read<T>(PortableReaderImpl ctx);
        }

        /// <summary>
        /// System type generic reader.
        /// </summary>
        private interface IPortableSystemReader<out T>
        {
            /// <summary>
            /// Reads a value of specified type from reader.
            /// </summary>
            T Read(PortableReaderImpl ctx);
        }

        /// <summary>
        /// Default reader with boxing.
        /// </summary>
        private class PortableSystemReader : IPortableSystemReader
        {
            /** */
            private readonly PortableSystemReadDelegate _readDelegate;

            /// <summary>
            /// Initializes a new instance of the <see cref="PortableSystemReader"/> class.
            /// </summary>
            /// <param name="readDelegate">The read delegate.</param>
            public PortableSystemReader(PortableSystemReadDelegate readDelegate)
            {
                Debug.Assert(readDelegate != null);

                _readDelegate = readDelegate;
            }

            /** <inheritdoc /> */
            public T Read<T>(PortableReaderImpl ctx)
            {
                return (T)_readDelegate(ctx, typeof(T));
            }
        }

        /// <summary>
        /// Reader without boxing.
        /// </summary>
        private class PortableSystemReader<T> : IPortableSystemReader
        {
            /** */
            private readonly Func<IPortableStream, T> _readDelegate;

            /// <summary>
            /// Initializes a new instance of the <see cref="PortableSystemReader{T}"/> class.
            /// </summary>
            /// <param name="readDelegate">The read delegate.</param>
            public PortableSystemReader(Func<IPortableStream, T> readDelegate)
            {
                Debug.Assert(readDelegate != null);

                _readDelegate = readDelegate;
            }

            /** <inheritdoc /> */
            public TResult Read<TResult>(PortableReaderImpl ctx)
            {
                return TypeCaster<TResult>.Cast(_readDelegate(ctx.Stream));
            }
        }

        /// <summary>
        /// Reader without boxing.
        /// </summary>
        private class PortableSystemGenericArrayReader<T> : IPortableSystemReader
        {
            public TResult Read<TResult>(PortableReaderImpl ctx)
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
            private readonly Func<IPortableStream, T1> _readDelegate1;

            /** */
            private readonly Func<IPortableStream, T2> _readDelegate2;

            /// <summary>
            /// Initializes a new instance of the <see cref="PortableSystemDualReader{T1, T2}"/> class.
            /// </summary>
            /// <param name="readDelegate1">The read delegate1.</param>
            /// <param name="readDelegate2">The read delegate2.</param>
            public PortableSystemDualReader(Func<IPortableStream, T1> readDelegate1, Func<IPortableStream, T2> readDelegate2)
            {
                Debug.Assert(readDelegate1 != null);
                Debug.Assert(readDelegate2 != null);

                _readDelegate1 = readDelegate1;
                _readDelegate2 = readDelegate2;
            }

            /** <inheritdoc /> */
            T2 IPortableSystemReader<T2>.Read(PortableReaderImpl ctx)
            {
                return _readDelegate2(ctx.Stream);
            }

            /** <inheritdoc /> */
            public T Read<T>(PortableReaderImpl ctx)
            {
                // Can't use "as" because of variance. 
                // For example, IPortableSystemReader<byte[]> can be cast to IPortableSystemReader<sbyte[]>, which
                // will cause incorrect behavior.
                if (typeof (T) == typeof (T2))  
                    return ((IPortableSystemReader<T>) this).Read(ctx);

                return TypeCaster<T>.Cast(_readDelegate1(ctx.Stream));
            }
        }
    }
}
