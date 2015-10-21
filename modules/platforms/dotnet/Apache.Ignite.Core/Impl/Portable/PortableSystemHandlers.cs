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
    using System.Diagnostics.CodeAnalysis;
    using Apache.Ignite.Core.Impl.Common;
    using Apache.Ignite.Core.Impl.Portable.IO;

    /// <summary>
    /// Write delegate.
    /// </summary>
    /// <param name="writer">Write context.</param>
    /// <param name="obj">Object to write.</param>
    internal delegate void PortableSystemWriteDelegate(PortableWriterImpl writer, object obj);

    /**
     * <summary>Collection of predefined handlers for various system types.</summary>
     */
    internal static class PortableSystemHandlers
    {
        /** Write handlers. */
        private static volatile Dictionary<Type, PortableSystemWriteDelegate> _writeHandlers =
            new Dictionary<Type, PortableSystemWriteDelegate>();

        /** Mutex for write handlers update. */
        private static readonly Object WriteHandlersMux = new Object();

        /** Read handlers. */
        private static readonly IPortableSystemReader[] ReadHandlers = new IPortableSystemReader[255];
        
        /** Write handler: collection. */
        public static readonly PortableSystemWriteDelegate WriteHndCollection = WriteCollection;

        /** Write handler: dictionary. */
        public static readonly PortableSystemWriteDelegate WriteHndDictionary = WriteDictionary;

        /// <summary>
        /// Initializes the <see cref="PortableSystemHandlers"/> class.
        /// </summary>
        [SuppressMessage("Microsoft.Performance", "CA1810:InitializeReferenceTypeStaticFieldsInline", 
            Justification = "Readability.")]
        static PortableSystemHandlers()
        {
            // 1. Primitives.
            ReadHandlers[PortableUtils.TypeBool] = new PortableSystemReader<bool>(s => s.ReadBool());
            ReadHandlers[PortableUtils.TypeByte] = new PortableSystemReader<byte>(s => s.ReadByte());
            ReadHandlers[PortableUtils.TypeShort] = new PortableSystemReader<short>(s => s.ReadShort());
            ReadHandlers[PortableUtils.TypeChar] = new PortableSystemReader<char>(s => s.ReadChar());
            ReadHandlers[PortableUtils.TypeInt] = new PortableSystemReader<int>(s => s.ReadInt());
            ReadHandlers[PortableUtils.TypeLong] = new PortableSystemReader<long>(s => s.ReadLong());
            ReadHandlers[PortableUtils.TypeFloat] = new PortableSystemReader<float>(s => s.ReadFloat());
            ReadHandlers[PortableUtils.TypeDouble] = new PortableSystemReader<double>(s => s.ReadDouble());
            ReadHandlers[PortableUtils.TypeDecimal] = new PortableSystemReader<decimal?>(PortableUtils.ReadDecimal);

            // 2. Date.
            ReadHandlers[PortableUtils.TypeDate] = new PortableSystemReader<DateTime?>(s => PortableUtils.ReadDate(s, false));

            // 3. String.
            ReadHandlers[PortableUtils.TypeString] = new PortableSystemReader<string>(PortableUtils.ReadString);

            // 4. Guid.
            ReadHandlers[PortableUtils.TypeGuid] = new PortableSystemReader<Guid?>(PortableUtils.ReadGuid);

            // 5. Primitive arrays.
            ReadHandlers[PortableUtils.TypeArrayBool] = new PortableSystemReader<bool[]>(PortableUtils.ReadBooleanArray);

            ReadHandlers[PortableUtils.TypeArrayByte] =
                new PortableSystemDualReader<byte[], sbyte[]>(PortableUtils.ReadByteArray, PortableUtils.ReadSbyteArray);
            
            ReadHandlers[PortableUtils.TypeArrayShort] =
                new PortableSystemDualReader<short[], ushort[]>(PortableUtils.ReadShortArray,
                    PortableUtils.ReadUshortArray);

            ReadHandlers[PortableUtils.TypeArrayChar] = 
                new PortableSystemReader<char[]>(PortableUtils.ReadCharArray);

            ReadHandlers[PortableUtils.TypeArrayInt] =
                new PortableSystemDualReader<int[], uint[]>(PortableUtils.ReadIntArray, PortableUtils.ReadUintArray);
            
            ReadHandlers[PortableUtils.TypeArrayLong] =
                new PortableSystemDualReader<long[], ulong[]>(PortableUtils.ReadLongArray, 
                    PortableUtils.ReadUlongArray);

            ReadHandlers[PortableUtils.TypeArrayFloat] =
                new PortableSystemReader<float[]>(PortableUtils.ReadFloatArray);

            ReadHandlers[PortableUtils.TypeArrayDouble] =
                new PortableSystemReader<double[]>(PortableUtils.ReadDoubleArray);

            ReadHandlers[PortableUtils.TypeArrayDecimal] =
                new PortableSystemReader<decimal?[]>(PortableUtils.ReadDecimalArray);

            // 6. Date array.
            ReadHandlers[PortableUtils.TypeArrayDate] =
                new PortableSystemReader<DateTime?[]>(s => PortableUtils.ReadDateArray(s, false));

            // 7. String array.
            ReadHandlers[PortableUtils.TypeArrayString] = new PortableSystemTypedArrayReader<string>();

            // 8. Guid array.
            ReadHandlers[PortableUtils.TypeArrayGuid] = new PortableSystemTypedArrayReader<Guid?>();

            // 9. Array.
            ReadHandlers[PortableUtils.TypeArray] = new PortableSystemReader(ReadArray);

            // 11. Arbitrary collection.
            ReadHandlers[PortableUtils.TypeCollection] = new PortableSystemReader(ReadCollection);

            // 13. Arbitrary dictionary.
            ReadHandlers[PortableUtils.TypeDictionary] = new PortableSystemReader(ReadDictionary);

            // 15. Map entry.
            ReadHandlers[PortableUtils.TypeMapEntry] = new PortableSystemReader(ReadMapEntry);
            
            // 16. Enum.
            ReadHandlers[PortableUtils.TypeEnum] = new PortableSystemReader<int>(PortableUtils.ReadEnum<int>);
            ReadHandlers[PortableUtils.TypeArrayEnum] = new PortableSystemReader(ReadEnumArray);
        }

        /// <summary>
        /// Try getting write handler for type.
        /// </summary>
        /// <param name="type"></param>
        /// <returns></returns>
        public static PortableSystemWriteDelegate GetWriteHandler(Type type)
        {
            PortableSystemWriteDelegate res;

            var writeHandlers0 = _writeHandlers;

            // Have we ever met this type?
            if (writeHandlers0 != null && writeHandlers0.TryGetValue(type, out res))
                return res;

            // Determine write handler for type and add it.
            res = FindWriteHandler(type);

            if (res != null)
                AddWriteHandler(type, res);

            return res;
        }

        /// <summary>
        /// Find write handler for type.
        /// </summary>
        /// <param name="type">Type.</param>
        /// <returns>Write handler or NULL.</returns>
        private static PortableSystemWriteDelegate FindWriteHandler(Type type)
        {
            // 1. Well-known types.
            if (type == typeof (string))
                return WriteString;
            if (type == typeof(decimal))
                return WriteDecimal;
            if (type == typeof(DateTime))
                return WriteDate;
            if (type == typeof(Guid))
                return WriteGuid;
            if (type == typeof (PortableUserObject))
                return WritePortable;
            if (type == typeof (ArrayList))
                return WriteArrayList;
            if (type == typeof(Hashtable))
                return WriteHashtable;
            if (type == typeof(DictionaryEntry))
                return WriteMapEntry;
            if (type.IsArray)
            {
                // We know how to write any array type.
                Type elemType = type.GetElementType();
                
                // Primitives.
                if (elemType == typeof (bool))
                    return WriteBoolArray;
                if (elemType == typeof(byte))
                    return WriteByteArray;
                if (elemType == typeof(short))
                    return WriteShortArray;
                if (elemType == typeof(char))
                    return WriteCharArray;
                if (elemType == typeof(int))
                    return WriteIntArray;
                if (elemType == typeof(long))
                    return WriteLongArray;
                if (elemType == typeof(float))
                    return WriteFloatArray;
                if (elemType == typeof(double))
                    return WriteDoubleArray;
                // Non-CLS primitives.
                if (elemType == typeof(sbyte))
                    return WriteSbyteArray;
                if (elemType == typeof(ushort))
                    return WriteUshortArray;
                if (elemType == typeof(uint))
                    return WriteUintArray;
                if (elemType == typeof(ulong))
                    return WriteUlongArray;
                // Special types.
                else if (elemType == typeof (decimal?))
                    return WriteDecimalArray;
                if (elemType == typeof(string))
                    return WriteStringArray;
//                else if (elemType == typeof(DateTime))
//                    return WriteDateArray;
                if (elemType == typeof(DateTime?))
                    return WriteNullableDateArray;
//                else if (elemType == typeof(Guid))
//                    return WriteGuidArray;
                if (elemType == typeof(Guid?))
                    return WriteNullableGuidArray;
                // Enums.
                if (elemType.IsEnum)
                    return WriteEnumArray;
                
                // Object array.
                if (elemType == typeof (object))
                    return WriteArray;
            }
            if (type.IsEnum)
                // We know how to write enums.
                return WriteEnum;

            return null;
        }

        /// <summary>
        /// Add write handler for type.
        /// </summary>
        /// <param name="type"></param>
        /// <param name="handler"></param>
        private static void AddWriteHandler(Type type, PortableSystemWriteDelegate handler)
        {
            lock (WriteHandlersMux)
            {
                if (_writeHandlers == null)
                {
                    Dictionary<Type, PortableSystemWriteDelegate> writeHandlers0 = 
                        new Dictionary<Type, PortableSystemWriteDelegate>();

                    writeHandlers0[type] = handler;

                    _writeHandlers = writeHandlers0;
                }
                else if (!_writeHandlers.ContainsKey(type))
                {
                    Dictionary<Type, PortableSystemWriteDelegate> writeHandlers0 =
                        new Dictionary<Type, PortableSystemWriteDelegate>(_writeHandlers);

                    writeHandlers0[type] = handler;

                    _writeHandlers = writeHandlers0;
                }
            }
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
        
        /// <summary>
        /// Write decimal.
        /// </summary>
        /// <param name="ctx">Context.</param>
        /// <param name="obj">Value.</param>
        private static void WriteDecimal(PortableWriterImpl ctx, object obj)
        {
            ctx.Stream.WriteByte(PortableUtils.TypeDecimal);

            PortableUtils.WriteDecimal((decimal)obj, ctx.Stream);
        }
        
        /// <summary>
        /// Write date.
        /// </summary>
        /// <param name="ctx">Context.</param>
        /// <param name="obj">Value.</param>
        private static void WriteDate(PortableWriterImpl ctx, object obj)
        {
            ctx.Stream.WriteByte(PortableUtils.TypeDate);

            PortableUtils.WriteDate((DateTime)obj, ctx.Stream);
        }
        
        /// <summary>
        /// Write string.
        /// </summary>
        /// <param name="ctx">Context.</param>
        /// <param name="obj">Object.</param>
        private static void WriteString(PortableWriterImpl ctx, object obj)
        {
            ctx.Stream.WriteByte(PortableUtils.TypeString);

            PortableUtils.WriteString((string)obj, ctx.Stream);
        }

        /// <summary>
        /// Write Guid.
        /// </summary>
        /// <param name="ctx">Context.</param>
        /// <param name="obj">Value.</param>
        private static void WriteGuid(PortableWriterImpl ctx, object obj)
        {
            ctx.Stream.WriteByte(PortableUtils.TypeGuid);

            PortableUtils.WriteGuid((Guid)obj, ctx.Stream);
        }

        /// <summary>
        /// Write boolaen array.
        /// </summary>
        /// <param name="ctx">Context.</param>
        /// <param name="obj">Value.</param>
        private static void WriteBoolArray(PortableWriterImpl ctx, object obj)
        {
            ctx.Stream.WriteByte(PortableUtils.TypeArrayBool);

            PortableUtils.WriteBooleanArray((bool[])obj, ctx.Stream);
        }
        
        /// <summary>
        /// Write byte array.
        /// </summary>
        /// <param name="ctx">Context.</param>
        /// <param name="obj">Value.</param>
        private static void WriteByteArray(PortableWriterImpl ctx, object obj)
        {
            ctx.Stream.WriteByte(PortableUtils.TypeArrayByte);

            PortableUtils.WriteByteArray((byte[])obj, ctx.Stream);
        }

        /// <summary>
        /// Write sbyte array.
        /// </summary>
        /// <param name="ctx">Context.</param>
        /// <param name="obj">Value.</param>
        private static void WriteSbyteArray(PortableWriterImpl ctx, object obj)
        {
            ctx.Stream.WriteByte(PortableUtils.TypeArrayByte);

            PortableUtils.WriteByteArray((byte[])(Array)obj, ctx.Stream);
        }

        /// <summary>
        /// Write short array.
        /// </summary>
        /// <param name="ctx">Context.</param>
        /// <param name="obj">Value.</param>
        private static void WriteShortArray(PortableWriterImpl ctx, object obj)
        {
            ctx.Stream.WriteByte(PortableUtils.TypeArrayShort);

            PortableUtils.WriteShortArray((short[])obj, ctx.Stream);
        }
        
        /// <summary>
        /// Write ushort array.
        /// </summary>
        /// <param name="ctx">Context.</param>
        /// <param name="obj">Value.</param>
        private static void WriteUshortArray(PortableWriterImpl ctx, object obj)
        {
            ctx.Stream.WriteByte(PortableUtils.TypeArrayShort);

            PortableUtils.WriteShortArray((short[])(Array)obj, ctx.Stream);
        }

        /// <summary>
        /// Write char array.
        /// </summary>
        /// <param name="ctx">Context.</param>
        /// <param name="obj">Value.</param>
        private static void WriteCharArray(PortableWriterImpl ctx, object obj)
        {
            ctx.Stream.WriteByte(PortableUtils.TypeArrayChar);

            PortableUtils.WriteCharArray((char[])obj, ctx.Stream);
        }

        /// <summary>
        /// Write int array.
        /// </summary>
        /// <param name="ctx">Context.</param>
        /// <param name="obj">Value.</param>
        private static void WriteIntArray(PortableWriterImpl ctx, object obj)
        {
            ctx.Stream.WriteByte(PortableUtils.TypeArrayInt);

            PortableUtils.WriteIntArray((int[])obj, ctx.Stream);
        }

        /// <summary>
        /// Write uint array.
        /// </summary>
        /// <param name="ctx">Context.</param>
        /// <param name="obj">Value.</param>
        private static void WriteUintArray(PortableWriterImpl ctx, object obj)
        {
            ctx.Stream.WriteByte(PortableUtils.TypeArrayInt);

            PortableUtils.WriteIntArray((int[])(Array)obj, ctx.Stream);
        }

        /// <summary>
        /// Write long array.
        /// </summary>
        /// <param name="ctx">Context.</param>
        /// <param name="obj">Value.</param>
        private static void WriteLongArray(PortableWriterImpl ctx, object obj)
        {
            ctx.Stream.WriteByte(PortableUtils.TypeArrayLong);

            PortableUtils.WriteLongArray((long[])obj, ctx.Stream);
        }

        /// <summary>
        /// Write ulong array.
        /// </summary>
        /// <param name="ctx">Context.</param>
        /// <param name="obj">Value.</param>
        private static void WriteUlongArray(PortableWriterImpl ctx, object obj)
        {
            ctx.Stream.WriteByte(PortableUtils.TypeArrayLong);

            PortableUtils.WriteLongArray((long[])(Array)obj, ctx.Stream);
        }

        /// <summary>
        /// Write float array.
        /// </summary>
        /// <param name="ctx">Context.</param>
        /// <param name="obj">Value.</param>
        private static void WriteFloatArray(PortableWriterImpl ctx, object obj)
        {
            ctx.Stream.WriteByte(PortableUtils.TypeArrayFloat);

            PortableUtils.WriteFloatArray((float[])obj, ctx.Stream);
        }

        /// <summary>
        /// Write double array.
        /// </summary>
        /// <param name="ctx">Context.</param>
        /// <param name="obj">Value.</param>
        private static void WriteDoubleArray(PortableWriterImpl ctx, object obj)
        {
            ctx.Stream.WriteByte(PortableUtils.TypeArrayDouble);

            PortableUtils.WriteDoubleArray((double[])obj, ctx.Stream);
        }

        /// <summary>
        /// Write decimal array.
        /// </summary>
        /// <param name="ctx">Context.</param>
        /// <param name="obj">Value.</param>
        private static void WriteDecimalArray(PortableWriterImpl ctx, object obj)
        {
            ctx.Stream.WriteByte(PortableUtils.TypeArrayDecimal);

            PortableUtils.WriteDecimalArray((decimal?[])obj, ctx.Stream);
        }

        /// <summary>
        /// Write date array.
        /// </summary>
        /// <param name="ctx">Context.</param>
        /// <param name="obj">Value.</param>
        private static void WriteDateArray(PortableWriterImpl ctx, object obj)
        {
            ctx.Stream.WriteByte(PortableUtils.TypeArrayDate);

            PortableUtils.WriteDateArray((DateTime[])obj, ctx.Stream);
        }

        /// <summary>
        /// Write nullable date array.
        /// </summary>
        /// <param name="ctx">Context.</param>
        /// <param name="obj">Value.</param>
        private static void WriteNullableDateArray(PortableWriterImpl ctx, object obj)
        {
            ctx.Stream.WriteByte(PortableUtils.TypeArrayDate);

            PortableUtils.WriteDateArray((DateTime?[])obj, ctx.Stream);
        }

        /// <summary>
        /// Write string array.
        /// </summary>
        /// <param name="ctx">Context.</param>
        /// <param name="obj">Value.</param>
        private static void WriteStringArray(PortableWriterImpl ctx, object obj)
        {
            ctx.Stream.WriteByte(PortableUtils.TypeArrayString);

            PortableUtils.WriteStringArray((string[])obj, ctx.Stream);
        }

        /// <summary>
        /// Write GUID array.
        /// </summary>
        /// <param name="ctx">Context.</param>
        /// <param name="obj">Value.</param>
        private static void WriteGuidArray(PortableWriterImpl ctx, object obj)
        {
            ctx.Stream.WriteByte(PortableUtils.TypeArrayGuid);

            PortableUtils.WriteGuidArray((Guid[])obj, ctx.Stream);
        }

        /// <summary>
        /// Write nullable GUID array.
        /// </summary>
        /// <param name="ctx">Context.</param>
        /// <param name="obj">Value.</param>
        private static void WriteNullableGuidArray(PortableWriterImpl ctx, object obj)
        {
            ctx.Stream.WriteByte(PortableUtils.TypeArrayGuid);

            PortableUtils.WriteGuidArray((Guid?[])obj, ctx.Stream);
        }
        
        /**
         * <summary>Write enum array.</summary>
         */
        private static void WriteEnumArray(PortableWriterImpl ctx, object obj)
        {
            ctx.Stream.WriteByte(PortableUtils.TypeArrayEnum);

            PortableUtils.WriteArray((Array)obj, ctx);
        }

        /**
         * <summary>Write array.</summary>
         */
        private static void WriteArray(PortableWriterImpl ctx, object obj)
        {
            ctx.Stream.WriteByte(PortableUtils.TypeArray);

            PortableUtils.WriteArray((Array)obj, ctx);
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
         * <summary>Write dictionary.</summary>
         */
        private static void WriteDictionary(PortableWriterImpl ctx, object obj)
        {
            ctx.Stream.WriteByte(PortableUtils.TypeDictionary);

            PortableUtils.WriteDictionary((IDictionary)obj, ctx);
        }

        /**
         * <summary>Write ArrayList.</summary>
         */
        private static void WriteArrayList(PortableWriterImpl ctx, object obj)
        {
            ctx.Stream.WriteByte(PortableUtils.TypeCollection);

            PortableUtils.WriteCollection((ICollection)obj, ctx, PortableUtils.CollectionArrayList);
        }

        /**
         * <summary>Write Hashtable.</summary>
         */
        private static void WriteHashtable(PortableWriterImpl ctx, object obj)
        {
            ctx.Stream.WriteByte(PortableUtils.TypeDictionary);

            PortableUtils.WriteDictionary((IDictionary)obj, ctx, PortableUtils.MapHashMap);
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
            return PortableUtils.ReadTypedArray(ctx, true, type.GetElementType());
        }

        /**
         * <summary>Read array.</summary>
         */
        private static object ReadArray(PortableReaderImpl ctx, Type type)
        {
            return PortableUtils.ReadArray<object>(ctx, true);
        }

        /**
         * <summary>Read collection.</summary>
         */
        private static object ReadCollection(PortableReaderImpl ctx, Type type)
        {
            return PortableUtils.ReadCollection(ctx, null, null);
        }

        /**
         * <summary>Read dictionary.</summary>
         */
        private static object ReadDictionary(PortableReaderImpl ctx, Type type)
        {
            return PortableUtils.ReadDictionary(ctx, null);
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
        private class PortableSystemTypedArrayReader<T> : IPortableSystemReader
        {
            public TResult Read<TResult>(PortableReaderImpl ctx)
            {
                return TypeCaster<TResult>.Cast(PortableUtils.ReadArray<T>(ctx, false));
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
