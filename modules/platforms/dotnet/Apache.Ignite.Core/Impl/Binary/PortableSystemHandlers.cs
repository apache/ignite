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
    using System.Diagnostics;
    using System.Diagnostics.CodeAnalysis;
    using Apache.Ignite.Core.Impl.Common;
    using Apache.Ignite.Core.Impl.Portable.IO;

    /// <summary>
    /// Write delegate.
    /// </summary>
    /// <param name="writer">Write context.</param>
    /// <param name="obj">Object to write.</param>
    internal delegate void PortableSystemWriteDelegate(BinaryWriterImpl writer, object obj);

    /**
     * <summary>Collection of predefined handlers for various system types.</summary>
     */
    internal static class PortableSystemHandlers
    {
        /** Write handlers. */
        private static volatile Dictionary<Type, PortableSystemWriteDelegate> _writeHandlers =
            new Dictionary<Type, PortableSystemWriteDelegate>();

        /** Mutex for write handlers update. */
        private static readonly object WriteHandlersMux = new object();

        /** Read handlers. */
        private static readonly IPortableSystemReader[] ReadHandlers = new IPortableSystemReader[255];

        /** Type ids. */
        private static readonly Dictionary<Type, byte> TypeIds = new Dictionary<Type, byte>
        {
            {typeof (bool), BinaryUtils.TypeBool},
            {typeof (byte), BinaryUtils.TypeByte},
            {typeof (sbyte), BinaryUtils.TypeByte},
            {typeof (short), BinaryUtils.TypeShort},
            {typeof (ushort), BinaryUtils.TypeShort},
            {typeof (char), BinaryUtils.TypeChar},
            {typeof (int), BinaryUtils.TypeInt},
            {typeof (uint), BinaryUtils.TypeInt},
            {typeof (long), BinaryUtils.TypeLong},
            {typeof (ulong), BinaryUtils.TypeLong},
            {typeof (float), BinaryUtils.TypeFloat},
            {typeof (double), BinaryUtils.TypeDouble},
            {typeof (string), BinaryUtils.TypeString},
            {typeof (decimal), BinaryUtils.TypeDecimal},
            {typeof (Guid), BinaryUtils.TypeGuid},
            {typeof (Guid?), BinaryUtils.TypeGuid},
            {typeof (ArrayList), BinaryUtils.TypeCollection},
            {typeof (Hashtable), BinaryUtils.TypeDictionary},
            {typeof (DictionaryEntry), BinaryUtils.TypeMapEntry},
            {typeof (bool[]), BinaryUtils.TypeArrayBool},
            {typeof (byte[]), BinaryUtils.TypeArrayByte},
            {typeof (sbyte[]), BinaryUtils.TypeArrayByte},
            {typeof (short[]), BinaryUtils.TypeArrayShort},
            {typeof (ushort[]), BinaryUtils.TypeArrayShort},
            {typeof (char[]), BinaryUtils.TypeArrayChar},
            {typeof (int[]), BinaryUtils.TypeArrayInt},
            {typeof (uint[]), BinaryUtils.TypeArrayInt},
            {typeof (long[]), BinaryUtils.TypeArrayLong},
            {typeof (ulong[]), BinaryUtils.TypeArrayLong},
            {typeof (float[]), BinaryUtils.TypeArrayFloat},
            {typeof (double[]), BinaryUtils.TypeArrayDouble},
            {typeof (string[]), BinaryUtils.TypeArrayString},
            {typeof (decimal?[]), BinaryUtils.TypeArrayDecimal},
            {typeof (Guid?[]), BinaryUtils.TypeArrayGuid},
            {typeof (object[]), BinaryUtils.TypeArray}
        };
        
        /// <summary>
        /// Initializes the <see cref="PortableSystemHandlers"/> class.
        /// </summary>
        [SuppressMessage("Microsoft.Performance", "CA1810:InitializeReferenceTypeStaticFieldsInline", 
            Justification = "Readability.")]
        static PortableSystemHandlers()
        {
            // 1. Primitives.
            ReadHandlers[BinaryUtils.TypeBool] = new PortableSystemReader<bool>(s => s.ReadBool());
            ReadHandlers[BinaryUtils.TypeByte] = new PortableSystemReader<byte>(s => s.ReadByte());
            ReadHandlers[BinaryUtils.TypeShort] = new PortableSystemReader<short>(s => s.ReadShort());
            ReadHandlers[BinaryUtils.TypeChar] = new PortableSystemReader<char>(s => s.ReadChar());
            ReadHandlers[BinaryUtils.TypeInt] = new PortableSystemReader<int>(s => s.ReadInt());
            ReadHandlers[BinaryUtils.TypeLong] = new PortableSystemReader<long>(s => s.ReadLong());
            ReadHandlers[BinaryUtils.TypeFloat] = new PortableSystemReader<float>(s => s.ReadFloat());
            ReadHandlers[BinaryUtils.TypeDouble] = new PortableSystemReader<double>(s => s.ReadDouble());
            ReadHandlers[BinaryUtils.TypeDecimal] = new PortableSystemReader<decimal?>(BinaryUtils.ReadDecimal);

            // 2. Date.
            ReadHandlers[BinaryUtils.TypeTimestamp] = new PortableSystemReader<DateTime?>(BinaryUtils.ReadTimestamp);

            // 3. String.
            ReadHandlers[BinaryUtils.TypeString] = new PortableSystemReader<string>(BinaryUtils.ReadString);

            // 4. Guid.
            ReadHandlers[BinaryUtils.TypeGuid] = new PortableSystemReader<Guid?>(BinaryUtils.ReadGuid);

            // 5. Primitive arrays.
            ReadHandlers[BinaryUtils.TypeArrayBool] = new PortableSystemReader<bool[]>(BinaryUtils.ReadBooleanArray);

            ReadHandlers[BinaryUtils.TypeArrayByte] =
                new PortableSystemDualReader<byte[], sbyte[]>(BinaryUtils.ReadByteArray, BinaryUtils.ReadSbyteArray);
            
            ReadHandlers[BinaryUtils.TypeArrayShort] =
                new PortableSystemDualReader<short[], ushort[]>(BinaryUtils.ReadShortArray,
                    BinaryUtils.ReadUshortArray);

            ReadHandlers[BinaryUtils.TypeArrayChar] = 
                new PortableSystemReader<char[]>(BinaryUtils.ReadCharArray);

            ReadHandlers[BinaryUtils.TypeArrayInt] =
                new PortableSystemDualReader<int[], uint[]>(BinaryUtils.ReadIntArray, BinaryUtils.ReadUintArray);
            
            ReadHandlers[BinaryUtils.TypeArrayLong] =
                new PortableSystemDualReader<long[], ulong[]>(BinaryUtils.ReadLongArray, 
                    BinaryUtils.ReadUlongArray);

            ReadHandlers[BinaryUtils.TypeArrayFloat] =
                new PortableSystemReader<float[]>(BinaryUtils.ReadFloatArray);

            ReadHandlers[BinaryUtils.TypeArrayDouble] =
                new PortableSystemReader<double[]>(BinaryUtils.ReadDoubleArray);

            ReadHandlers[BinaryUtils.TypeArrayDecimal] =
                new PortableSystemReader<decimal?[]>(BinaryUtils.ReadDecimalArray);

            // 6. Date array.
            ReadHandlers[BinaryUtils.TypeArrayTimestamp] =
                new PortableSystemReader<DateTime?[]>(BinaryUtils.ReadTimestampArray);

            // 7. String array.
            ReadHandlers[BinaryUtils.TypeArrayString] = new PortableSystemTypedArrayReader<string>();

            // 8. Guid array.
            ReadHandlers[BinaryUtils.TypeArrayGuid] = new PortableSystemTypedArrayReader<Guid?>();

            // 9. Array.
            ReadHandlers[BinaryUtils.TypeArray] = new PortableSystemReader(ReadArray);

            // 11. Arbitrary collection.
            ReadHandlers[BinaryUtils.TypeCollection] = new PortableSystemReader(ReadCollection);

            // 13. Arbitrary dictionary.
            ReadHandlers[BinaryUtils.TypeDictionary] = new PortableSystemReader(ReadDictionary);

            // 15. Map entry.
            ReadHandlers[BinaryUtils.TypeMapEntry] = new PortableSystemReader(ReadMapEntry);
            
            // 16. Enum.
            ReadHandlers[BinaryUtils.TypeEnum] = new PortableSystemReader<int>(BinaryUtils.ReadEnum<int>);
            ReadHandlers[BinaryUtils.TypeArrayEnum] = new PortableSystemReader(ReadEnumArray);
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
            if (type == typeof(string))
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
                if (elemType == typeof (decimal?))
                    return WriteDecimalArray;
                if (elemType == typeof(string))
                    return WriteStringArray;
                if (elemType == typeof(Guid?))
                    return WriteGuidArray;
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

            if (type.IsSerializable)
                return WriteSerializable;

            return null;
        }

        /// <summary>
        /// Find write handler for type.
        /// </summary>
        /// <param name="type">Type.</param>
        /// <returns>Write handler or NULL.</returns>
        public static byte GetTypeId(Type type)
        {
            byte res;

            if (TypeIds.TryGetValue(type, out res))
                return res;

            if (type.IsEnum)
                return BinaryUtils.TypeEnum;

            if (type.IsArray && type.GetElementType().IsEnum)
                return BinaryUtils.TypeArrayEnum;

            return BinaryUtils.TypeObject;
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
        public static T ReadSystemType<T>(byte typeId, BinaryReaderImpl ctx)
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
        private static void WriteDecimal(BinaryWriterImpl ctx, object obj)
        {
            ctx.Stream.WriteByte(BinaryUtils.TypeDecimal);

            BinaryUtils.WriteDecimal((decimal)obj, ctx.Stream);
        }
        
        /// <summary>
        /// Write date.
        /// </summary>
        /// <param name="ctx">Context.</param>
        /// <param name="obj">Value.</param>
        private static void WriteDate(BinaryWriterImpl ctx, object obj)
        {
            ctx.Write(new DateTimeHolder((DateTime) obj));
        }
        
        /// <summary>
        /// Write string.
        /// </summary>
        /// <param name="ctx">Context.</param>
        /// <param name="obj">Object.</param>
        private static void WriteString(BinaryWriterImpl ctx, object obj)
        {
            ctx.Stream.WriteByte(BinaryUtils.TypeString);

            BinaryUtils.WriteString((string)obj, ctx.Stream);
        }

        /// <summary>
        /// Write Guid.
        /// </summary>
        /// <param name="ctx">Context.</param>
        /// <param name="obj">Value.</param>
        private static void WriteGuid(BinaryWriterImpl ctx, object obj)
        {
            ctx.Stream.WriteByte(BinaryUtils.TypeGuid);

            BinaryUtils.WriteGuid((Guid)obj, ctx.Stream);
        }

        /// <summary>
        /// Write boolaen array.
        /// </summary>
        /// <param name="ctx">Context.</param>
        /// <param name="obj">Value.</param>
        private static void WriteBoolArray(BinaryWriterImpl ctx, object obj)
        {
            ctx.Stream.WriteByte(BinaryUtils.TypeArrayBool);

            BinaryUtils.WriteBooleanArray((bool[])obj, ctx.Stream);
        }
        
        /// <summary>
        /// Write byte array.
        /// </summary>
        /// <param name="ctx">Context.</param>
        /// <param name="obj">Value.</param>
        private static void WriteByteArray(BinaryWriterImpl ctx, object obj)
        {
            ctx.Stream.WriteByte(BinaryUtils.TypeArrayByte);

            BinaryUtils.WriteByteArray((byte[])obj, ctx.Stream);
        }

        /// <summary>
        /// Write sbyte array.
        /// </summary>
        /// <param name="ctx">Context.</param>
        /// <param name="obj">Value.</param>
        private static void WriteSbyteArray(BinaryWriterImpl ctx, object obj)
        {
            ctx.Stream.WriteByte(BinaryUtils.TypeArrayByte);

            BinaryUtils.WriteByteArray((byte[])(Array)obj, ctx.Stream);
        }

        /// <summary>
        /// Write short array.
        /// </summary>
        /// <param name="ctx">Context.</param>
        /// <param name="obj">Value.</param>
        private static void WriteShortArray(BinaryWriterImpl ctx, object obj)
        {
            ctx.Stream.WriteByte(BinaryUtils.TypeArrayShort);

            BinaryUtils.WriteShortArray((short[])obj, ctx.Stream);
        }
        
        /// <summary>
        /// Write ushort array.
        /// </summary>
        /// <param name="ctx">Context.</param>
        /// <param name="obj">Value.</param>
        private static void WriteUshortArray(BinaryWriterImpl ctx, object obj)
        {
            ctx.Stream.WriteByte(BinaryUtils.TypeArrayShort);

            BinaryUtils.WriteShortArray((short[])(Array)obj, ctx.Stream);
        }

        /// <summary>
        /// Write char array.
        /// </summary>
        /// <param name="ctx">Context.</param>
        /// <param name="obj">Value.</param>
        private static void WriteCharArray(BinaryWriterImpl ctx, object obj)
        {
            ctx.Stream.WriteByte(BinaryUtils.TypeArrayChar);

            BinaryUtils.WriteCharArray((char[])obj, ctx.Stream);
        }

        /// <summary>
        /// Write int array.
        /// </summary>
        /// <param name="ctx">Context.</param>
        /// <param name="obj">Value.</param>
        private static void WriteIntArray(BinaryWriterImpl ctx, object obj)
        {
            ctx.Stream.WriteByte(BinaryUtils.TypeArrayInt);

            BinaryUtils.WriteIntArray((int[])obj, ctx.Stream);
        }

        /// <summary>
        /// Write uint array.
        /// </summary>
        /// <param name="ctx">Context.</param>
        /// <param name="obj">Value.</param>
        private static void WriteUintArray(BinaryWriterImpl ctx, object obj)
        {
            ctx.Stream.WriteByte(BinaryUtils.TypeArrayInt);

            BinaryUtils.WriteIntArray((int[])(Array)obj, ctx.Stream);
        }

        /// <summary>
        /// Write long array.
        /// </summary>
        /// <param name="ctx">Context.</param>
        /// <param name="obj">Value.</param>
        private static void WriteLongArray(BinaryWriterImpl ctx, object obj)
        {
            ctx.Stream.WriteByte(BinaryUtils.TypeArrayLong);

            BinaryUtils.WriteLongArray((long[])obj, ctx.Stream);
        }

        /// <summary>
        /// Write ulong array.
        /// </summary>
        /// <param name="ctx">Context.</param>
        /// <param name="obj">Value.</param>
        private static void WriteUlongArray(BinaryWriterImpl ctx, object obj)
        {
            ctx.Stream.WriteByte(BinaryUtils.TypeArrayLong);

            BinaryUtils.WriteLongArray((long[])(Array)obj, ctx.Stream);
        }

        /// <summary>
        /// Write float array.
        /// </summary>
        /// <param name="ctx">Context.</param>
        /// <param name="obj">Value.</param>
        private static void WriteFloatArray(BinaryWriterImpl ctx, object obj)
        {
            ctx.Stream.WriteByte(BinaryUtils.TypeArrayFloat);

            BinaryUtils.WriteFloatArray((float[])obj, ctx.Stream);
        }

        /// <summary>
        /// Write double array.
        /// </summary>
        /// <param name="ctx">Context.</param>
        /// <param name="obj">Value.</param>
        private static void WriteDoubleArray(BinaryWriterImpl ctx, object obj)
        {
            ctx.Stream.WriteByte(BinaryUtils.TypeArrayDouble);

            BinaryUtils.WriteDoubleArray((double[])obj, ctx.Stream);
        }

        /// <summary>
        /// Write decimal array.
        /// </summary>
        /// <param name="ctx">Context.</param>
        /// <param name="obj">Value.</param>
        private static void WriteDecimalArray(BinaryWriterImpl ctx, object obj)
        {
            ctx.Stream.WriteByte(BinaryUtils.TypeArrayDecimal);

            BinaryUtils.WriteDecimalArray((decimal?[])obj, ctx.Stream);
        }
        
        /// <summary>
        /// Write string array.
        /// </summary>
        /// <param name="ctx">Context.</param>
        /// <param name="obj">Value.</param>
        private static void WriteStringArray(BinaryWriterImpl ctx, object obj)
        {
            ctx.Stream.WriteByte(BinaryUtils.TypeArrayString);

            BinaryUtils.WriteStringArray((string[])obj, ctx.Stream);
        }
        
        /// <summary>
        /// Write nullable GUID array.
        /// </summary>
        /// <param name="ctx">Context.</param>
        /// <param name="obj">Value.</param>
        private static void WriteGuidArray(BinaryWriterImpl ctx, object obj)
        {
            ctx.Stream.WriteByte(BinaryUtils.TypeArrayGuid);

            BinaryUtils.WriteGuidArray((Guid?[])obj, ctx.Stream);
        }
        
        /**
         * <summary>Write enum array.</summary>
         */
        private static void WriteEnumArray(BinaryWriterImpl ctx, object obj)
        {
            ctx.Stream.WriteByte(BinaryUtils.TypeArrayEnum);

            BinaryUtils.WriteArray((Array)obj, ctx);
        }

        /**
         * <summary>Write array.</summary>
         */
        private static void WriteArray(BinaryWriterImpl ctx, object obj)
        {
            ctx.Stream.WriteByte(BinaryUtils.TypeArray);

            BinaryUtils.WriteArray((Array)obj, ctx);
        }

        /**
         * <summary>Write ArrayList.</summary>
         */
        private static void WriteArrayList(BinaryWriterImpl ctx, object obj)
        {
            ctx.Stream.WriteByte(BinaryUtils.TypeCollection);

            BinaryUtils.WriteCollection((ICollection)obj, ctx, BinaryUtils.CollectionArrayList);
        }

        /**
         * <summary>Write Hashtable.</summary>
         */
        private static void WriteHashtable(BinaryWriterImpl ctx, object obj)
        {
            ctx.Stream.WriteByte(BinaryUtils.TypeDictionary);

            BinaryUtils.WriteDictionary((IDictionary)obj, ctx, BinaryUtils.MapHashMap);
        }

        /**
         * <summary>Write map entry.</summary>
         */
        private static void WriteMapEntry(BinaryWriterImpl ctx, object obj)
        {
            ctx.Stream.WriteByte(BinaryUtils.TypeMapEntry);

            BinaryUtils.WriteMapEntry(ctx, (DictionaryEntry)obj);
        }

        /**
         * <summary>Write portable object.</summary>
         */
        private static void WritePortable(BinaryWriterImpl ctx, object obj)
        {
            ctx.Stream.WriteByte(BinaryUtils.TypePortable);

            BinaryUtils.WritePortable(ctx.Stream, (PortableUserObject)obj);
        }
        
        /// <summary>
        /// Write enum.
        /// </summary>
        private static void WriteEnum(BinaryWriterImpl ctx, object obj)
        {
            ctx.Stream.WriteByte(BinaryUtils.TypeEnum);

            BinaryUtils.WriteEnum(ctx.Stream, (Enum)obj);
        }

        /// <summary>
        /// Writes serializable.
        /// </summary>
        /// <param name="writer">The writer.</param>
        /// <param name="o">The object.</param>
        private static void WriteSerializable(BinaryWriterImpl writer, object o)
        {
            writer.Write(new SerializableObjectHolder(o));
        }

        /**
         * <summary>Read enum array.</summary>
         */
        private static object ReadEnumArray(BinaryReaderImpl ctx, Type type)
        {
            return BinaryUtils.ReadTypedArray(ctx, true, type.GetElementType());
        }

        /**
         * <summary>Read array.</summary>
         */
        private static object ReadArray(BinaryReaderImpl ctx, Type type)
        {
            var elemType = type.IsArray ? type.GetElementType() : typeof(object);

            return BinaryUtils.ReadTypedArray(ctx, true, elemType);
        }

        /**
         * <summary>Read collection.</summary>
         */
        private static object ReadCollection(BinaryReaderImpl ctx, Type type)
        {
            return BinaryUtils.ReadCollection(ctx, null, null);
        }

        /**
         * <summary>Read dictionary.</summary>
         */
        private static object ReadDictionary(BinaryReaderImpl ctx, Type type)
        {
            return BinaryUtils.ReadDictionary(ctx, null);
        }

        /**
         * <summary>Read map entry.</summary>
         */
        private static object ReadMapEntry(BinaryReaderImpl ctx, Type type)
        {
            return BinaryUtils.ReadMapEntry(ctx);
        }

        /**
         * <summary>Add element to array list.</summary>
         * <param name="col">Array list.</param>
         * <param name="elem">Element.</param>
         */


        /**
         * <summary>Read delegate.</summary>
         * <param name="ctx">Read context.</param>
         * <param name="type">Type.</param>
         */
        private delegate object PortableSystemReadDelegate(BinaryReaderImpl ctx, Type type);

        /// <summary>
        /// System type reader.
        /// </summary>
        private interface IPortableSystemReader
        {
            /// <summary>
            /// Reads a value of specified type from reader.
            /// </summary>
            T Read<T>(BinaryReaderImpl ctx);
        }

        /// <summary>
        /// System type generic reader.
        /// </summary>
        private interface IPortableSystemReader<out T>
        {
            /// <summary>
            /// Reads a value of specified type from reader.
            /// </summary>
            T Read(BinaryReaderImpl ctx);
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
            public T Read<T>(BinaryReaderImpl ctx)
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
            public TResult Read<TResult>(BinaryReaderImpl ctx)
            {
                return TypeCaster<TResult>.Cast(_readDelegate(ctx.Stream));
            }
        }

        /// <summary>
        /// Reader without boxing.
        /// </summary>
        private class PortableSystemTypedArrayReader<T> : IPortableSystemReader
        {
            public TResult Read<TResult>(BinaryReaderImpl ctx)
            {
                return TypeCaster<TResult>.Cast(BinaryUtils.ReadArray<T>(ctx, false));
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
            T2 IPortableSystemReader<T2>.Read(BinaryReaderImpl ctx)
            {
                return _readDelegate2(ctx.Stream);
            }

            /** <inheritdoc /> */
            public T Read<T>(BinaryReaderImpl ctx)
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
