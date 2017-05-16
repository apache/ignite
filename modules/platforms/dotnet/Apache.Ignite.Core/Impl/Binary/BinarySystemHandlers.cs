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
    using Apache.Ignite.Core.Impl.Binary.IO;
    using Apache.Ignite.Core.Impl.Common;

    /**
     * <summary>Collection of predefined handlers for various system types.</summary>
     */
    internal static class BinarySystemHandlers
    {
        /** Write handlers. */
        private static readonly CopyOnWriteConcurrentDictionary<Type, IBinarySystemWriteHandler> WriteHandlers =
            new CopyOnWriteConcurrentDictionary<Type, IBinarySystemWriteHandler>();

        /** Read handlers. */
        private static readonly IBinarySystemReader[] ReadHandlers = new IBinarySystemReader[255];

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
        /// Initializes the <see cref="BinarySystemHandlers"/> class.
        /// </summary>
        [SuppressMessage("Microsoft.Performance", "CA1810:InitializeReferenceTypeStaticFieldsInline", 
            Justification = "Readability.")]
        static BinarySystemHandlers()
        {
            // 1. Primitives.
            ReadHandlers[BinaryUtils.TypeBool] = new BinarySystemReader<bool>(s => s.ReadBool());
            ReadHandlers[BinaryUtils.TypeByte] = new BinarySystemReader<byte>(s => s.ReadByte());
            ReadHandlers[BinaryUtils.TypeShort] = new BinarySystemReader<short>(s => s.ReadShort());
            ReadHandlers[BinaryUtils.TypeChar] = new BinarySystemReader<char>(s => s.ReadChar());
            ReadHandlers[BinaryUtils.TypeInt] = new BinarySystemReader<int>(s => s.ReadInt());
            ReadHandlers[BinaryUtils.TypeLong] = new BinarySystemReader<long>(s => s.ReadLong());
            ReadHandlers[BinaryUtils.TypeFloat] = new BinarySystemReader<float>(s => s.ReadFloat());
            ReadHandlers[BinaryUtils.TypeDouble] = new BinarySystemReader<double>(s => s.ReadDouble());
            ReadHandlers[BinaryUtils.TypeDecimal] = new BinarySystemReader<decimal?>(BinaryUtils.ReadDecimal);

            // 2. Date.
            ReadHandlers[BinaryUtils.TypeTimestamp] = new BinarySystemReader<DateTime?>(BinaryUtils.ReadTimestamp);

            // 3. String.
            ReadHandlers[BinaryUtils.TypeString] = new BinarySystemReader<string>(BinaryUtils.ReadString);

            // 4. Guid.
            ReadHandlers[BinaryUtils.TypeGuid] = new BinarySystemReader<Guid?>(s => BinaryUtils.ReadGuid(s));

            // 5. Primitive arrays.
            ReadHandlers[BinaryUtils.TypeArrayBool] = new BinarySystemReader<bool[]>(BinaryUtils.ReadBooleanArray);

            ReadHandlers[BinaryUtils.TypeArrayByte] =
                new BinarySystemDualReader<byte[], sbyte[]>(BinaryUtils.ReadByteArray, BinaryUtils.ReadSbyteArray);
            
            ReadHandlers[BinaryUtils.TypeArrayShort] =
                new BinarySystemDualReader<short[], ushort[]>(BinaryUtils.ReadShortArray,
                    BinaryUtils.ReadUshortArray);

            ReadHandlers[BinaryUtils.TypeArrayChar] = 
                new BinarySystemReader<char[]>(BinaryUtils.ReadCharArray);

            ReadHandlers[BinaryUtils.TypeArrayInt] =
                new BinarySystemDualReader<int[], uint[]>(BinaryUtils.ReadIntArray, BinaryUtils.ReadUintArray);
            
            ReadHandlers[BinaryUtils.TypeArrayLong] =
                new BinarySystemDualReader<long[], ulong[]>(BinaryUtils.ReadLongArray, 
                    BinaryUtils.ReadUlongArray);

            ReadHandlers[BinaryUtils.TypeArrayFloat] =
                new BinarySystemReader<float[]>(BinaryUtils.ReadFloatArray);

            ReadHandlers[BinaryUtils.TypeArrayDouble] =
                new BinarySystemReader<double[]>(BinaryUtils.ReadDoubleArray);

            ReadHandlers[BinaryUtils.TypeArrayDecimal] =
                new BinarySystemReader<decimal?[]>(BinaryUtils.ReadDecimalArray);

            // 6. Date array.
            ReadHandlers[BinaryUtils.TypeArrayTimestamp] =
                new BinarySystemReader<DateTime?[]>(BinaryUtils.ReadTimestampArray);

            // 7. String array.
            ReadHandlers[BinaryUtils.TypeArrayString] = new BinarySystemTypedArrayReader<string>();

            // 8. Guid array.
            ReadHandlers[BinaryUtils.TypeArrayGuid] = new BinarySystemTypedArrayReader<Guid?>();

            // 9. Array.
            ReadHandlers[BinaryUtils.TypeArray] = new BinarySystemReader(ReadArray);

            // 11. Arbitrary collection.
            ReadHandlers[BinaryUtils.TypeCollection] = new BinarySystemReader(ReadCollection);

            // 13. Arbitrary dictionary.
            ReadHandlers[BinaryUtils.TypeDictionary] = new BinarySystemReader(ReadDictionary);
            
            // 14. Enum.
            ReadHandlers[BinaryUtils.TypeArrayEnum] = new BinarySystemReader(ReadEnumArray);
        }

        /// <summary>
        /// Try getting write handler for type.
        /// </summary>
        /// <param name="type"></param>
        /// <returns></returns>
        public static IBinarySystemWriteHandler GetWriteHandler(Type type)
        {
            return WriteHandlers.GetOrAdd(type, t =>
            {
                return FindWriteHandler(t);
            });
        }

        /// <summary>
        /// Find write handler for type.
        /// </summary>
        /// <param name="type">Type.</param>
        /// <returns>
        /// Write handler or NULL.
        /// </returns>
        private static IBinarySystemWriteHandler FindWriteHandler(Type type)
        {
            // 1. Well-known types.
            if (type == typeof(string))
                return new BinarySystemWriteHandler<string>(WriteString, false);
            if (type == typeof(decimal))
                return new BinarySystemWriteHandler<decimal>(WriteDecimal, false);
            if (type == typeof(Guid))
                return new BinarySystemWriteHandler<Guid>(WriteGuid, false);
            if (type == typeof (BinaryObject))
                return new BinarySystemWriteHandler<BinaryObject>(WriteBinary, false);
            if (type == typeof (BinaryEnum))
                return new BinarySystemWriteHandler<BinaryEnum>(WriteBinaryEnum, false);
            if (type.IsEnum)
            {
                var underlyingType = Enum.GetUnderlyingType(type);

                if (underlyingType == typeof(int))
                    return new BinarySystemWriteHandler<int>((w, i) => w.WriteEnum(i, type), false);
                if (underlyingType == typeof(uint))
                    return new BinarySystemWriteHandler<uint>((w, i) => w.WriteEnum(unchecked((int) i), type), false);
                if (underlyingType == typeof(byte))
                    return new BinarySystemWriteHandler<byte>((w, i) => w.WriteEnum(i, type), false);
                if (underlyingType == typeof(sbyte))
                    return new BinarySystemWriteHandler<sbyte>((w, i) => w.WriteEnum(i, type), false);
                if (underlyingType == typeof(short))
                    return new BinarySystemWriteHandler<short>((w, i) => w.WriteEnum(i, type), false);
                if (underlyingType == typeof(ushort))
                    return new BinarySystemWriteHandler<ushort>((w, i) => w.WriteEnum(i, type), false);

                return null; // Other enums, such as long and ulong, can't be expressed as int.
            }
            if (type == typeof(Ignite))
                return new BinarySystemWriteHandler<object>(WriteIgnite, false);

            // All types below can be written as handles.
            if (type == typeof (ArrayList))
                return new BinarySystemWriteHandler<ICollection>(WriteArrayList, true);
            if (type == typeof (Hashtable))
                return new BinarySystemWriteHandler<IDictionary>(WriteHashtable, true);

            if (type.IsArray)
            {
                // We know how to write any array type.
                Type elemType = type.GetElementType();
                
                // Primitives.
                if (elemType == typeof (bool))
                    return new BinarySystemWriteHandler<bool[]>(WriteBoolArray, true);
                if (elemType == typeof(byte))
                    return new BinarySystemWriteHandler<byte[]>(WriteByteArray, true);
                if (elemType == typeof(short))
                    return new BinarySystemWriteHandler<short[]>(WriteShortArray, true);
                if (elemType == typeof(char))
                    return new BinarySystemWriteHandler<char[]>(WriteCharArray, true);
                if (elemType == typeof(int))
                    return new BinarySystemWriteHandler<int[]>(WriteIntArray, true);
                if (elemType == typeof(long))
                    return new BinarySystemWriteHandler<long[]>(WriteLongArray, true);
                if (elemType == typeof(float))
                    return new BinarySystemWriteHandler<float[]>(WriteFloatArray, true);
                if (elemType == typeof(double))
                    return new BinarySystemWriteHandler<double[]>(WriteDoubleArray, true);
                // Non-CLS primitives.
                if (elemType == typeof(sbyte))
                    return new BinarySystemWriteHandler<byte[]>(WriteByteArray, true);
                if (elemType == typeof(ushort))
                    return new BinarySystemWriteHandler<short[]>(WriteShortArray, true);
                if (elemType == typeof(uint))
                    return new BinarySystemWriteHandler<int[]>(WriteIntArray, true);
                if (elemType == typeof(ulong))
                    return new BinarySystemWriteHandler<long[]>(WriteLongArray, true);
                // Special types.
                if (elemType == typeof (decimal?))
                    return new BinarySystemWriteHandler<decimal?[]>(WriteDecimalArray, true);
                if (elemType == typeof(string))
                    return new BinarySystemWriteHandler<string[]>(WriteStringArray, true);
                if (elemType == typeof(Guid?))
                    return new BinarySystemWriteHandler<Guid?[]>(WriteGuidArray, true);
                // Enums.
                if (IsIntEnum(elemType) || elemType == typeof(BinaryEnum))
                    return new BinarySystemWriteHandler<object>(WriteEnumArray, true);

                // Object array.
                return new BinarySystemWriteHandler<object>(WriteArray, true);
            }

            return null;
        }

        /// <summary>
        /// Determines whether specified type is an enum which fits into Int32.
        /// </summary>
        private static bool IsIntEnum(Type type)
        {
            if (!type.IsEnum)
                return false;

            var underlyingType = Enum.GetUnderlyingType(type);

            return underlyingType == typeof(int) 
                || underlyingType == typeof(short)
                || underlyingType == typeof(ushort)
                || underlyingType == typeof(byte)
                || underlyingType == typeof(sbyte);
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
        /// Reads an object of predefined type.
        /// </summary>
        public static bool TryReadSystemType<T>(byte typeId, BinaryReader ctx, out T res)
        {
            var handler = ReadHandlers[typeId];

            if (handler == null)
            {
                res = default(T);
                return false;
            }

            res = handler.Read<T>(ctx);
            return true;
        }
        
        /// <summary>
        /// Write decimal.
        /// </summary>
        /// <param name="ctx">Context.</param>
        /// <param name="obj">Value.</param>
        private static void WriteDecimal(BinaryWriter ctx, decimal obj)
        {
            ctx.Stream.WriteByte(BinaryUtils.TypeDecimal);

            BinaryUtils.WriteDecimal(obj, ctx.Stream);
        }
        
        /// <summary>
        /// Write string.
        /// </summary>
        /// <param name="ctx">Context.</param>
        /// <param name="obj">Object.</param>
        private static void WriteString(BinaryWriter ctx, string obj)
        {
            ctx.Stream.WriteByte(BinaryUtils.TypeString);

            BinaryUtils.WriteString(obj, ctx.Stream);
        }

        /// <summary>
        /// Write Guid.
        /// </summary>
        /// <param name="ctx">Context.</param>
        /// <param name="obj">Value.</param>
        private static void WriteGuid(BinaryWriter ctx, Guid obj)
        {
            ctx.Stream.WriteByte(BinaryUtils.TypeGuid);

            BinaryUtils.WriteGuid(obj, ctx.Stream);
        }

        /// <summary>
        /// Write boolaen array.
        /// </summary>
        /// <param name="ctx">Context.</param>
        /// <param name="obj">Value.</param>
        private static void WriteBoolArray(BinaryWriter ctx, bool[] obj)
        {
            ctx.Stream.WriteByte(BinaryUtils.TypeArrayBool);

            BinaryUtils.WriteBooleanArray(obj, ctx.Stream);
        }
        
        /// <summary>
        /// Write byte array.
        /// </summary>
        /// <param name="ctx">Context.</param>
        /// <param name="obj">Value.</param>
        private static void WriteByteArray(BinaryWriter ctx, byte[] obj)
        {
            ctx.Stream.WriteByte(BinaryUtils.TypeArrayByte);

            BinaryUtils.WriteByteArray(obj, ctx.Stream);
        }

        /// <summary>
        /// Write short array.
        /// </summary>
        /// <param name="ctx">Context.</param>
        /// <param name="obj">Value.</param>
        private static void WriteShortArray(BinaryWriter ctx, short[] obj)
        {
            ctx.Stream.WriteByte(BinaryUtils.TypeArrayShort);

            BinaryUtils.WriteShortArray(obj, ctx.Stream);
        }
        
        /// <summary>
        /// Write char array.
        /// </summary>
        /// <param name="ctx">Context.</param>
        /// <param name="obj">Value.</param>
        private static void WriteCharArray(BinaryWriter ctx, object obj)
        {
            ctx.Stream.WriteByte(BinaryUtils.TypeArrayChar);

            BinaryUtils.WriteCharArray((char[])obj, ctx.Stream);
        }

        /// <summary>
        /// Write int array.
        /// </summary>
        /// <param name="ctx">Context.</param>
        /// <param name="obj">Value.</param>
        private static void WriteIntArray(BinaryWriter ctx, int[] obj)
        {
            ctx.Stream.WriteByte(BinaryUtils.TypeArrayInt);

            BinaryUtils.WriteIntArray(obj, ctx.Stream);
        }

        /// <summary>
        /// Write long array.
        /// </summary>
        /// <param name="ctx">Context.</param>
        /// <param name="obj">Value.</param>
        private static void WriteLongArray(BinaryWriter ctx, long[] obj)
        {
            ctx.Stream.WriteByte(BinaryUtils.TypeArrayLong);

            BinaryUtils.WriteLongArray(obj, ctx.Stream);
        }

        /// <summary>
        /// Write float array.
        /// </summary>
        /// <param name="ctx">Context.</param>
        /// <param name="obj">Value.</param>
        private static void WriteFloatArray(BinaryWriter ctx, float[] obj)
        {
            ctx.Stream.WriteByte(BinaryUtils.TypeArrayFloat);

            BinaryUtils.WriteFloatArray(obj, ctx.Stream);
        }

        /// <summary>
        /// Write double array.
        /// </summary>
        /// <param name="ctx">Context.</param>
        /// <param name="obj">Value.</param>
        private static void WriteDoubleArray(BinaryWriter ctx, double[] obj)
        {
            ctx.Stream.WriteByte(BinaryUtils.TypeArrayDouble);

            BinaryUtils.WriteDoubleArray(obj, ctx.Stream);
        }

        /// <summary>
        /// Write decimal array.
        /// </summary>
        /// <param name="ctx">Context.</param>
        /// <param name="obj">Value.</param>
        private static void WriteDecimalArray(BinaryWriter ctx, decimal?[] obj)
        {
            ctx.Stream.WriteByte(BinaryUtils.TypeArrayDecimal);

            BinaryUtils.WriteDecimalArray(obj, ctx.Stream);
        }
        
        /// <summary>
        /// Write string array.
        /// </summary>
        /// <param name="ctx">Context.</param>
        /// <param name="obj">Value.</param>
        private static void WriteStringArray(BinaryWriter ctx, string[] obj)
        {
            ctx.Stream.WriteByte(BinaryUtils.TypeArrayString);

            BinaryUtils.WriteStringArray(obj, ctx.Stream);
        }
        
        /// <summary>
        /// Write nullable GUID array.
        /// </summary>
        /// <param name="ctx">Context.</param>
        /// <param name="obj">Value.</param>
        private static void WriteGuidArray(BinaryWriter ctx, Guid?[] obj)
        {
            ctx.Stream.WriteByte(BinaryUtils.TypeArrayGuid);

            BinaryUtils.WriteGuidArray(obj, ctx.Stream);
        }

        /// <summary>
        /// Writes the enum array.
        /// </summary>
        private static void WriteEnumArray(BinaryWriter ctx, object obj)
        {
            ctx.Stream.WriteByte(BinaryUtils.TypeArrayEnum);

            BinaryUtils.WriteArray((Array) obj, ctx);
        }

        /// <summary>
        /// Writes the array.
        /// </summary>
        private static void WriteArray(BinaryWriter ctx, object obj)
        {
            ctx.Stream.WriteByte(BinaryUtils.TypeArray);

            BinaryUtils.WriteArray((Array) obj, ctx);
        }

        /**
         * <summary>Write ArrayList.</summary>
         */
        private static void WriteArrayList(BinaryWriter ctx, ICollection obj)
        {
            ctx.Stream.WriteByte(BinaryUtils.TypeCollection);

            BinaryUtils.WriteCollection(obj, ctx, BinaryUtils.CollectionArrayList);
        }

        /**
         * <summary>Write Hashtable.</summary>
         */
        private static void WriteHashtable(BinaryWriter ctx, IDictionary obj)
        {
            ctx.Stream.WriteByte(BinaryUtils.TypeDictionary);

            BinaryUtils.WriteDictionary(obj, ctx, BinaryUtils.MapHashMap);
        }

        /**
         * <summary>Write binary object.</summary>
         */
        private static void WriteBinary(BinaryWriter ctx, BinaryObject obj)
        {
            ctx.Stream.WriteByte(BinaryUtils.TypeBinary);

            BinaryUtils.WriteBinary(ctx.Stream, obj);
        }
        
        /// <summary>
        /// Write enum.
        /// </summary>
        private static void WriteBinaryEnum(BinaryWriter ctx, BinaryEnum obj)
        {
            var binEnum = obj;

            ctx.Stream.WriteByte(BinaryUtils.TypeEnum);

            ctx.WriteInt(binEnum.TypeId);
            ctx.WriteInt(binEnum.EnumValue);
        }

        /**
         * <summary>Read enum array.</summary>
         */
        private static object ReadEnumArray(BinaryReader ctx, Type type)
        {
            var elemType = type.GetElementType() ?? typeof(object);

            return BinaryUtils.ReadTypedArray(ctx, true, elemType);
        }

        /// <summary>
        /// Reads the array.
        /// </summary>
        private static object ReadArray(BinaryReader ctx, Type type)
        {
            var elemType = type.GetElementType();

            if (elemType == null)
            {
                if (ctx.Mode == BinaryMode.ForceBinary)
                {
                    // Forced binary mode: use object because primitives are not represented as IBinaryObject.
                    elemType = typeof(object);
                }
                else
                {
                    // Infer element type from typeId.
                    var typeId = ctx.ReadInt();

                    if (typeId != BinaryUtils.ObjTypeId)
                    {
                        elemType = ctx.Marshaller.GetDescriptor(true, typeId, true).Type;
                    }

                    return BinaryUtils.ReadTypedArray(ctx, false, elemType ?? typeof(object));
                }
            }

            // Element type is known, no need to check typeId.
            // In case of incompatible types we'll get exception either way.
            return BinaryUtils.ReadTypedArray(ctx, true, elemType);
        }

        /**
         * <summary>Read collection.</summary>
         */
        private static object ReadCollection(BinaryReader ctx, Type type)
        {
            return BinaryUtils.ReadCollection(ctx, null, null);
        }

        /**
         * <summary>Read dictionary.</summary>
         */
        private static object ReadDictionary(BinaryReader ctx, Type type)
        {
            return BinaryUtils.ReadDictionary(ctx, null);
        }
                
        /// <summary>
        /// Write Ignite.
        /// </summary>
        private static void WriteIgnite(BinaryWriter ctx, object obj)
        {
            ctx.Stream.WriteByte(BinaryUtils.HdrNull);
        }

        /**
         * <summary>Read delegate.</summary>
         * <param name="ctx">Read context.</param>
         * <param name="type">Type.</param>
         */
        private delegate object BinarySystemReadDelegate(BinaryReader ctx, Type type);

        /// <summary>
        /// System type reader.
        /// </summary>
        private interface IBinarySystemReader
        {
            /// <summary>
            /// Reads a value of specified type from reader.
            /// </summary>
            T Read<T>(BinaryReader ctx);
        }

        /// <summary>
        /// System type generic reader.
        /// </summary>
        private interface IBinarySystemReader<out T>
        {
            /// <summary>
            /// Reads a value of specified type from reader.
            /// </summary>
            T Read(BinaryReader ctx);
        }

        /// <summary>
        /// Default reader with boxing.
        /// </summary>
        private class BinarySystemReader : IBinarySystemReader
        {
            /** */
            private readonly BinarySystemReadDelegate _readDelegate;

            /// <summary>
            /// Initializes a new instance of the <see cref="BinarySystemReader"/> class.
            /// </summary>
            /// <param name="readDelegate">The read delegate.</param>
            public BinarySystemReader(BinarySystemReadDelegate readDelegate)
            {
                Debug.Assert(readDelegate != null);

                _readDelegate = readDelegate;
            }

            /** <inheritdoc /> */
            public T Read<T>(BinaryReader ctx)
            {
                return (T)_readDelegate(ctx, typeof(T));
            }
        }

        /// <summary>
        /// Reader without boxing.
        /// </summary>
        private class BinarySystemReader<T> : IBinarySystemReader
        {
            /** */
            private readonly Func<IBinaryStream, T> _readDelegate;

            /// <summary>
            /// Initializes a new instance of the <see cref="BinarySystemReader{T}"/> class.
            /// </summary>
            /// <param name="readDelegate">The read delegate.</param>
            public BinarySystemReader(Func<IBinaryStream, T> readDelegate)
            {
                Debug.Assert(readDelegate != null);

                _readDelegate = readDelegate;
            }

            /** <inheritdoc /> */
            public TResult Read<TResult>(BinaryReader ctx)
            {
                return TypeCaster<TResult>.Cast(_readDelegate(ctx.Stream));
            }
        }

        /// <summary>
        /// Reader without boxing.
        /// </summary>
        private class BinarySystemTypedArrayReader<T> : IBinarySystemReader
        {
            public TResult Read<TResult>(BinaryReader ctx)
            {
                return TypeCaster<TResult>.Cast(BinaryUtils.ReadArray<T>(ctx, false));
            }
        }

        /// <summary>
        /// Reader with selection based on requested type.
        /// </summary>
        private class BinarySystemDualReader<T1, T2> : IBinarySystemReader, IBinarySystemReader<T2>
        {
            /** */
            private readonly Func<IBinaryStream, T1> _readDelegate1;

            /** */
            private readonly Func<IBinaryStream, T2> _readDelegate2;

            /// <summary>
            /// Initializes a new instance of the <see cref="BinarySystemDualReader{T1,T2}"/> class.
            /// </summary>
            /// <param name="readDelegate1">The read delegate1.</param>
            /// <param name="readDelegate2">The read delegate2.</param>
            public BinarySystemDualReader(Func<IBinaryStream, T1> readDelegate1, Func<IBinaryStream, T2> readDelegate2)
            {
                Debug.Assert(readDelegate1 != null);
                Debug.Assert(readDelegate2 != null);

                _readDelegate1 = readDelegate1;
                _readDelegate2 = readDelegate2;
            }

            /** <inheritdoc /> */
            [SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods")]
            T2 IBinarySystemReader<T2>.Read(BinaryReader ctx)
            {
                return _readDelegate2(ctx.Stream);
            }

            /** <inheritdoc /> */
            public T Read<T>(BinaryReader ctx)
            {
                // Can't use "as" because of variance. 
                // For example, IBinarySystemReader<byte[]> can be cast to IBinarySystemReader<sbyte[]>, which
                // will cause incorrect behavior.
                if (typeof (T) == typeof (T2))  
                    return ((IBinarySystemReader<T>) this).Read(ctx);

                return TypeCaster<T>.Cast(_readDelegate1(ctx.Stream));
            }
        }
    }

    /// <summary>
    /// Write delegate + handles flag.
    /// </summary>
    internal interface IBinarySystemWriteHandler
    {
        /// <summary>
        /// Gets a value indicating whether this handler supports handles.
        /// </summary>
        bool SupportsHandles { get; }

        /// <summary>
        /// Writes object to a specified writer.
        /// </summary>
        /// <param name="writer">The writer.</param>
        /// <param name="obj">The object.</param>
        void Write<T>(BinaryWriter writer, T obj);
    }

    /// <summary>
    /// Write delegate + handles flag.
    /// </summary>
    internal class BinarySystemWriteHandler<T1> : IBinarySystemWriteHandler
    {
        /** */
        private readonly Action<BinaryWriter, T1> _writeAction;

        /** */
        private readonly bool _supportsHandles;

        /// <summary>
        /// Initializes a new instance of the <see cref="BinarySystemWriteHandler{T1}" /> class.
        /// </summary>
        /// <param name="writeAction">The write action.</param>
        /// <param name="supportsHandles">Handles flag.</param>
        public BinarySystemWriteHandler(Action<BinaryWriter, T1> writeAction, bool supportsHandles)
        {
            Debug.Assert(writeAction != null);

            _writeAction = writeAction;
            _supportsHandles = supportsHandles;
        }

        /** <inheritdoc /> */
        public void Write<T>(BinaryWriter writer, T obj)
        {
            _writeAction(writer, TypeCaster<T1>.Cast(obj));
        }

        /** <inheritdoc /> */
        public bool SupportsHandles
        {
            get { return _supportsHandles; }
        }
    }
}
