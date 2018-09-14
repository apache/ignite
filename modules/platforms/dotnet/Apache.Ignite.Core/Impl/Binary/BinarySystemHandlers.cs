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

        /// <summary>
        /// Initializes the <see cref="BinarySystemHandlers"/> class.
        /// </summary>
        [SuppressMessage("Microsoft.Performance", "CA1810:InitializeReferenceTypeStaticFieldsInline", 
            Justification = "Readability.")]
        static BinarySystemHandlers()
        {
            // 1. Primitives.
            ReadHandlers[BinaryTypeId.Bool] = new BinarySystemReader<bool>(s => s.ReadBool());
            ReadHandlers[BinaryTypeId.Byte] = new BinarySystemReader<byte>(s => s.ReadByte());
            ReadHandlers[BinaryTypeId.Short] = new BinarySystemReader<short>(s => s.ReadShort());
            ReadHandlers[BinaryTypeId.Char] = new BinarySystemReader<char>(s => s.ReadChar());
            ReadHandlers[BinaryTypeId.Int] = new BinarySystemReader<int>(s => s.ReadInt());
            ReadHandlers[BinaryTypeId.Long] = new BinarySystemReader<long>(s => s.ReadLong());
            ReadHandlers[BinaryTypeId.Float] = new BinarySystemReader<float>(s => s.ReadFloat());
            ReadHandlers[BinaryTypeId.Double] = new BinarySystemReader<double>(s => s.ReadDouble());
            ReadHandlers[BinaryTypeId.Decimal] = new BinarySystemReader<decimal?>(BinaryUtils.ReadDecimal);

            // 2. Date.
            ReadHandlers[BinaryTypeId.Timestamp] = new BinarySystemReader<DateTime?>(BinaryUtils.ReadTimestamp);

            // 3. String.
            ReadHandlers[BinaryTypeId.String] = new BinarySystemReader<string>(BinaryUtils.ReadString);

            // 4. Guid.
            ReadHandlers[BinaryTypeId.Guid] = new BinarySystemReader<Guid?>(s => BinaryUtils.ReadGuid(s));

            // 5. Primitive arrays.
            ReadHandlers[BinaryTypeId.ArrayBool] = new BinarySystemReader<bool[]>(BinaryUtils.ReadBooleanArray);

            ReadHandlers[BinaryTypeId.ArrayByte] =
                new BinarySystemDualReader<byte[], sbyte[]>(BinaryUtils.ReadByteArray, BinaryUtils.ReadSbyteArray);
            
            ReadHandlers[BinaryTypeId.ArrayShort] =
                new BinarySystemDualReader<short[], ushort[]>(BinaryUtils.ReadShortArray,
                    BinaryUtils.ReadUshortArray);

            ReadHandlers[BinaryTypeId.ArrayChar] = 
                new BinarySystemReader<char[]>(BinaryUtils.ReadCharArray);

            ReadHandlers[BinaryTypeId.ArrayInt] =
                new BinarySystemDualReader<int[], uint[]>(BinaryUtils.ReadIntArray, BinaryUtils.ReadUintArray);
            
            ReadHandlers[BinaryTypeId.ArrayLong] =
                new BinarySystemDualReader<long[], ulong[]>(BinaryUtils.ReadLongArray, 
                    BinaryUtils.ReadUlongArray);

            ReadHandlers[BinaryTypeId.ArrayFloat] =
                new BinarySystemReader<float[]>(BinaryUtils.ReadFloatArray);

            ReadHandlers[BinaryTypeId.ArrayDouble] =
                new BinarySystemReader<double[]>(BinaryUtils.ReadDoubleArray);

            ReadHandlers[BinaryTypeId.ArrayDecimal] =
                new BinarySystemReader<decimal?[]>(BinaryUtils.ReadDecimalArray);

            // 6. Date array.
            ReadHandlers[BinaryTypeId.ArrayTimestamp] =
                new BinarySystemReader<DateTime?[]>(BinaryUtils.ReadTimestampArray);

            // 7. String array.
            ReadHandlers[BinaryTypeId.ArrayString] = new BinarySystemTypedArrayReader<string>();

            // 8. Guid array.
            ReadHandlers[BinaryTypeId.ArrayGuid] = new BinarySystemTypedArrayReader<Guid?>();

            // 9. Array.
            ReadHandlers[BinaryTypeId.Array] = new BinarySystemReader(ReadArray);

            // 11. Arbitrary collection.
            ReadHandlers[BinaryTypeId.Collection] = new BinarySystemReader(ReadCollection);

            // 13. Arbitrary dictionary.
            ReadHandlers[BinaryTypeId.Dictionary] = new BinarySystemReader(ReadDictionary);

            // 14. Enum. Should be read as Array, see WriteEnumArray implementation.
            ReadHandlers[BinaryTypeId.ArrayEnum] = new BinarySystemReader(ReadArray);

            // 15. Optimized marshaller objects.
            ReadHandlers[BinaryTypeId.OptimizedMarshaller] = new BinarySystemReader(ReadOptimizedMarshallerObject);
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
                if (type.GetArrayRank() > 1)
                {
                    // int[,]-style arrays are wrapped, see comments in holder.
                    return new BinarySystemWriteHandler<Array>(
                        (w, o) => w.WriteObject(new MultidimensionalArrayHolder(o)), true);
                }

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
                if (BinaryUtils.IsIgniteEnum(elemType) || elemType == typeof(BinaryEnum))
                    return new BinarySystemWriteHandler<object>(WriteEnumArray, true);

                // Object array.
                return new BinarySystemWriteHandler<object>(WriteArray, true);
            }

            if (type == typeof(OptimizedMarshallerObject))
            {
                return new BinarySystemWriteHandler<OptimizedMarshallerObject>((w, o) => o.Write(w.Stream), false);
            }

            return null;
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
            ctx.Stream.WriteByte(BinaryTypeId.Decimal);

            BinaryUtils.WriteDecimal(obj, ctx.Stream);
        }
        
        /// <summary>
        /// Write string.
        /// </summary>
        /// <param name="ctx">Context.</param>
        /// <param name="obj">Object.</param>
        private static void WriteString(BinaryWriter ctx, string obj)
        {
            ctx.Stream.WriteByte(BinaryTypeId.String);

            BinaryUtils.WriteString(obj, ctx.Stream);
        }

        /// <summary>
        /// Write Guid.
        /// </summary>
        /// <param name="ctx">Context.</param>
        /// <param name="obj">Value.</param>
        private static void WriteGuid(BinaryWriter ctx, Guid obj)
        {
            ctx.Stream.WriteByte(BinaryTypeId.Guid);

            BinaryUtils.WriteGuid(obj, ctx.Stream);
        }

        /// <summary>
        /// Write boolaen array.
        /// </summary>
        /// <param name="ctx">Context.</param>
        /// <param name="obj">Value.</param>
        private static void WriteBoolArray(BinaryWriter ctx, bool[] obj)
        {
            ctx.Stream.WriteByte(BinaryTypeId.ArrayBool);

            BinaryUtils.WriteBooleanArray(obj, ctx.Stream);
        }
        
        /// <summary>
        /// Write byte array.
        /// </summary>
        /// <param name="ctx">Context.</param>
        /// <param name="obj">Value.</param>
        private static void WriteByteArray(BinaryWriter ctx, byte[] obj)
        {
            ctx.Stream.WriteByte(BinaryTypeId.ArrayByte);

            BinaryUtils.WriteByteArray(obj, ctx.Stream);
        }

        /// <summary>
        /// Write short array.
        /// </summary>
        /// <param name="ctx">Context.</param>
        /// <param name="obj">Value.</param>
        private static void WriteShortArray(BinaryWriter ctx, short[] obj)
        {
            ctx.Stream.WriteByte(BinaryTypeId.ArrayShort);

            BinaryUtils.WriteShortArray(obj, ctx.Stream);
        }
        
        /// <summary>
        /// Write char array.
        /// </summary>
        /// <param name="ctx">Context.</param>
        /// <param name="obj">Value.</param>
        private static void WriteCharArray(BinaryWriter ctx, object obj)
        {
            ctx.Stream.WriteByte(BinaryTypeId.ArrayChar);

            BinaryUtils.WriteCharArray((char[])obj, ctx.Stream);
        }

        /// <summary>
        /// Write int array.
        /// </summary>
        /// <param name="ctx">Context.</param>
        /// <param name="obj">Value.</param>
        private static void WriteIntArray(BinaryWriter ctx, int[] obj)
        {
            ctx.Stream.WriteByte(BinaryTypeId.ArrayInt);

            BinaryUtils.WriteIntArray(obj, ctx.Stream);
        }

        /// <summary>
        /// Write long array.
        /// </summary>
        /// <param name="ctx">Context.</param>
        /// <param name="obj">Value.</param>
        private static void WriteLongArray(BinaryWriter ctx, long[] obj)
        {
            ctx.Stream.WriteByte(BinaryTypeId.ArrayLong);

            BinaryUtils.WriteLongArray(obj, ctx.Stream);
        }

        /// <summary>
        /// Write float array.
        /// </summary>
        /// <param name="ctx">Context.</param>
        /// <param name="obj">Value.</param>
        private static void WriteFloatArray(BinaryWriter ctx, float[] obj)
        {
            ctx.Stream.WriteByte(BinaryTypeId.ArrayFloat);

            BinaryUtils.WriteFloatArray(obj, ctx.Stream);
        }

        /// <summary>
        /// Write double array.
        /// </summary>
        /// <param name="ctx">Context.</param>
        /// <param name="obj">Value.</param>
        private static void WriteDoubleArray(BinaryWriter ctx, double[] obj)
        {
            ctx.Stream.WriteByte(BinaryTypeId.ArrayDouble);

            BinaryUtils.WriteDoubleArray(obj, ctx.Stream);
        }

        /// <summary>
        /// Write decimal array.
        /// </summary>
        /// <param name="ctx">Context.</param>
        /// <param name="obj">Value.</param>
        private static void WriteDecimalArray(BinaryWriter ctx, decimal?[] obj)
        {
            ctx.Stream.WriteByte(BinaryTypeId.ArrayDecimal);

            BinaryUtils.WriteDecimalArray(obj, ctx.Stream);
        }
        
        /// <summary>
        /// Write string array.
        /// </summary>
        /// <param name="ctx">Context.</param>
        /// <param name="obj">Value.</param>
        private static void WriteStringArray(BinaryWriter ctx, string[] obj)
        {
            ctx.Stream.WriteByte(BinaryTypeId.ArrayString);

            BinaryUtils.WriteStringArray(obj, ctx.Stream);
        }
        
        /// <summary>
        /// Write nullable GUID array.
        /// </summary>
        /// <param name="ctx">Context.</param>
        /// <param name="obj">Value.</param>
        private static void WriteGuidArray(BinaryWriter ctx, Guid?[] obj)
        {
            ctx.Stream.WriteByte(BinaryTypeId.ArrayGuid);

            BinaryUtils.WriteGuidArray(obj, ctx.Stream);
        }

        /// <summary>
        /// Writes the enum array.
        /// </summary>
        private static void WriteEnumArray(BinaryWriter ctx, object obj)
        {
            ctx.Stream.WriteByte(BinaryTypeId.ArrayEnum);

            BinaryUtils.WriteArray((Array) obj, ctx);
        }

        /// <summary>
        /// Writes the array.
        /// </summary>
        private static void WriteArray(BinaryWriter ctx, object obj)
        {
            ctx.Stream.WriteByte(BinaryTypeId.Array);

            BinaryUtils.WriteArray((Array) obj, ctx);
        }

        /**
         * <summary>Write ArrayList.</summary>
         */
        private static void WriteArrayList(BinaryWriter ctx, ICollection obj)
        {
            ctx.Stream.WriteByte(BinaryTypeId.Collection);

            BinaryUtils.WriteCollection(obj, ctx, BinaryUtils.CollectionArrayList);
        }

        /**
         * <summary>Write Hashtable.</summary>
         */
        private static void WriteHashtable(BinaryWriter ctx, IDictionary obj)
        {
            ctx.Stream.WriteByte(BinaryTypeId.Dictionary);

            BinaryUtils.WriteDictionary(obj, ctx, BinaryUtils.MapHashMap);
        }

        /**
         * <summary>Write binary object.</summary>
         */
        private static void WriteBinary(BinaryWriter ctx, BinaryObject obj)
        {
            ctx.Stream.WriteByte(BinaryTypeId.Binary);

            BinaryUtils.WriteBinary(ctx.Stream, obj);
        }
        
        /// <summary>
        /// Write enum.
        /// </summary>
        private static void WriteBinaryEnum(BinaryWriter ctx, BinaryEnum obj)
        {
            var binEnum = obj;

            ctx.Stream.WriteByte(BinaryTypeId.BinaryEnum);

            ctx.WriteInt(binEnum.TypeId);
            ctx.WriteInt(binEnum.EnumValue);
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
                    elemType = BinaryUtils.GetArrayElementType(typeId, ctx.Marshaller);

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

        /// <summary>
        /// Reads the optimized marshaller object.
        /// </summary>
        private static object ReadOptimizedMarshallerObject(BinaryReader ctx, Type type)
        {
            return new OptimizedMarshallerObject(ctx.Stream);
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
