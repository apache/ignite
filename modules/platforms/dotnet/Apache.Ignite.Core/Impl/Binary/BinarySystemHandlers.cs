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
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Impl.Binary.IO;
    using Apache.Ignite.Core.Impl.Common;

    /**
     * <summary>Collection of predefined handlers for various system types.</summary>
     */
    internal static class BinarySystemHandlers
    {
        /** Write handlers. */
        private static readonly CopyOnWriteConcurrentDictionary<Type, BinarySystemWriteHandler> WriteHandlers =
            new CopyOnWriteConcurrentDictionary<Type, BinarySystemWriteHandler>();

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
            ReadHandlers[BinaryUtils.TypeGuid] = new BinarySystemReader<Guid?>(BinaryUtils.ReadGuid);

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
        public static BinarySystemWriteHandler GetWriteHandler(Type type)
        {
            return WriteHandlers.GetOrAdd(type, t =>
            {
                bool supportsHandles;

                var handler = FindWriteHandler(t, out supportsHandles);

                return handler == null ? null : new BinarySystemWriteHandler(handler, supportsHandles, 
                    handler == WriteSerializable);
            });
        }

        /// <summary>
        /// Find write handler for type.
        /// </summary>
        /// <param name="type">Type.</param>
        /// <param name="supportsHandles">Flag indicating whether returned delegate supports handles.</param>
        /// <returns>
        /// Write handler or NULL.
        /// </returns>
        private static Action<BinaryWriter, object> FindWriteHandler(Type type, out bool supportsHandles)
        {
            supportsHandles = false;

            // 1. Well-known types.
            if (type == typeof(string))
                return WriteString;
            if (type == typeof(decimal))
                return WriteDecimal;
            if (type == typeof(DateTime))
                return WriteDate;
            if (type == typeof(Guid))
                return WriteGuid;
            if (type == typeof (BinaryObject))
                return WriteBinary;
            if (type == typeof (BinaryEnum))
                return WriteBinaryEnum;
            if (type.IsEnum)
                return WriteEnum;

            // All types below can be written as handles.
            supportsHandles = true;

            if (type == typeof (ArrayList))
                return WriteArrayList;
            if (type == typeof (Hashtable))
                return WriteHashtable;

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
                if (elemType.IsEnum || elemType == typeof(BinaryEnum))
                    return WriteEnumArray;
                
                // Object array.
                if (elemType == typeof (object) || elemType == typeof (IBinaryObject) ||
                    elemType == typeof (BinaryObject))
                    return WriteArray;
            }

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
        private static void WriteDecimal(BinaryWriter ctx, object obj)
        {
            ctx.Stream.WriteByte(BinaryUtils.TypeDecimal);

            BinaryUtils.WriteDecimal((decimal)obj, ctx.Stream);
        }
        
        /// <summary>
        /// Write date.
        /// </summary>
        /// <param name="ctx">Context.</param>
        /// <param name="obj">Value.</param>
        private static void WriteDate(BinaryWriter ctx, object obj)
        {
            ctx.Write(new DateTimeHolder((DateTime) obj));
        }
        
        /// <summary>
        /// Write string.
        /// </summary>
        /// <param name="ctx">Context.</param>
        /// <param name="obj">Object.</param>
        private static void WriteString(BinaryWriter ctx, object obj)
        {
            ctx.Stream.WriteByte(BinaryUtils.TypeString);

            BinaryUtils.WriteString((string)obj, ctx.Stream);
        }

        /// <summary>
        /// Write Guid.
        /// </summary>
        /// <param name="ctx">Context.</param>
        /// <param name="obj">Value.</param>
        private static void WriteGuid(BinaryWriter ctx, object obj)
        {
            ctx.Stream.WriteByte(BinaryUtils.TypeGuid);

            BinaryUtils.WriteGuid((Guid)obj, ctx.Stream);
        }

        /// <summary>
        /// Write boolaen array.
        /// </summary>
        /// <param name="ctx">Context.</param>
        /// <param name="obj">Value.</param>
        private static void WriteBoolArray(BinaryWriter ctx, object obj)
        {
            ctx.Stream.WriteByte(BinaryUtils.TypeArrayBool);

            BinaryUtils.WriteBooleanArray((bool[])obj, ctx.Stream);
        }
        
        /// <summary>
        /// Write byte array.
        /// </summary>
        /// <param name="ctx">Context.</param>
        /// <param name="obj">Value.</param>
        private static void WriteByteArray(BinaryWriter ctx, object obj)
        {
            ctx.Stream.WriteByte(BinaryUtils.TypeArrayByte);

            BinaryUtils.WriteByteArray((byte[])obj, ctx.Stream);
        }

        /// <summary>
        /// Write sbyte array.
        /// </summary>
        /// <param name="ctx">Context.</param>
        /// <param name="obj">Value.</param>
        private static void WriteSbyteArray(BinaryWriter ctx, object obj)
        {
            ctx.Stream.WriteByte(BinaryUtils.TypeArrayByte);

            BinaryUtils.WriteByteArray((byte[]) obj, ctx.Stream);
        }

        /// <summary>
        /// Write short array.
        /// </summary>
        /// <param name="ctx">Context.</param>
        /// <param name="obj">Value.</param>
        private static void WriteShortArray(BinaryWriter ctx, object obj)
        {
            ctx.Stream.WriteByte(BinaryUtils.TypeArrayShort);

            BinaryUtils.WriteShortArray((short[])obj, ctx.Stream);
        }
        
        /// <summary>
        /// Write ushort array.
        /// </summary>
        /// <param name="ctx">Context.</param>
        /// <param name="obj">Value.</param>
        private static void WriteUshortArray(BinaryWriter ctx, object obj)
        {
            ctx.Stream.WriteByte(BinaryUtils.TypeArrayShort);

            BinaryUtils.WriteShortArray((short[]) obj, ctx.Stream);
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
        private static void WriteIntArray(BinaryWriter ctx, object obj)
        {
            ctx.Stream.WriteByte(BinaryUtils.TypeArrayInt);

            BinaryUtils.WriteIntArray((int[])obj, ctx.Stream);
        }

        /// <summary>
        /// Write uint array.
        /// </summary>
        /// <param name="ctx">Context.</param>
        /// <param name="obj">Value.</param>
        private static void WriteUintArray(BinaryWriter ctx, object obj)
        {
            ctx.Stream.WriteByte(BinaryUtils.TypeArrayInt);

            BinaryUtils.WriteIntArray((int[]) obj, ctx.Stream);
        }

        /// <summary>
        /// Write long array.
        /// </summary>
        /// <param name="ctx">Context.</param>
        /// <param name="obj">Value.</param>
        private static void WriteLongArray(BinaryWriter ctx, object obj)
        {
            ctx.Stream.WriteByte(BinaryUtils.TypeArrayLong);

            BinaryUtils.WriteLongArray((long[])obj, ctx.Stream);
        }

        /// <summary>
        /// Write ulong array.
        /// </summary>
        /// <param name="ctx">Context.</param>
        /// <param name="obj">Value.</param>
        private static void WriteUlongArray(BinaryWriter ctx, object obj)
        {
            ctx.Stream.WriteByte(BinaryUtils.TypeArrayLong);

            BinaryUtils.WriteLongArray((long[]) obj, ctx.Stream);
        }

        /// <summary>
        /// Write float array.
        /// </summary>
        /// <param name="ctx">Context.</param>
        /// <param name="obj">Value.</param>
        private static void WriteFloatArray(BinaryWriter ctx, object obj)
        {
            ctx.Stream.WriteByte(BinaryUtils.TypeArrayFloat);

            BinaryUtils.WriteFloatArray((float[])obj, ctx.Stream);
        }

        /// <summary>
        /// Write double array.
        /// </summary>
        /// <param name="ctx">Context.</param>
        /// <param name="obj">Value.</param>
        private static void WriteDoubleArray(BinaryWriter ctx, object obj)
        {
            ctx.Stream.WriteByte(BinaryUtils.TypeArrayDouble);

            BinaryUtils.WriteDoubleArray((double[])obj, ctx.Stream);
        }

        /// <summary>
        /// Write decimal array.
        /// </summary>
        /// <param name="ctx">Context.</param>
        /// <param name="obj">Value.</param>
        private static void WriteDecimalArray(BinaryWriter ctx, object obj)
        {
            ctx.Stream.WriteByte(BinaryUtils.TypeArrayDecimal);

            BinaryUtils.WriteDecimalArray((decimal?[])obj, ctx.Stream);
        }
        
        /// <summary>
        /// Write string array.
        /// </summary>
        /// <param name="ctx">Context.</param>
        /// <param name="obj">Value.</param>
        private static void WriteStringArray(BinaryWriter ctx, object obj)
        {
            ctx.Stream.WriteByte(BinaryUtils.TypeArrayString);

            BinaryUtils.WriteStringArray((string[])obj, ctx.Stream);
        }
        
        /// <summary>
        /// Write nullable GUID array.
        /// </summary>
        /// <param name="ctx">Context.</param>
        /// <param name="obj">Value.</param>
        private static void WriteGuidArray(BinaryWriter ctx, object obj)
        {
            ctx.Stream.WriteByte(BinaryUtils.TypeArrayGuid);

            BinaryUtils.WriteGuidArray((Guid?[])obj, ctx.Stream);
        }
        
        /**
         * <summary>Write enum array.</summary>
         */
        private static void WriteEnumArray(BinaryWriter ctx, object obj)
        {
            ctx.Stream.WriteByte(BinaryUtils.TypeArrayEnum);

            BinaryUtils.WriteArray((Array)obj, ctx);
        }

        /**
         * <summary>Write array.</summary>
         */
        private static void WriteArray(BinaryWriter ctx, object obj)
        {
            ctx.Stream.WriteByte(BinaryUtils.TypeArray);

            BinaryUtils.WriteArray((Array)obj, ctx);
        }

        /**
         * <summary>Write ArrayList.</summary>
         */
        private static void WriteArrayList(BinaryWriter ctx, object obj)
        {
            ctx.Stream.WriteByte(BinaryUtils.TypeCollection);

            BinaryUtils.WriteCollection((ICollection)obj, ctx, BinaryUtils.CollectionArrayList);
        }

        /**
         * <summary>Write Hashtable.</summary>
         */
        private static void WriteHashtable(BinaryWriter ctx, object obj)
        {
            ctx.Stream.WriteByte(BinaryUtils.TypeDictionary);

            BinaryUtils.WriteDictionary((IDictionary)obj, ctx, BinaryUtils.MapHashMap);
        }

        /**
         * <summary>Write binary object.</summary>
         */
        private static void WriteBinary(BinaryWriter ctx, object obj)
        {
            ctx.Stream.WriteByte(BinaryUtils.TypeBinary);

            BinaryUtils.WriteBinary(ctx.Stream, (BinaryObject)obj);
        }
        
        /// <summary>
        /// Write enum.
        /// </summary>
        private static void WriteEnum(BinaryWriter ctx, object obj)
        {
            ctx.WriteEnum(obj);
        }

        /// <summary>
        /// Write enum.
        /// </summary>
        private static void WriteBinaryEnum(BinaryWriter ctx, object obj)
        {
            var binEnum = (BinaryEnum) obj;

            ctx.Stream.WriteByte(BinaryUtils.TypeEnum);

            ctx.WriteInt(binEnum.TypeId);
            ctx.WriteInt(binEnum.EnumValue);
        }

        /// <summary>
        /// Writes serializable.
        /// </summary>
        /// <param name="writer">The writer.</param>
        /// <param name="o">The object.</param>
        private static void WriteSerializable(BinaryWriter writer, object o)
        {
            writer.Write(new SerializableObjectHolder(o));
        }

        /**
         * <summary>Read enum array.</summary>
         */
        private static object ReadEnumArray(BinaryReader ctx, Type type)
        {
            var elemType = type.GetElementType() ?? typeof(object);

            return BinaryUtils.ReadTypedArray(ctx, true, elemType);
        }

        /**
         * <summary>Read array.</summary>
         */
        private static object ReadArray(BinaryReader ctx, Type type)
        {
            var elemType = type.GetElementType() ?? typeof(object);

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
    internal class BinarySystemWriteHandler
    {
        /** */
        private readonly Action<BinaryWriter, object> _writeAction;

        /** */
        private readonly bool _supportsHandles;

        /** */
        private readonly bool _isSerializable;

        /// <summary>
        /// Initializes a new instance of the <see cref="BinarySystemWriteHandler" /> class.
        /// </summary>
        /// <param name="writeAction">The write action.</param>
        /// <param name="supportsHandles">Handles flag.</param>
        /// <param name="isSerializable">Determines whether this handler writes objects as serializable.</param>
        public BinarySystemWriteHandler(Action<BinaryWriter, object> writeAction, bool supportsHandles, 
            bool isSerializable)
        {
            Debug.Assert(writeAction != null);

            _writeAction = writeAction;
            _supportsHandles = supportsHandles;
            _isSerializable = isSerializable;
        }

        /// <summary>
        /// Writes object to a specified writer.
        /// </summary>
        /// <param name="writer">The writer.</param>
        /// <param name="obj">The object.</param>
        public void Write(BinaryWriter writer, object obj)
        {
            _writeAction(writer, obj);
        }

        /// <summary>
        /// Gets a value indicating whether this handler supports handles.
        /// </summary>
        public bool SupportsHandles
        {
            get { return _supportsHandles; }
        }

        /// <summary>
        /// Gets or sets a value indicating whether this handler writes objects as serializable
        /// </summary>
        public bool IsSerializable
        {
            get { return _isSerializable; }
        }
    }
}
