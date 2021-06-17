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
    using Apache.Ignite.Core.Impl.Common;

    /**
     * <summary>Collection of predefined handlers for various system types.</summary>
     */
    internal static class BinarySystemHandlers
    {
        /** Write handlers. */
        private static readonly CopyOnWriteConcurrentDictionary<Type, IBinarySystemWriteHandler> WriteHandlers =
            new CopyOnWriteConcurrentDictionary<Type, IBinarySystemWriteHandler>();

        /** */
        private static readonly BinarySystemWriteHandler<DateTime> TimestampWriteHandler =
            new BinarySystemWriteHandler<DateTime>(WriteTimestamp, false);

        /** */
        private static readonly BinarySystemWriteHandler<DateTime?[]> TimestampArrayWriteHandler =
            new BinarySystemWriteHandler<DateTime?[]>(WriteTimestampArray, true);

        /// <summary>
        /// Try getting write handler for type.
        /// </summary>
        /// <param name="type"></param>
        /// <param name="forceTimestamp"></param>
        /// <returns></returns>
        public static IBinarySystemWriteHandler GetWriteHandler(Type type, bool forceTimestamp)
        {
            if (forceTimestamp)
            {
                if (type == typeof(DateTime))
                    return TimestampWriteHandler;

                if (type == typeof(DateTime?[]))
                    return TimestampArrayWriteHandler;
            }

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
            var stream = ctx.Stream;

            switch (typeId)
            {
                case BinaryTypeId.Byte:
                    res = TypeCaster<T>.Cast(stream.ReadByte());
                    return true;

                case BinaryTypeId.Short:
                    res = TypeCaster<T>.Cast(stream.ReadShort());
                    return true;

                case BinaryTypeId.Int:
                    res = TypeCaster<T>.Cast(stream.ReadInt());
                    return true;

                case BinaryTypeId.Long:
                    res = TypeCaster<T>.Cast(stream.ReadLong());
                    return true;

                case BinaryTypeId.Float:
                    res = TypeCaster<T>.Cast(stream.ReadFloat());
                    return true;

                case BinaryTypeId.Double:
                    res = TypeCaster<T>.Cast(stream.ReadDouble());
                    return true;

                case BinaryTypeId.Char:
                    res = TypeCaster<T>.Cast(stream.ReadChar());
                    return true;

                case BinaryTypeId.Bool:
                    res = TypeCaster<T>.Cast(stream.ReadBool());
                    return true;

                case BinaryTypeId.String:
                    res = TypeCaster<T>.Cast(BinaryUtils.ReadString(stream));
                    return true;

                case BinaryTypeId.Guid:
                    res = TypeCaster<T>.Cast(BinaryUtils.ReadGuid(stream));
                    return true;

                case BinaryTypeId.ArrayByte:
                    res = typeof(T) == typeof(sbyte[])
                        ? TypeCaster<T>.Cast(BinaryUtils.ReadSbyteArray(stream))
                        : TypeCaster<T>.Cast(BinaryUtils.ReadByteArray(stream));
                    return true;

                case BinaryTypeId.ArrayShort:
                    res = typeof(T) == typeof(ushort[])
                        ? TypeCaster<T>.Cast(BinaryUtils.ReadUshortArray(stream))
                        : TypeCaster<T>.Cast(BinaryUtils.ReadShortArray(stream));
                    return true;

                case BinaryTypeId.ArrayInt:
                    res = typeof(T) == typeof(uint[])
                        ? TypeCaster<T>.Cast(BinaryUtils.ReadUintArray(stream))
                        : TypeCaster<T>.Cast(BinaryUtils.ReadIntArray(stream));
                    return true;

                case BinaryTypeId.ArrayLong:
                    res = typeof(T) == typeof(ulong[])
                        ? TypeCaster<T>.Cast(BinaryUtils.ReadUlongArray(stream))
                        : TypeCaster<T>.Cast(BinaryUtils.ReadLongArray(stream));
                    return true;

                case BinaryTypeId.ArrayFloat:
                    res = TypeCaster<T>.Cast(BinaryUtils.ReadFloatArray(stream));
                    return true;

                case BinaryTypeId.ArrayDouble:
                    res = TypeCaster<T>.Cast(BinaryUtils.ReadDoubleArray(stream));
                    return true;

                case BinaryTypeId.ArrayChar:
                    res = TypeCaster<T>.Cast(BinaryUtils.ReadCharArray(stream));
                    return true;

                case BinaryTypeId.ArrayBool:
                    res = TypeCaster<T>.Cast(BinaryUtils.ReadBooleanArray(stream));
                    return true;

                case BinaryTypeId.ArrayString:
                    res = TypeCaster<T>.Cast(BinaryUtils.ReadStringArray(stream));
                    return true;

                case BinaryTypeId.ArrayGuid:
                    res = TypeCaster<T>.Cast(BinaryUtils.ReadGuidArray(stream));
                    return true;

                case BinaryTypeId.Array:
                    res = (T) ReadArray(ctx, typeof(T));
                    return true;

                case BinaryTypeId.Collection:
                    res = (T) BinaryUtils.ReadCollection(ctx, null, null);
                    return true;

                case BinaryTypeId.Dictionary:
                    res = (T) BinaryUtils.ReadDictionary(ctx, null);
                    return true;

                case BinaryTypeId.ArrayEnum:
                    res = (T) ReadArray(ctx, typeof(T));
                    return true;

                case BinaryTypeId.Decimal:
                    res = TypeCaster<T>.Cast(BinaryUtils.ReadDecimal(stream));
                    return true;

                case BinaryTypeId.ArrayDecimal:
                    res = TypeCaster<T>.Cast(BinaryUtils.ReadDecimalArray(stream));
                    return true;

                case BinaryTypeId.Timestamp:
                    res = TypeCaster<T>.Cast(BinaryUtils.ReadTimestamp(stream, ctx.Marshaller.TimestampConverter));
                    return true;

                case BinaryTypeId.ArrayTimestamp:
                    res = TypeCaster<T>.Cast(BinaryUtils.ReadTimestampArray(stream, ctx.Marshaller.TimestampConverter));
                    return true;

                case BinaryTypeId.OptimizedMarshaller:
                    res = (T) (object) new OptimizedMarshallerObject(ctx.Stream);
                    return true;
            }

            res = default(T);
            return false;
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
        /// Write Date.
        /// </summary>
        /// <param name="ctx">Context.</param>
        /// <param name="obj">Value.</param>
        private static void WriteTimestamp(BinaryWriter ctx, DateTime obj)
        {
            ctx.Stream.WriteByte(BinaryTypeId.Timestamp);

            BinaryUtils.WriteTimestamp(obj, ctx.Stream, ctx.Marshaller.TimestampConverter);
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
        /// Write nullable Date array.
        /// </summary>
        /// <param name="ctx">Context.</param>
        /// <param name="obj">Value.</param>
        private static void WriteTimestampArray(BinaryWriter ctx, DateTime?[] obj)
        {
            ctx.Stream.WriteByte(BinaryTypeId.ArrayTimestamp);

            BinaryUtils.WriteTimestampArray(obj, ctx.Stream, ctx.Marshaller.TimestampConverter);
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

        /// <summary>
        /// Write Ignite.
        /// </summary>
        private static void WriteIgnite(BinaryWriter ctx, object obj)
        {
            ctx.Stream.WriteByte(BinaryUtils.HdrNull);
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
    internal sealed class BinarySystemWriteHandler<T1> : IBinarySystemWriteHandler
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
