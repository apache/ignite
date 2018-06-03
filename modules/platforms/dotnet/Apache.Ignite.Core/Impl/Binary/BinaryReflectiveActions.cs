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
    using System.Linq;
    using System.Linq.Expressions;
    using System.Reflection;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Cache.Configuration;
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Impl.Common;

    /// <summary>
    /// Write action delegate.
    /// </summary>
    /// <param name="obj">Target object.</param>
    /// <param name="writer">Writer.</param>
    internal delegate void BinaryReflectiveWriteAction(object obj, IBinaryWriter writer);

    /// <summary>
    /// Read action delegate.
    /// </summary>
    /// <param name="obj">Target object.</param>
    /// <param name="reader">Reader.</param>
    internal delegate void BinaryReflectiveReadAction(object obj, IBinaryReader reader);

    /// <summary>
    /// Routines for reflective reads and writes.
    /// </summary>
    internal static class BinaryReflectiveActions
    {
        /** Method: read enum. */
        private static readonly MethodInfo MthdReadEnum =
            typeof(IBinaryReader).GetMethod("ReadEnum", new[] { typeof(string) });

        /** Method: read enum. */
        private static readonly MethodInfo MthdReadEnumRaw = typeof (IBinaryRawReader).GetMethod("ReadEnum");

        /** Method: read enum array. */
        private static readonly MethodInfo MthdReadEnumArray =
            typeof(IBinaryReader).GetMethod("ReadEnumArray", new[] { typeof(string) });

        /** Method: read enum array. */
        private static readonly MethodInfo MthdReadEnumArrayRaw = typeof(IBinaryRawReader).GetMethod("ReadEnumArray");

        /** Method: read array. */
        private static readonly MethodInfo MthdReadObjArray =
            typeof(IBinaryReader).GetMethod("ReadArray", new[] { typeof(string) });

        /** Method: read array. */
        private static readonly MethodInfo MthdReadObjArrayRaw = typeof(IBinaryRawReader).GetMethod("ReadArray");

        /** Method: read object. */
        private static readonly MethodInfo MthdReadObj=
            typeof(IBinaryReader).GetMethod("ReadObject", new[] { typeof(string) });

        /** Method: read object. */
        private static readonly MethodInfo MthdReadObjRaw = typeof(IBinaryRawReader).GetMethod("ReadObject");

        /** Method: write enum array. */
        private static readonly MethodInfo MthdWriteEnumArray = typeof(IBinaryWriter).GetMethod("WriteEnumArray");

        /** Method: write enum array. */
        private static readonly MethodInfo MthdWriteEnumArrayRaw = typeof(IBinaryRawWriter).GetMethod("WriteEnumArray");

        /** Method: write array. */
        private static readonly MethodInfo MthdWriteObjArray = typeof(IBinaryWriter).GetMethod("WriteArray");

        /** Method: write array. */
        private static readonly MethodInfo MthdWriteObjArrayRaw = typeof(IBinaryRawWriter).GetMethod("WriteArray");

        /** Method: write object. */
        private static readonly MethodInfo MthdWriteObj = typeof(IBinaryWriter).GetMethod("WriteObject");

        /** Method: write object. */
        private static readonly MethodInfo MthdWriteObjRaw = typeof(IBinaryRawWriter).GetMethod("WriteObject");

        /** Method: raw writer */
        private static readonly MethodInfo MthdGetRawWriter = typeof(IBinaryWriter).GetMethod("GetRawWriter");

        /** Method: raw writer */
        private static readonly MethodInfo MthdGetRawReader = typeof(IBinaryReader).GetMethod("GetRawReader");

        /// <summary>
        /// Lookup read/write actions for the given type.
        /// </summary>
        /// <param name="field">The field.</param>
        /// <param name="writeAction">Write action.</param>
        /// <param name="readAction">Read action.</param>
        /// <param name="raw">Raw mode.</param>
        /// <param name="forceTimestamp">Force timestamp serialization for DateTime fields..</param>
        public static void GetTypeActions(FieldInfo field, out BinaryReflectiveWriteAction writeAction,
            out BinaryReflectiveReadAction readAction, bool raw, bool forceTimestamp)
        {
            Debug.Assert(field != null);
            Debug.Assert(field.DeclaringType != null);

            var type = field.FieldType;

            forceTimestamp = forceTimestamp ||
                             field.DeclaringType.GetCustomAttributes(typeof(TimestampAttribute), true).Any();

            if (type.IsPrimitive)
                HandlePrimitive(field, out writeAction, out readAction, raw);
            else if (type.IsArray)
            {
                HandleArray(field, out writeAction, out readAction, raw);
            }
            else
                HandleOther(field, out writeAction, out readAction, raw, forceTimestamp);
        }

        /// <summary>
        /// Handle primitive type.
        /// </summary>
        /// <param name="field">The field.</param>
        /// <param name="writeAction">Write action.</param>
        /// <param name="readAction">Read action.</param>
        /// <param name="raw">Raw mode.</param>
        /// <exception cref="IgniteException">Unsupported primitive type:  + type.Name</exception>
        private static void HandlePrimitive(FieldInfo field, out BinaryReflectiveWriteAction writeAction,
            out BinaryReflectiveReadAction readAction, bool raw)
        {
            var type = field.FieldType;

            if (type == typeof (bool))
            {
                writeAction = raw
                    ? GetRawWriter<bool>(field, (w, o) => w.WriteBoolean(o))
                    : GetWriter<bool>(field, (f, w, o) => w.WriteBoolean(f, o));
                readAction = raw
                    ? GetRawReader(field, r => r.ReadBoolean())
                    : GetReader(field, (f, r) => r.ReadBoolean(f));
            }
            else if (type == typeof (sbyte))
            {
                writeAction = raw
                    ? GetRawWriter<sbyte>(field, (w, o) => w.WriteByte(unchecked((byte) o)))
                    : GetWriter<sbyte>(field, (f, w, o) => w.WriteByte(f, unchecked((byte) o)));
                readAction = raw
                    ? GetRawReader(field, r => unchecked ((sbyte) r.ReadByte()))
                    : GetReader(field, (f, r) => unchecked ((sbyte) r.ReadByte(f)));
            }
            else if (type == typeof (byte))
            {
                writeAction = raw
                    ? GetRawWriter<byte>(field, (w, o) => w.WriteByte(o))
                    : GetWriter<byte>(field, (f, w, o) => w.WriteByte(f, o));
                readAction = raw ? GetRawReader(field, r => r.ReadByte()) : GetReader(field, (f, r) => r.ReadByte(f));
            }
            else if (type == typeof (short))
            {
                writeAction = raw
                    ? GetRawWriter<short>(field, (w, o) => w.WriteShort(o))
                    : GetWriter<short>(field, (f, w, o) => w.WriteShort(f, o));
                readAction = raw ? GetRawReader(field, r => r.ReadShort()) : GetReader(field, (f, r) => r.ReadShort(f));
            }
            else if (type == typeof (ushort))
            {
                writeAction = raw
                    ? GetRawWriter<ushort>(field, (w, o) => w.WriteShort(unchecked((short) o)))
                    : GetWriter<ushort>(field, (f, w, o) => w.WriteShort(f, unchecked((short) o)));
                readAction = raw
                    ? GetRawReader(field, r => unchecked((ushort) r.ReadShort()))
                    : GetReader(field, (f, r) => unchecked((ushort) r.ReadShort(f)));
            }
            else if (type == typeof (char))
            {
                writeAction = raw
                    ? GetRawWriter<char>(field, (w, o) => w.WriteChar(o))
                    : GetWriter<char>(field, (f, w, o) => w.WriteChar(f, o));
                readAction = raw ? GetRawReader(field, r => r.ReadChar()) : GetReader(field, (f, r) => r.ReadChar(f));
            }
            else if (type == typeof (int))
            {
                writeAction = raw
                    ? GetRawWriter<int>(field, (w, o) => w.WriteInt(o))
                    : GetWriter<int>(field, (f, w, o) => w.WriteInt(f, o));
                readAction = raw ? GetRawReader(field, r => r.ReadInt()) : GetReader(field, (f, r) => r.ReadInt(f));
            }
            else if (type == typeof (uint))
            {
                writeAction = raw
                    ? GetRawWriter<uint>(field, (w, o) => w.WriteInt(unchecked((int) o)))
                    : GetWriter<uint>(field, (f, w, o) => w.WriteInt(f, unchecked((int) o)));
                readAction = raw
                    ? GetRawReader(field, r => unchecked((uint) r.ReadInt()))
                    : GetReader(field, (f, r) => unchecked((uint) r.ReadInt(f)));
            }
            else if (type == typeof (long))
            {
                writeAction = raw
                    ? GetRawWriter<long>(field, (w, o) => w.WriteLong(o))
                    : GetWriter<long>(field, (f, w, o) => w.WriteLong(f, o));
                readAction = raw ? GetRawReader(field, r => r.ReadLong()) : GetReader(field, (f, r) => r.ReadLong(f));
            }
            else if (type == typeof (ulong))
            {
                writeAction = raw
                    ? GetRawWriter<ulong>(field, (w, o) => w.WriteLong(unchecked((long) o)))
                    : GetWriter<ulong>(field, (f, w, o) => w.WriteLong(f, unchecked((long) o)));
                readAction = raw
                    ? GetRawReader(field, r => unchecked((ulong) r.ReadLong()))
                    : GetReader(field, (f, r) => unchecked((ulong) r.ReadLong(f)));
            }
            else if (type == typeof (float))
            {
                writeAction = raw
                    ? GetRawWriter<float>(field, (w, o) => w.WriteFloat(o))
                    : GetWriter<float>(field, (f, w, o) => w.WriteFloat(f, o));
                readAction = raw ? GetRawReader(field, r => r.ReadFloat()) : GetReader(field, (f, r) => r.ReadFloat(f));
            }
            else if (type == typeof(double))
            {
                writeAction = raw
                    ? GetRawWriter<double>(field, (w, o) => w.WriteDouble(o))
                    : GetWriter<double>(field, (f, w, o) => w.WriteDouble(f, o));
                readAction = raw
                    ? GetRawReader(field, r => r.ReadDouble())
                    : GetReader(field, (f, r) => r.ReadDouble(f));
            }
            else if (type == typeof(IntPtr))
            {
                writeAction = raw
                    ? GetRawWriter<IntPtr>(field, (w, o) => w.WriteLong((long) o))
                    : GetWriter<IntPtr>(field, (f, w, o) => w.WriteLong(f, (long) o));
                readAction = raw
                    ? GetRawReader(field, r => (IntPtr) r.ReadLong())
                    : GetReader(field, (f, r) => (IntPtr) r.ReadLong(f));
            }
            else if (type == typeof(UIntPtr))
            {
                writeAction = raw
                    ? GetRawWriter<UIntPtr>(field, (w, o) => w.WriteLong((long) o))
                    : GetWriter<UIntPtr>(field, (f, w, o) => w.WriteLong(f, (long) o));
                readAction = raw
                    ? GetRawReader(field, r => (UIntPtr) r.ReadLong())
                    : GetReader(field, (f, r) => (UIntPtr) r.ReadLong(f));
            }
            else
            {
                throw new IgniteException(string.Format("Unsupported primitive type '{0}' [Field={1}, " +
                                                        "DeclaringType={2}", type, field, field.DeclaringType));
            }
        }

        /// <summary>
        /// Handle array type.
        /// </summary>
        /// <param name="field">The field.</param>
        /// <param name="writeAction">Write action.</param>
        /// <param name="readAction">Read action.</param>
        /// <param name="raw">Raw mode.</param>
        private static void HandleArray(FieldInfo field, out BinaryReflectiveWriteAction writeAction,
            out BinaryReflectiveReadAction readAction, bool raw)
        {
            if (field.FieldType.GetArrayRank() > 1)
            {
                writeAction = raw
                    ? GetRawWriter<Array>(field, (w, o) => w.WriteObject(
                        o == null ? null : new MultidimensionalArrayHolder(o)))
                    : GetWriter<Array>(field, (f, w, o) => w.WriteObject(f,
                        o == null ? null : new MultidimensionalArrayHolder(o)));
                readAction = raw
                    ? GetRawReader(field, r => r.ReadObject<object>())
                    : GetReader(field, (f, r) => r.ReadObject<object>(f));

                return;
            }

            var elemType = field.FieldType.GetElementType();
            Debug.Assert(elemType != null);

            if (elemType == typeof (bool))
            {
                writeAction = raw
                    ? GetRawWriter<bool[]>(field, (w, o) => w.WriteBooleanArray(o))
                    : GetWriter<bool[]>(field, (f, w, o) => w.WriteBooleanArray(f, o));
                readAction = raw
                    ? GetRawReader(field, r => r.ReadBooleanArray())
                    : GetReader(field, (f, r) => r.ReadBooleanArray(f));
            }
            else if (elemType == typeof (byte))
            {
                writeAction = raw
                    ? GetRawWriter<byte[]>(field, (w, o) => w.WriteByteArray(o))
                    : GetWriter<byte[]>(field, (f, w, o) => w.WriteByteArray(f, o));
                readAction = raw
                    ? GetRawReader(field, r => r.ReadByteArray())
                    : GetReader(field, (f, r) => r.ReadByteArray(f));
            }
            else if (elemType == typeof (sbyte))
            {
                writeAction = raw
                    ? GetRawWriter<sbyte[]>(field, (w, o) => w.WriteByteArray((byte[]) (Array) o))
                    : GetWriter<sbyte[]>(field, (f, w, o) => w.WriteByteArray(f, (byte[]) (Array) o));
                readAction = raw
                    ? GetRawReader(field, r => (sbyte[]) (Array) r.ReadByteArray())
                    : GetReader(field, (f, r) => (sbyte[]) (Array) r.ReadByteArray(f));
            }
            else if (elemType == typeof (short))
            {
                writeAction = raw
                    ? GetRawWriter<short[]>(field, (w, o) => w.WriteShortArray(o))
                    : GetWriter<short[]>(field, (f, w, o) => w.WriteShortArray(f, o));
                readAction = raw
                    ? GetRawReader(field, r => r.ReadShortArray())
                    : GetReader(field, (f, r) => r.ReadShortArray(f));
            }
            else if (elemType == typeof (ushort))
            {
                writeAction = raw
                    ? GetRawWriter<ushort[]>(field, (w, o) => w.WriteShortArray((short[]) (Array) o))
                    : GetWriter<ushort[]>(field, (f, w, o) => w.WriteShortArray(f, (short[]) (Array) o));
                readAction = raw
                    ? GetRawReader(field, r => (ushort[]) (Array) r.ReadShortArray())
                    : GetReader(field, (f, r) => (ushort[]) (Array) r.ReadShortArray(f));
            }
            else if (elemType == typeof (char))
            {
                writeAction = raw
                    ? GetRawWriter<char[]>(field, (w, o) => w.WriteCharArray(o))
                    : GetWriter<char[]>(field, (f, w, o) => w.WriteCharArray(f, o));
                readAction = raw
                    ? GetRawReader(field, r => r.ReadCharArray())
                    : GetReader(field, (f, r) => r.ReadCharArray(f));
            }
            else if (elemType == typeof (int))
            {
                writeAction = raw
                    ? GetRawWriter<int[]>(field, (w, o) => w.WriteIntArray(o))
                    : GetWriter<int[]>(field, (f, w, o) => w.WriteIntArray(f, o));
                readAction = raw
                    ? GetRawReader(field, r => r.ReadIntArray())
                    : GetReader(field, (f, r) => r.ReadIntArray(f));
            }
            else if (elemType == typeof (uint))
            {
                writeAction = raw
                    ? GetRawWriter<uint[]>(field, (w, o) => w.WriteIntArray((int[]) (Array) o))
                    : GetWriter<uint[]>(field, (f, w, o) => w.WriteIntArray(f, (int[]) (Array) o));
                readAction = raw
                    ? GetRawReader(field, r => (uint[]) (Array) r.ReadIntArray())
                    : GetReader(field, (f, r) => (uint[]) (Array) r.ReadIntArray(f));
            }
            else if (elemType == typeof (long))
            {
                writeAction = raw
                    ? GetRawWriter<long[]>(field, (w, o) => w.WriteLongArray(o))
                    : GetWriter<long[]>(field, (f, w, o) => w.WriteLongArray(f, o));
                readAction = raw
                    ? GetRawReader(field, r => r.ReadLongArray())
                    : GetReader(field, (f, r) => r.ReadLongArray(f));
            }
            else if (elemType == typeof (ulong))
            {
                writeAction = raw
                    ? GetRawWriter<ulong[]>(field, (w, o) => w.WriteLongArray((long[]) (Array) o))
                    : GetWriter<ulong[]>(field, (f, w, o) => w.WriteLongArray(f, (long[]) (Array) o));
                readAction = raw
                    ? GetRawReader(field, r => (ulong[]) (Array) r.ReadLongArray())
                    : GetReader(field, (f, r) => (ulong[]) (Array) r.ReadLongArray(f));
            }
            else if (elemType == typeof (float))
            {
                writeAction = raw
                    ? GetRawWriter<float[]>(field, (w, o) => w.WriteFloatArray(o))
                    : GetWriter<float[]>(field, (f, w, o) => w.WriteFloatArray(f, o));
                readAction = raw
                    ? GetRawReader(field, r => r.ReadFloatArray())
                    : GetReader(field, (f, r) => r.ReadFloatArray(f));
            }
            else if (elemType == typeof (double))
            {
                writeAction = raw
                    ? GetRawWriter<double[]>(field, (w, o) => w.WriteDoubleArray(o))
                    : GetWriter<double[]>(field, (f, w, o) => w.WriteDoubleArray(f, o));
                readAction = raw
                    ? GetRawReader(field, r => r.ReadDoubleArray())
                    : GetReader(field, (f, r) => r.ReadDoubleArray(f));
            }
            else if (elemType == typeof (decimal?))
            {
                writeAction = raw
                    ? GetRawWriter<decimal?[]>(field, (w, o) => w.WriteDecimalArray(o))
                    : GetWriter<decimal?[]>(field, (f, w, o) => w.WriteDecimalArray(f, o));
                readAction = raw
                    ? GetRawReader(field, r => r.ReadDecimalArray())
                    : GetReader(field, (f, r) => r.ReadDecimalArray(f));
            }
            else if (elemType == typeof (string))
            {
                writeAction = raw
                    ? GetRawWriter<string[]>(field, (w, o) => w.WriteStringArray(o))
                    : GetWriter<string[]>(field, (f, w, o) => w.WriteStringArray(f, o));
                readAction = raw
                    ? GetRawReader(field, r => r.ReadStringArray())
                    : GetReader(field, (f, r) => r.ReadStringArray(f));
            }
            else if (elemType == typeof (Guid?))
            {
                writeAction = raw
                    ? GetRawWriter<Guid?[]>(field, (w, o) => w.WriteGuidArray(o))
                    : GetWriter<Guid?[]>(field, (f, w, o) => w.WriteGuidArray(f, o));
                readAction = raw
                    ? GetRawReader(field, r => r.ReadGuidArray())
                    : GetReader(field, (f, r) => r.ReadGuidArray(f));
            }
            else if (elemType.IsEnum)
            {
                writeAction = raw
                    ? GetRawWriter(field, MthdWriteEnumArrayRaw, elemType)
                    : GetWriter(field, MthdWriteEnumArray, elemType);
                readAction = raw
                    ? GetRawReader(field, MthdReadEnumArrayRaw, elemType)
                    : GetReader(field, MthdReadEnumArray, elemType);
            }
            else
            {
                writeAction = raw
                    ? GetRawWriter(field, MthdWriteObjArrayRaw, elemType)
                    : GetWriter(field, MthdWriteObjArray, elemType);
                readAction = raw
                    ? GetRawReader(field, MthdReadObjArrayRaw, elemType)
                    : GetReader(field, MthdReadObjArray, elemType);
            }
        }

        /// <summary>
        /// Handle other type.
        /// </summary>
        /// <param name="field">The field.</param>
        /// <param name="writeAction">Write action.</param>
        /// <param name="readAction">Read action.</param>
        /// <param name="raw">Raw mode.</param>
        /// <param name="forceTimestamp">Force timestamp serialization for DateTime fields..</param>
        private static void HandleOther(FieldInfo field, out BinaryReflectiveWriteAction writeAction,
            out BinaryReflectiveReadAction readAction, bool raw, bool forceTimestamp)
        {
            var type = field.FieldType;

            var nullableType = Nullable.GetUnderlyingType(type);

            if (type == typeof (decimal))
            {
                writeAction = raw
                    ? GetRawWriter<decimal>(field, (w, o) => w.WriteDecimal(o))
                    : GetWriter<decimal>(field, (f, w, o) => w.WriteDecimal(f, o));
                readAction = raw
                    ? GetRawReader(field, r => r.ReadDecimal())
                    : GetReader(field, (f, r) => r.ReadDecimal(f));
            }
            else if (type == typeof (string))
            {
                writeAction = raw
                    ? GetRawWriter<string>(field, (w, o) => w.WriteString(o))
                    : GetWriter<string>(field, (f, w, o) => w.WriteString(f, o));
                readAction = raw
                    ? GetRawReader(field, r => r.ReadString())
                    : GetReader(field, (f, r) => r.ReadString(f));
            }
            else if (type == typeof (Guid))
            {
                writeAction = raw
                    ? GetRawWriter<Guid>(field, (w, o) => w.WriteGuid(o))
                    : GetWriter<Guid>(field, (f, w, o) => w.WriteGuid(f, o));
                readAction = raw
                    ? GetRawReader(field, r => r.ReadObject<Guid>())
                    : GetReader(field, (f, r) => r.ReadObject<Guid>(f));
            }
            else if (nullableType == typeof (Guid))
            {
                writeAction = raw
                    ? GetRawWriter<Guid?>(field, (w, o) => w.WriteGuid(o))
                    : GetWriter<Guid?>(field, (f, w, o) => w.WriteGuid(f, o));
                readAction = raw ? GetRawReader(field, r => r.ReadGuid()) : GetReader(field, (f, r) => r.ReadGuid(f));
            }
            else if ((nullableType ?? type).IsEnum && 
                !new[] {typeof(long), typeof(ulong)}.Contains(Enum.GetUnderlyingType(nullableType ?? type)))
            {
                writeAction = raw
                    ? GetRawWriter<object>(field, (w, o) => w.WriteEnum(o))
                    : GetWriter<object>(field, (f, w, o) => w.WriteEnum(f, o));
                readAction = raw ? GetRawReader(field, MthdReadEnumRaw) : GetReader(field, MthdReadEnum);
            }
            else if (type == typeof(IDictionary) || type == typeof(Hashtable))
            {
                writeAction = raw
                    ? GetRawWriter<IDictionary>(field, (w, o) => w.WriteDictionary(o))
                    : GetWriter<IDictionary>(field, (f, w, o) => w.WriteDictionary(f, o));
                readAction = raw
                    ? GetRawReader(field, r => r.ReadDictionary())
                    : GetReader(field, (f, r) => r.ReadDictionary(f));
            }
            else if (type == typeof(ICollection) || type == typeof(ArrayList))
            {
                writeAction = raw
                    ? GetRawWriter<ICollection>(field, (w, o) => w.WriteCollection(o))
                    : GetWriter<ICollection>(field, (f, w, o) => w.WriteCollection(f, o));
                readAction = raw
                    ? GetRawReader(field, r => r.ReadCollection())
                    : GetReader(field, (f, r) => r.ReadCollection(f));
            }
            else if (type == typeof(DateTime) && IsTimestamp(field, forceTimestamp, raw))
            {
                writeAction = GetWriter<DateTime>(field, (f, w, o) => w.WriteTimestamp(f, o));
                readAction = GetReader(field, (f, r) => r.ReadObject<DateTime>(f));
            }
            else if (nullableType == typeof(DateTime) && IsTimestamp(field, forceTimestamp, raw))
            {
                writeAction = GetWriter<DateTime?>(field, (f, w, o) => w.WriteTimestamp(f, o));
                readAction = GetReader(field, (f, r) => r.ReadTimestamp(f));
            }
            else if (type.IsPointer)
                unsafe
                {
                    // Expression trees do not work with pointers properly, use reflection.
                    var fieldName = BinaryUtils.CleanFieldName(field.Name);
                    writeAction = raw
                        ? (BinaryReflectiveWriteAction) ((o, w) =>
                            w.GetRawWriter().WriteLong((long) Pointer.Unbox(field.GetValue(o))))
                        : ((o, w) => w.WriteLong(fieldName, (long) Pointer.Unbox(field.GetValue(o))));

                    readAction = raw
                        ? (BinaryReflectiveReadAction) ((o, r) =>
                            field.SetValue(o, Pointer.Box((void*) r.GetRawReader().ReadLong(), field.FieldType)))
                        : ((o, r) => field.SetValue(o, Pointer.Box((void*) r.ReadLong(fieldName), field.FieldType)));
                }
            else
            {
                writeAction = raw ? GetRawWriter(field, MthdWriteObjRaw) : GetWriter(field, MthdWriteObj);
                readAction = raw ? GetRawReader(field, MthdReadObjRaw) : GetReader(field, MthdReadObj);
            }
        }

        /// <summary>
        /// Determines whether specified field should be written as timestamp.
        /// </summary>
        private static bool IsTimestamp(FieldInfo field, bool forceTimestamp, bool raw)
        {
            if (forceTimestamp)
            {
                return true;
            }

            Debug.Assert(field != null && field.DeclaringType != null);

            var fieldName = BinaryUtils.CleanFieldName(field.Name);

            object[] attrs = null;

            if (fieldName != field.Name)
            {
                // Backing field, check corresponding property
                var prop = field.DeclaringType.GetProperty(fieldName, field.FieldType);

                if (prop != null)
                    attrs = prop.GetCustomAttributes(true);
            }

            attrs = attrs ?? field.GetCustomAttributes(true);

            if (attrs.Any(x => x is TimestampAttribute))
            {
                return true;
            }

            // Special case for DateTime and query fields.
            // If a field is marked with [QuerySqlField], write it as TimeStamp so that queries work.
            // This is not needed in raw mode (queries do not work anyway).
            // It may cause issues when field has attribute, but is used in a cache without queries, and user
            // may expect non-UTC dates to work. However, such cases are rare, and there are workarounds.
            return !raw && attrs.Any(x => x is QuerySqlFieldAttribute);
        }

        /// <summary>
        /// Gets the reader with a specified write action.
        /// </summary>
        private static BinaryReflectiveWriteAction GetWriter<T>(FieldInfo field,
            Expression<Action<string, IBinaryWriter, T>> write)
        {
            Debug.Assert(field != null);
            Debug.Assert(field.DeclaringType != null);   // non-static
            Debug.Assert(write != null);

            // Get field value
            var targetParam = Expression.Parameter(typeof(object));
            var targetParamConverted = Expression.Convert(targetParam, field.DeclaringType);
            Expression fldExpr = Expression.Field(targetParamConverted, field);

            if (field.FieldType != typeof(T))
            {
                fldExpr = Expression.Convert(fldExpr, typeof(T));
            }

            // Call Writer method
            var writerParam = Expression.Parameter(typeof(IBinaryWriter));
            var fldNameParam = Expression.Constant(BinaryUtils.CleanFieldName(field.Name));
            var writeExpr = Expression.Invoke(write, fldNameParam, writerParam, fldExpr);

            // Compile and return
            return Expression.Lambda<BinaryReflectiveWriteAction>(writeExpr, targetParam, writerParam).Compile();
        }

        /// <summary>
        /// Gets the reader with a specified write action.
        /// </summary>
        private static BinaryReflectiveWriteAction GetRawWriter<T>(FieldInfo field,
            Expression<Action<IBinaryRawWriter, T>> write)
        {
            Debug.Assert(field != null);
            Debug.Assert(field.DeclaringType != null);   // non-static
            Debug.Assert(write != null);

            // Get field value
            var targetParam = Expression.Parameter(typeof(object));
            var targetParamConverted = Expression.Convert(targetParam, field.DeclaringType);
            Expression fldExpr = Expression.Field(targetParamConverted, field);

            if (field.FieldType != typeof(T))
                fldExpr = Expression.Convert(fldExpr, typeof (T));

            // Call Writer method
            var writerParam = Expression.Parameter(typeof(IBinaryWriter));
            var writeExpr = Expression.Invoke(write, Expression.Call(writerParam, MthdGetRawWriter), fldExpr);

            // Compile and return
            return Expression.Lambda<BinaryReflectiveWriteAction>(writeExpr, targetParam, writerParam).Compile();
        }

        /// <summary>
        /// Gets the writer with a specified generic method.
        /// </summary>
        private static BinaryReflectiveWriteAction GetWriter(FieldInfo field, MethodInfo method,
            params Type[] genericArgs)
        {
            return GetWriter0(field, method, false, genericArgs);
        }

        /// <summary>
        /// Gets the writer with a specified generic method.
        /// </summary>
        private static BinaryReflectiveWriteAction GetRawWriter(FieldInfo field, MethodInfo method,
            params Type[] genericArgs)
        {
            return GetWriter0(field, method, true, genericArgs);
        }

        /// <summary>
        /// Gets the writer with a specified generic method.
        /// </summary>
        private static BinaryReflectiveWriteAction GetWriter0(FieldInfo field, MethodInfo method, bool raw, 
            params Type[] genericArgs)
        {
            Debug.Assert(field != null);
            Debug.Assert(field.DeclaringType != null);   // non-static
            Debug.Assert(method != null);

            if (genericArgs.Length == 0)
                genericArgs = new[] {field.FieldType};

            // Get field value
            var targetParam = Expression.Parameter(typeof(object));
            var targetParamConverted = Expression.Convert(targetParam, field.DeclaringType);
            var fldExpr = Expression.Field(targetParamConverted, field);

            // Call Writer method
            var writerParam = Expression.Parameter(typeof (IBinaryWriter));

            var writeMethod = method.MakeGenericMethod(genericArgs);

            var writeExpr = raw
                ? Expression.Call(Expression.Call(writerParam, MthdGetRawWriter), writeMethod, fldExpr)
                : Expression.Call(writerParam, writeMethod, Expression.Constant(BinaryUtils.CleanFieldName(field.Name)),
                    fldExpr);

            // Compile and return
            return Expression.Lambda<BinaryReflectiveWriteAction>(writeExpr, targetParam, writerParam).Compile();
        }

        /// <summary>
        /// Gets the reader with a specified read action.
        /// </summary>
        private static BinaryReflectiveReadAction GetReader<T>(FieldInfo field, 
            Expression<Func<string, IBinaryReader, T>> read)
        {
            Debug.Assert(field != null);
            Debug.Assert(field.DeclaringType != null);   // non-static

            // Call Reader method
            var readerParam = Expression.Parameter(typeof(IBinaryReader));
            var fldNameParam = Expression.Constant(BinaryUtils.CleanFieldName(field.Name));
            Expression readExpr = Expression.Invoke(read, fldNameParam, readerParam);

            if (typeof(T) != field.FieldType)
            {
                readExpr = Expression.Convert(readExpr, field.FieldType);
            }

            // Assign field value
            var targetParam = Expression.Parameter(typeof(object));
            var assignExpr = Expression.Call(DelegateConverter.GetWriteFieldMethod(field), targetParam, readExpr);

            // Compile and return
            return Expression.Lambda<BinaryReflectiveReadAction>(assignExpr, targetParam, readerParam).Compile();
        }

        /// <summary>
        /// Gets the reader with a specified read action.
        /// </summary>
        private static BinaryReflectiveReadAction GetRawReader<T>(FieldInfo field, 
            Expression<Func<IBinaryRawReader, T>> read)
        {
            Debug.Assert(field != null);
            Debug.Assert(field.DeclaringType != null);   // non-static

            // Call Reader method
            var readerParam = Expression.Parameter(typeof(IBinaryReader));
            Expression readExpr = Expression.Invoke(read, Expression.Call(readerParam, MthdGetRawReader));

            if (typeof(T) != field.FieldType)
                readExpr = Expression.Convert(readExpr, field.FieldType);

            // Assign field value
            var targetParam = Expression.Parameter(typeof(object));
            var assignExpr = Expression.Call(DelegateConverter.GetWriteFieldMethod(field), targetParam, readExpr);

            // Compile and return
            return Expression.Lambda<BinaryReflectiveReadAction>(assignExpr, targetParam, readerParam).Compile();
        }

        /// <summary>
        /// Gets the reader with a specified generic method.
        /// </summary>
        private static BinaryReflectiveReadAction GetReader(FieldInfo field, MethodInfo method,
            params Type[] genericArgs)
        {
            return GetReader0(field, method, false, genericArgs);
        }

        /// <summary>
        /// Gets the reader with a specified generic method.
        /// </summary>
        private static BinaryReflectiveReadAction GetRawReader(FieldInfo field, MethodInfo method,
            params Type[] genericArgs)
        {
            return GetReader0(field, method, true, genericArgs);
        }

        /// <summary>
        /// Gets the reader with a specified generic method.
        /// </summary>
        private static BinaryReflectiveReadAction GetReader0(FieldInfo field, MethodInfo method, bool raw, 
            params Type[] genericArgs)
        {
            Debug.Assert(field != null);
            Debug.Assert(field.DeclaringType != null);   // non-static

            if (genericArgs.Length == 0)
                genericArgs = new[] {field.FieldType};

            // Call Reader method
            var readerParam = Expression.Parameter(typeof (IBinaryReader));
            var readMethod = method.MakeGenericMethod(genericArgs);
            Expression readExpr = raw
                ? Expression.Call(Expression.Call(readerParam, MthdGetRawReader), readMethod)
                : Expression.Call(readerParam, readMethod, Expression.Constant(BinaryUtils.CleanFieldName(field.Name)));

            if (readMethod.ReturnType != field.FieldType)
                readExpr = Expression.Convert(readExpr, field.FieldType);

            // Assign field value
            var targetParam = Expression.Parameter(typeof(object));
            var targetParamConverted = Expression.Convert(targetParam, typeof(object));
            var assignExpr = Expression.Call(DelegateConverter.GetWriteFieldMethod(field), targetParamConverted, 
                readExpr);

            // Compile and return
            return Expression.Lambda<BinaryReflectiveReadAction>(assignExpr, targetParam, readerParam).Compile();
        }
    }
}
