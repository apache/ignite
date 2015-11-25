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
    using System.Linq.Expressions;
    using System.Reflection;
    using Apache.Ignite.Core.Binary;
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

        /** Method: read enum array. */
        private static readonly MethodInfo MthdReadEnumArray =
            typeof(IBinaryReader).GetMethod("ReadEnumArray", new[] { typeof(string) });

        /** Method: read array. */
        private static readonly MethodInfo MthdReadObjArray =
            typeof(IBinaryReader).GetMethod("ReadArray", new[] { typeof(string) });

        /** Method: read object. */
        private static readonly MethodInfo MthdReadObj=
            typeof(IBinaryReader).GetMethod("ReadObject", new[] { typeof(string) });

        /** Method: write enum array. */
        private static readonly MethodInfo MthdWriteEnumArray =
            typeof(IBinaryWriter).GetMethod("WriteEnumArray");

        /** Method: write array. */
        private static readonly MethodInfo MthdWriteObjArray =
            typeof(IBinaryWriter).GetMethod("WriteArray");

        /** Method: read object. */
        private static readonly MethodInfo MthdWriteObj =
            typeof(IBinaryWriter).GetMethod("WriteObject");

        /// <summary>
        /// Lookup read/write actions for the given type.
        /// </summary>
        /// <param name="field">The field.</param>
        /// <param name="writeAction">Write action.</param>
        /// <param name="readAction">Read action.</param>
        public static void TypeActions(FieldInfo field, out BinaryReflectiveWriteAction writeAction, 
            out BinaryReflectiveReadAction readAction)
        {
            var type = field.FieldType;

            if (type.IsPrimitive)
                HandlePrimitive(field, out writeAction, out readAction);
            else if (type.IsArray)
                HandleArray(field, out writeAction, out readAction);
            else
                HandleOther(field, out writeAction, out readAction);
        }

        /// <summary>
        /// Handle primitive type.
        /// </summary>
        /// <param name="field">The field.</param>
        /// <param name="writeAction">Write action.</param>
        /// <param name="readAction">Read action.</param>
        /// <exception cref="IgniteException">Unsupported primitive type:  + type.Name</exception>
        private static void HandlePrimitive(FieldInfo field, out BinaryReflectiveWriteAction writeAction,
            out BinaryReflectiveReadAction readAction)
        {
            var type = field.FieldType;

            if (type == typeof(bool))
            {
                writeAction = GetWriter<bool>(field, (f, w, o) => w.WriteBoolean(f, o));
                readAction = GetReader(field, (f, r) => r.ReadBoolean(f));
            }
            else if (type == typeof(sbyte))
            {
                writeAction = GetWriter<sbyte>(field, (f, w, o) => w.WriteByte(f, unchecked((byte) o)));
                readAction = GetReader(field, (f, r) => unchecked ((sbyte)r.ReadByte(f)));
            }
            else if (type == typeof(byte))
            {
                writeAction = GetWriter<byte>(field, (f, w, o) => w.WriteByte(f, o));
                readAction = GetReader(field, (f, r) => r.ReadByte(f));
            }
            else if (type == typeof(short))
            {
                writeAction = GetWriter<short>(field, (f, w, o) => w.WriteShort(f, o));
                readAction = GetReader(field, (f, r) => r.ReadShort(f));
            }
            else if (type == typeof(ushort))
            {
                writeAction = GetWriter<ushort>(field, (f, w, o) => w.WriteShort(f, unchecked((short) o)));
                readAction = GetReader(field, (f, r) => unchecked((ushort) r.ReadShort(f)));
            }
            else if (type == typeof(char))
            {
                writeAction = GetWriter<char>(field, (f, w, o) => w.WriteChar(f, o));
                readAction = GetReader(field, (f, r) => r.ReadChar(f));
            }
            else if (type == typeof(int))
            {
                writeAction = GetWriter<int>(field, (f, w, o) => w.WriteInt(f, o));
                readAction = GetReader(field, (f, r) => r.ReadInt(f));
            }
            else if (type == typeof(uint))
            {
                writeAction = GetWriter<uint>(field, (f, w, o) => w.WriteInt(f, unchecked((int) o)));
                readAction = GetReader(field, (f, r) => unchecked((uint) r.ReadInt(f)));
            }
            else if (type == typeof(long))
            {
                writeAction = GetWriter<long>(field, (f, w, o) => w.WriteLong(f, o));
                readAction = GetReader(field, (f, r) => r.ReadLong(f));
            }
            else if (type == typeof(ulong))
            {
                writeAction = GetWriter<ulong>(field, (f, w, o) => w.WriteLong(f, unchecked((long) o)));
                readAction = GetReader(field, (f, r) => unchecked((ulong) r.ReadLong(f)));
            }
            else if (type == typeof(float))
            {
                writeAction = GetWriter<float>(field, (f, w, o) => w.WriteFloat(f, o));
                readAction = GetReader(field, (f, r) => r.ReadFloat(f));
            }
            else if (type == typeof(double))
            {
                writeAction = GetWriter<double>(field, (f, w, o) => w.WriteDouble(f, o));
                readAction = GetReader(field, (f, r) => r.ReadDouble(f));
            }
            else
                throw new IgniteException("Unsupported primitive type: " + type.Name);
        }

        /// <summary>
        /// Handle array type.
        /// </summary>
        /// <param name="field">The field.</param>
        /// <param name="writeAction">Write action.</param>
        /// <param name="readAction">Read action.</param>
        private static void HandleArray(FieldInfo field, out BinaryReflectiveWriteAction writeAction,
            out BinaryReflectiveReadAction readAction)
        {
            Type elemType = field.FieldType.GetElementType();

            if (elemType == typeof(bool))
            {
                writeAction = GetWriter<bool[]>(field, (f, w, o) => w.WriteBooleanArray(f, o));
                readAction = GetReader(field, (f, r) => r.ReadBooleanArray(f));
            }
            else if (elemType == typeof(byte))
            {
                writeAction = GetWriter<byte[]>(field, (f, w, o) => w.WriteByteArray(f, o));
                readAction = GetReader(field, (f, r) => r.ReadByteArray(f));
            }
            else if (elemType == typeof(sbyte))
            {
                writeAction = GetWriter<sbyte[]>(field, (f, w, o) => w.WriteByteArray(f, (byte[]) (Array) o));
                readAction = GetReader(field, (f, r) => (sbyte[]) (Array) r.ReadByteArray(f));
            }
            else if (elemType == typeof(short))
            {
                writeAction = GetWriter<short[]>(field, (f, w, o) => w.WriteShortArray(f, o));
                readAction = GetReader(field, (f, r) => r.ReadShortArray(f));
            }
            else if (elemType == typeof(ushort))
            {
                writeAction = GetWriter<ushort[]>(field, (f, w, o) => w.WriteShortArray(f, (short[]) (Array) o));
                readAction = GetReader(field, (f, r) => (ushort[]) (Array) r.ReadShortArray(f));
            }
            else if (elemType == typeof(char))
            {
                writeAction = GetWriter<char[]>(field, (f, w, o) => w.WriteCharArray(f, o));
                readAction = GetReader(field, (f, r) => r.ReadCharArray(f));
            }
            else if (elemType == typeof(int))
            {
                writeAction = GetWriter<int[]>(field, (f, w, o) => w.WriteIntArray(f, o));
                readAction = GetReader(field, (f, r) => r.ReadIntArray(f));
            }
            else if (elemType == typeof(uint))
            {
                writeAction = GetWriter<uint[]>(field, (f, w, o) => w.WriteIntArray(f, (int[]) (Array) o));
                readAction = GetReader(field, (f, r) => (uint[]) (Array) r.ReadIntArray(f));
            }
            else if (elemType == typeof(long))
            {
                writeAction = GetWriter<long[]>(field, (f, w, o) => w.WriteLongArray(f, o));
                readAction = GetReader(field, (f, r) => r.ReadLongArray(f));
            }
            else if (elemType == typeof(ulong))
            {
                writeAction = GetWriter<ulong[]>(field, (f, w, o) => w.WriteLongArray(f, (long[]) (Array) o));
                readAction = GetReader(field, (f, r) => (ulong[]) (Array) r.ReadLongArray(f));
            }
            else if (elemType == typeof(float))
            {
                writeAction = GetWriter<float[]>(field, (f, w, o) => w.WriteFloatArray(f, o));
                readAction = GetReader(field, (f, r) => r.ReadFloatArray(f));
            }
            else if (elemType == typeof(double))
            {
                writeAction = GetWriter<double[]>(field, (f, w, o) => w.WriteDoubleArray(f, o));
                readAction = GetReader(field, (f, r) => r.ReadDoubleArray(f));
            }
            else if (elemType == typeof(decimal?))
            {
                writeAction = GetWriter<decimal?[]>(field, (f, w, o) => w.WriteDecimalArray(f, o));
                readAction = GetReader(field, (f, r) => r.ReadDecimalArray(f));
            }
            else if (elemType == typeof(string))
            {
                writeAction = GetWriter<string[]>(field, (f, w, o) => w.WriteStringArray(f, o));
                readAction = GetReader(field, (f, r) => r.ReadStringArray(f));
            }
            else if (elemType == typeof(Guid?))
            {
                writeAction = GetWriter<Guid?[]>(field, (f, w, o) => w.WriteGuidArray(f, o));
                readAction = GetReader(field, (f, r) => r.ReadGuidArray(f));
            } 
            else if (elemType.IsEnum)
            {
                writeAction = GetWriter(field, MthdWriteEnumArray, elemType);
                readAction = GetReader(field, MthdReadEnumArray, elemType);
            }
            else
            {
                writeAction = GetWriter(field, MthdWriteObjArray, elemType);
                readAction = GetReader(field, MthdReadObjArray, elemType);
            }  
        }

        /// <summary>
        /// Handle other type.
        /// </summary>
        /// <param name="field">The field.</param>
        /// <param name="writeAction">Write action.</param>
        /// <param name="readAction">Read action.</param>
        private static void HandleOther(FieldInfo field, out BinaryReflectiveWriteAction writeAction,
            out BinaryReflectiveReadAction readAction)
        {
            var type = field.FieldType;

            var genericDef = type.IsGenericType ? type.GetGenericTypeDefinition() : null;

            bool nullable = genericDef == typeof(Nullable<>);

            var nullableType = nullable ? type.GetGenericArguments()[0] : null;

            if (type == typeof(decimal))
            {
                writeAction = GetWriter<decimal>(field, (f, w, o) => w.WriteDecimal(f, o));
                readAction = GetReader(field, (f, r) => r.ReadDecimal(f));
            }
            else if (type == typeof(string))
            {
                writeAction = GetWriter<string>(field, (f, w, o) => w.WriteString(f, o));
                readAction = GetReader(field, (f, r) => r.ReadString(f));
            }
            else if (type == typeof(Guid))
            {
                writeAction = GetWriter<Guid>(field, (f, w, o) => w.WriteGuid(f, o));
                readAction = GetReader(field, (f, r) => r.ReadObject<Guid>(f));
            }
            else if (nullable && nullableType == typeof(Guid))
            {
                writeAction = GetWriter<Guid?>(field, (f, w, o) => w.WriteGuid(f, o));
                readAction = GetReader(field, (f, r) => r.ReadGuid(f));
            } 
            else if (type.IsEnum)
            {
                writeAction = GetWriter<object>(field, (f, w, o) => w.WriteEnum(f, o), true);
                readAction = GetReader(field, MthdReadEnum);
            }
            else if (type == BinaryUtils.TypDictionary || type.GetInterface(BinaryUtils.TypDictionary.FullName) != null && !type.IsGenericType)
            {
                writeAction = GetWriter<IDictionary>(field, (f, w, o) => w.WriteDictionary(f, o));
                readAction = GetReader(field, (f, r) => r.ReadDictionary(f));
            }
            else if (type == BinaryUtils.TypCollection || type.GetInterface(BinaryUtils.TypCollection.FullName) != null && !type.IsGenericType)
            {
                writeAction = GetWriter<ICollection>(field, (f, w, o) => w.WriteCollection(f, o));
                readAction = GetReader(field, (f, r) => r.ReadCollection(f));
            }
            else
            {
                writeAction = GetWriter(field, MthdWriteObj);
                readAction = GetReader(field, MthdReadObj);
            }                
        }

        /// <summary>
        /// Gets the reader with a specified write action.
        /// </summary>
        private static BinaryReflectiveWriteAction GetWriter<T>(FieldInfo field,
            Expression<Action<string, IBinaryWriter, T>> write,
            bool convertFieldValToObject = false)
        {
            Debug.Assert(field != null);
            Debug.Assert(field.DeclaringType != null);   // non-static

            // Get field value
            var targetParam = Expression.Parameter(typeof(object));
            var targetParamConverted = Expression.Convert(targetParam, field.DeclaringType);
            Expression fldExpr = Expression.Field(targetParamConverted, field);

            if (convertFieldValToObject)
                fldExpr = Expression.Convert(fldExpr, typeof (object));

            // Call Writer method
            var writerParam = Expression.Parameter(typeof(IBinaryWriter));
            var fldNameParam = Expression.Constant(BinaryUtils.CleanFieldName(field.Name));
            var writeExpr = Expression.Invoke(write, fldNameParam, writerParam, fldExpr);

            // Compile and return
            return Expression.Lambda<BinaryReflectiveWriteAction>(writeExpr, targetParam, writerParam).Compile();
        }

        /// <summary>
        /// Gets the writer with a specified generic method.
        /// </summary>
        private static BinaryReflectiveWriteAction GetWriter(FieldInfo field, MethodInfo method, 
            params Type[] genericArgs)
        {
            Debug.Assert(field != null);
            Debug.Assert(field.DeclaringType != null);   // non-static

            if (genericArgs.Length == 0)
                genericArgs = new[] {field.FieldType};

            // Get field value
            var targetParam = Expression.Parameter(typeof(object));
            var targetParamConverted = Expression.Convert(targetParam, field.DeclaringType);
            var fldExpr = Expression.Field(targetParamConverted, field);

            // Call Writer method
            var writerParam = Expression.Parameter(typeof(IBinaryWriter));
            var fldNameParam = Expression.Constant(BinaryUtils.CleanFieldName(field.Name));
            var writeMethod = method.MakeGenericMethod(genericArgs);
            var writeExpr = Expression.Call(writerParam, writeMethod, fldNameParam, fldExpr);

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
                readExpr = Expression.Convert(readExpr, field.FieldType);

            // Assign field value
            var targetParam = Expression.Parameter(typeof(object));
            var targetParamConverted = Expression.Convert(targetParam, field.DeclaringType);
            var assignExpr = Expression.Call(DelegateConverter.GetWriteFieldMethod(field), targetParamConverted, 
                readExpr);

            // Compile and return
            return Expression.Lambda<BinaryReflectiveReadAction>(assignExpr, targetParam, readerParam).Compile();
        }

        /// <summary>
        /// Gets the reader with a specified generic method.
        /// </summary>
        private static BinaryReflectiveReadAction GetReader(FieldInfo field, MethodInfo method, 
            params Type[] genericArgs)
        {
            Debug.Assert(field != null);
            Debug.Assert(field.DeclaringType != null);   // non-static

            if (genericArgs.Length == 0)
                genericArgs = new[] {field.FieldType};

            // Call Reader method
            var readerParam = Expression.Parameter(typeof (IBinaryReader));
            var fldNameParam = Expression.Constant(BinaryUtils.CleanFieldName(field.Name));
            var readMethod = method.MakeGenericMethod(genericArgs);
            Expression readExpr = Expression.Call(readerParam, readMethod, fldNameParam);

            if (readMethod.ReturnType != field.FieldType)
                readExpr = Expression.Convert(readExpr, field.FieldType);

            // Assign field value
            var targetParam = Expression.Parameter(typeof(object));
            var targetParamConverted = Expression.Convert(targetParam, field.DeclaringType);
            var assignExpr = Expression.Call(DelegateConverter.GetWriteFieldMethod(field), targetParamConverted, 
                readExpr);

            // Compile and return
            return Expression.Lambda<BinaryReflectiveReadAction>(assignExpr, targetParam, readerParam).Compile();
        }
    }
}
