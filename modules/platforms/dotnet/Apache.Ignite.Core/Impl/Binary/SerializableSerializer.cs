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
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Runtime.Serialization;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Impl.Binary.Metadata;
    using Apache.Ignite.Core.Impl.Common;

    /// <summary>
    /// Serializes classes that implement <see cref="ISerializable"/>.
    /// </summary>
    internal class SerializableSerializer : IBinarySerializerInternal
    {
        /** */
        private readonly SerializableTypeDescriptor _serializableTypeDesc;

        /// <summary>
        /// Initializes a new instance of the <see cref="SerializableSerializer"/> class.
        /// </summary>
        public SerializableSerializer(Type type)
        {
            IgniteArgumentCheck.NotNull(type, "type");

            _serializableTypeDesc = SerializableTypeDescriptor.Get(type);
        }
        
        /** <inheritdoc /> */
        public bool SupportsHandles
        {
            get { return true; }
        }

        /** <inheritdoc /> */
        public void WriteBinary<T>(T obj, BinaryWriter writer)
        {
            var ctx = GetStreamingContext(writer);
            _serializableTypeDesc.OnSerializing(obj, ctx);

            var serializable = (ISerializable) obj;
            var objType = obj.GetType();

            // Get field values and write them.
            var serInfo = new SerializationInfo(objType, new FormatterConverter());
            serializable.GetObjectData(serInfo, ctx);

            var dotNetFields = WriteSerializationInfo(writer, serInfo);

            // Check if there is any additional information to be written.
            var customType = GetCustomType(serInfo, serializable);

            if (dotNetFields != null || writer.Marshaller.Ignite == null || customType != null)
            {
                // Set custom type flag in object header.
                writer.SetCustomTypeDataFlag(true);

                // Write additional information in raw mode.
                writer.GetRawWriter();

                WriteFieldNames(writer, serInfo);

                WriteCustomTypeInfo(writer, customType);

                WriteDotNetFields(writer, dotNetFields);
            }

            _serializableTypeDesc.OnSerialized(obj, ctx);
        }

        /** <inheritdoc /> */
        public T ReadBinary<T>(BinaryReader reader, IBinaryTypeDescriptor desc, int pos)
        {
            object res;
            var ctx = GetStreamingContext(reader);

            // Read additional information from raw part, if flag is set.
            IEnumerable<string> fieldNames;
            Type customType = null;
            ICollection<int> dotNetFields = null;

            if (reader.GetCustomTypeDataFlag())
            {
                var oldPos = reader.Stream.Position;
                reader.SeekToRaw();

                fieldNames = ReadFieldNames(reader, desc);
                customType = ReadCustomTypeInfo(reader);
                dotNetFields = ReadDotNetFields(reader);

                // Restore stream position.
                reader.Stream.Seek(oldPos, SeekOrigin.Begin);
            }
            else
            {
                fieldNames = GetBinaryTypeFields(reader, desc);
            }

            try
            {
                if (customType != null)
                {
                    // Custom type is present, which returns original type via IObjectReference.
                    var serInfo = ReadSerializationInfo(reader, fieldNames, desc, dotNetFields);

                    res = ReadAsCustomType(customType, serInfo, ctx);

                    // Handle is added after entire object is deserialized,
                    // because handles should not point to a custom type wrapper.
                    reader.AddHandle(pos, res);

                    DeserializationCallbackProcessor.Push(res);
                }
                else
                {
                    res = FormatterServices.GetUninitializedObject(desc.Type);

                    _serializableTypeDesc.OnDeserializing(res, ctx);

                    DeserializationCallbackProcessor.Push(res);

                    reader.AddHandle(pos, res);

                    // Read actual data and call constructor.
                    var serInfo = ReadSerializationInfo(reader, fieldNames, desc, dotNetFields);
                    _serializableTypeDesc.SerializationCtorUninitialized(res, serInfo, ctx);
                }

                _serializableTypeDesc.OnDeserialized(res, ctx);
                DeserializationCallbackProcessor.Pop();
            }
            catch (Exception)
            {
                DeserializationCallbackProcessor.Clear();
                throw;
            }

            return (T) res;
        }

        /// <summary>
        /// Writes .NET-specific fields.
        /// </summary>
        private static void WriteDotNetFields(IBinaryRawWriter writer, ICollection<string> dotNetFields)
        {
            if (dotNetFields == null)
            {
                writer.WriteInt(0);

                return;
            }

            writer.WriteInt(dotNetFields.Count);

            foreach (var dotNetField in dotNetFields)
            {
                writer.WriteInt(BinaryUtils.GetStringHashCode(dotNetField));
            }
        }

        /// <summary>
        /// Writes .NET-specific fields.
        /// </summary>
        private static ICollection<int> ReadDotNetFields(IBinaryRawReader reader)
        {
            int count = reader.ReadInt();

            if (count <= 0)
                return null;

            var res = new HashSet<int>();

            for (int i = 0; i < count; i++)
            {
                res.Add(reader.ReadInt());
            }

            return res;
        }

        /// <summary>
        /// Writes the field names.
        /// </summary>
        private static void WriteFieldNames(BinaryWriter writer, SerializationInfo serInfo)
        {
            if (writer.Marshaller.Ignite != null)
            {
                // Online mode: field names are in binary metadata.
                writer.WriteInt(-1);
                return;
            }

            // Offline mode: write all field names.
            // Even if MemberCount is 0, write empty array to denote offline mode.
            writer.WriteInt(serInfo.MemberCount);

            foreach (var entry in serInfo)
            {
                writer.WriteString(entry.Name);
            }
        }

        /// <summary>
        /// Gets the field names.
        /// </summary>
        private static IEnumerable<string> ReadFieldNames(BinaryReader reader, IBinaryTypeDescriptor desc)
        {
            var fieldCount = reader.ReadInt();

            if (fieldCount == 0)
                return Enumerable.Empty<string>();

            if (fieldCount > 0)
            {
                var fieldNames = new string[fieldCount];

                for (var i = 0; i < fieldCount; i++)
                {
                    fieldNames[i] = reader.ReadString();
                }

                return fieldNames;
            }

            // Negative field count: online mode.
            return GetBinaryTypeFields(reader, desc);
        }

        /// <summary>
        /// Gets the binary type fields.
        /// </summary>
        private static IEnumerable<string> GetBinaryTypeFields(BinaryReader reader, IBinaryTypeDescriptor desc)
        {
            var binaryType = reader.Marshaller.GetBinaryType(desc.TypeId);

            if (binaryType == BinaryType.Empty)
            {
                // Object without fields.
                return Enumerable.Empty<string>();
            }

            return binaryType.Fields;
        }

        /// <summary>
        /// Writes the custom type information.
        /// </summary>
        private static void WriteCustomTypeInfo(BinaryWriter writer, Type customType)
        {
            var raw = writer.GetRawWriter();

            if (customType != null)
            {
                raw.WriteBoolean(true);

                var desc = writer.Marshaller.GetDescriptor(customType);

                if (desc.IsRegistered)
                {
                    raw.WriteBoolean(true);
                    raw.WriteInt(desc.TypeId);
                }
                else
                {
                    raw.WriteBoolean(false);
                    raw.WriteString(customType.FullName);
                }
            }
            else
            {
                raw.WriteBoolean(false);
            }
        }

        /// <summary>
        /// Gets the custom serialization type.
        /// </summary>
        private static Type GetCustomType(SerializationInfo serInfo, ISerializable serializable)
        {
            // ISerializable implementor may call SerializationInfo.SetType() or FullTypeName setter.
            // In that case there is no serialization ctor on objType. 
            // Instead, we should instantiate specified custom type and then call IObjectReference.GetRealObject().
            if (serInfo.IsFullTypeNameSetExplicit)
            {
                return new TypeResolver().ResolveType(serInfo.FullTypeName, serInfo.AssemblyName);
            }
            
            if (serInfo.ObjectType != serializable.GetType())
            {
                return serInfo.ObjectType;
            }

            return null;
        }

        /// <summary>
        /// Reads the custom type information.
        /// </summary>
        private static Type ReadCustomTypeInfo(BinaryReader reader)
        {
            if (!reader.ReadBoolean())
                return null;

            Type customType;

            if (reader.ReadBoolean())
            {
                // Registered type written as type id.
                var typeId = reader.ReadInt();
                customType = reader.Marshaller.GetDescriptor(true, typeId, true).Type;

                if (customType == null)
                {
                    throw new BinaryObjectException(string.Format(
                        "Failed to resolve custom type provided by SerializationInfo: [typeId={0}]", typeId));
                }
            }
            else
            {
                // Unregistered type written as type name.
                var typeName = reader.ReadString();
                customType = new TypeResolver().ResolveType(typeName);

                if (customType == null)
                {
                    throw new BinaryObjectException(string.Format(
                        "Failed to resolve custom type provided by SerializationInfo: [typeName={0}]", typeName));
                }
            }

            return customType;
        }

        /// <summary>
        /// Writes the serialization information.
        /// </summary>
        private static List<string> WriteSerializationInfo(IBinaryWriter writer, SerializationInfo serInfo)
        {
            List<string> dotNetFields = null;

            // Write fields.
            foreach (var entry in GetEntries(serInfo).OrderBy(x => x.Name))
            {
                WriteEntry(writer, entry);

                var type = entry.Value == null ? null : entry.Value.GetType();

                if (type == typeof(sbyte) || type == typeof(ushort) || type == typeof(uint) || type == typeof(ulong)
                    || type == typeof(sbyte[]) || type == typeof(ushort[])
                    || type == typeof(uint[]) || type == typeof(ulong[]))
                {
                    // Denote .NET-specific type.
                    dotNetFields = dotNetFields ?? new List<string>();

                    dotNetFields.Add(entry.Name);
                }
            }

            return dotNetFields;
        }

        /// <summary>
        /// Writes the serialization entry.
        /// </summary>
        private static void WriteEntry(IBinaryWriter writer, SerializationEntry entry)
        {
            unchecked
            {
                var type = entry.ObjectType;

                if (type == typeof(byte))
                {
                    writer.WriteByte(entry.Name, (byte) entry.Value);
                }
                else if (type == typeof(byte[]))
                {
                    writer.WriteByteArray(entry.Name, (byte[]) entry.Value);
                }
                if (type == typeof(sbyte))
                {
                    writer.WriteByte(entry.Name, (byte) (sbyte) entry.Value);
                }
                else if (type == typeof(sbyte[]))
                {
                    writer.WriteByteArray(entry.Name, (byte[]) (Array) entry.Value);
                }
                else if (type == typeof(bool))
                {
                    writer.WriteBoolean(entry.Name, (bool) entry.Value);
                }
                else if (type == typeof(bool[]))
                {
                    writer.WriteBooleanArray(entry.Name, (bool[]) entry.Value);
                }
                else if (type == typeof(char))
                {
                    writer.WriteChar(entry.Name, (char) entry.Value);
                }
                else if (type == typeof(char[]))
                {
                    writer.WriteCharArray(entry.Name, (char[]) entry.Value);
                }
                else if (type == typeof(short))
                {
                    writer.WriteShort(entry.Name, (short) entry.Value);
                }
                else if (type == typeof(short[]))
                {
                    writer.WriteShortArray(entry.Name, (short[]) entry.Value);
                }
                else if (type == typeof(ushort))
                {
                    writer.WriteShort(entry.Name, (short) (ushort) entry.Value);
                }
                else if (type == typeof(ushort[]))
                {
                    writer.WriteShortArray(entry.Name, (short[]) (Array) entry.Value);
                }
                else if (type == typeof(int))
                {
                    writer.WriteInt(entry.Name, (int) entry.Value);
                }
                else if (type == typeof(int[]))
                {
                    writer.WriteIntArray(entry.Name, (int[]) entry.Value);
                }
                else if (type == typeof(uint))
                {
                    writer.WriteInt(entry.Name, (int) (uint) entry.Value);
                }
                else if (type == typeof(uint[]))
                {
                    writer.WriteIntArray(entry.Name, (int[]) (Array) entry.Value);
                }
                else if (type == typeof(long))
                {
                    writer.WriteLong(entry.Name, (long) entry.Value);
                }
                else if (type == typeof(long[]))
                {
                    writer.WriteLongArray(entry.Name, (long[]) entry.Value);
                }
                else if (type == typeof(ulong))
                {
                    writer.WriteLong(entry.Name, (long) (ulong) entry.Value);
                }
                else if (type == typeof(ulong[]))
                {
                    writer.WriteLongArray(entry.Name, (long[]) (Array) entry.Value);
                }
                else if (type == typeof(float))
                {
                    writer.WriteFloat(entry.Name, (float) entry.Value);
                }
                else if (type == typeof(float[]))
                {
                    writer.WriteFloatArray(entry.Name, (float[]) entry.Value);
                }
                else if (type == typeof(double))
                {
                    writer.WriteDouble(entry.Name, (double) entry.Value);
                }
                else if (type == typeof(double[]))
                {
                    writer.WriteDoubleArray(entry.Name, (double[]) entry.Value);
                }
                else if (type == typeof(decimal))
                {
                    writer.WriteDecimal(entry.Name, (decimal) entry.Value);
                }
                else if (type == typeof(decimal?))
                {
                    writer.WriteDecimal(entry.Name, (decimal?) entry.Value);
                }
                else if (type == typeof(decimal?[]))
                {
                    writer.WriteDecimalArray(entry.Name, (decimal?[]) entry.Value);
                }
                else if (type == typeof(string))
                {
                    writer.WriteString(entry.Name, (string) entry.Value);
                }
                else if (type == typeof(string[]))
                {
                    writer.WriteStringArray(entry.Name, (string[]) entry.Value);
                }
                else if (type == typeof(Guid))
                {
                    writer.WriteGuid(entry.Name, (Guid) entry.Value);
                }
                else if (type == typeof(Guid?))
                {
                    writer.WriteGuid(entry.Name, (Guid?) entry.Value);
                }
                else if (type == typeof(Guid?[]))
                {
                    writer.WriteGuidArray(entry.Name, (Guid?[]) entry.Value);
                }
                else
                {
                    writer.WriteObject(entry.Name, entry.Value);
                }
            }
        }

        /// <summary>
        /// Gets the entries.
        /// </summary>
        private static IEnumerable<SerializationEntry> GetEntries(SerializationInfo serInfo)
        {
            foreach (var entry in serInfo)
            {
                yield return entry;
            }
        }

        /// <summary>
        /// Reads the serialization information.
        /// </summary>
        private static SerializationInfo ReadSerializationInfo(BinaryReader reader, 
            IEnumerable<string> fieldNames, IBinaryTypeDescriptor desc, ICollection<int> dotNetFields)
        {
            var serInfo = new SerializationInfo(desc.Type, new FormatterConverter());

            if (dotNetFields == null)
            {
                foreach (var fieldName in fieldNames)
                {
                    var fieldVal = reader.ReadObject<object>(fieldName);

                    serInfo.AddValue(fieldName, fieldVal);
                }
            }
            else
            {
                foreach (var fieldName in fieldNames)
                {
                    var fieldVal = ReadField(reader, fieldName, dotNetFields);

                    serInfo.AddValue(fieldName, fieldVal);
                }
            }

            return serInfo;
        }

        /// <summary>
        /// Reads the object as a custom type.
        /// </summary>
        private static object ReadAsCustomType(Type customType, SerializationInfo serInfo, StreamingContext ctx)
        {
            var ctorFunc = SerializableTypeDescriptor.Get(customType).SerializationCtor;

            var customObj = ctorFunc(serInfo, ctx);

            var wrapper = customObj as IObjectReference;

            return wrapper == null
                ? customObj
                : wrapper.GetRealObject(ctx);
        }

        /// <summary>
        /// Gets the streaming context.
        /// </summary>
        private static StreamingContext GetStreamingContext(IBinaryReader reader)
        {
            return new StreamingContext(StreamingContextStates.All, reader);
        }

        /// <summary>
        /// Gets the streaming context.
        /// </summary>
        private static StreamingContext GetStreamingContext(IBinaryWriter writer)
        {
            return new StreamingContext(StreamingContextStates.All, writer);
        }

        /// <summary>
        /// Reads the field.
        /// <para />
        /// Java side does not have counterparts for byte, ushort, uint, ulong.
        /// For such fields we write a special boolean field indicating the type.
        /// If special field is present, then the value has to be converted to .NET-specific type.
        /// </summary>
        private static object ReadField(IBinaryReader reader, string fieldName, ICollection<int> dotNetFields)
        {
            var fieldVal = reader.ReadObject<object>(fieldName);

            if (fieldVal == null)
                return null;

            var fieldType = fieldVal.GetType();

            unchecked
            {
                if (fieldType == typeof(byte))
                {
                    return dotNetFields.Contains(BinaryUtils.GetStringHashCode(fieldName)) 
                        ? (sbyte) (byte) fieldVal : fieldVal;
                }

                if (fieldType == typeof(short))
                {
                    return dotNetFields.Contains(BinaryUtils.GetStringHashCode(fieldName)) 
                        ? (ushort) (short) fieldVal : fieldVal;
                }

                if (fieldType == typeof(int))
                {
                    return dotNetFields.Contains(BinaryUtils.GetStringHashCode(fieldName)) 
                        ? (uint) (int) fieldVal : fieldVal;
                }

                if (fieldType == typeof(long))
                {
                    return dotNetFields.Contains(BinaryUtils.GetStringHashCode(fieldName)) 
                        ? (ulong) (long) fieldVal : fieldVal;
                }

                if (fieldType == typeof(byte[]))
                {
                    return dotNetFields.Contains(BinaryUtils.GetStringHashCode(fieldName)) 
                        ? ConvertArray<byte, sbyte>((byte[]) fieldVal) : fieldVal;
                }

                if (fieldType == typeof(short[]))
                {
                    return dotNetFields.Contains(BinaryUtils.GetStringHashCode(fieldName))
                        ? ConvertArray<short, ushort>((short[]) fieldVal) : fieldVal;
                }

                if (fieldType == typeof(int[]))
                {
                    return dotNetFields.Contains(BinaryUtils.GetStringHashCode(fieldName)) 
                        ? ConvertArray<int, uint>((int[]) fieldVal) : fieldVal;
                }

                if (fieldType == typeof(long[]))
                {
                    return dotNetFields.Contains(BinaryUtils.GetStringHashCode(fieldName)) 
                        ? ConvertArray<long, ulong>((long[]) fieldVal) : fieldVal;
                }
            }

            return fieldVal;
        }

        /// <summary>
        /// Converts the array.
        /// </summary>
        private static TU[] ConvertArray<T, TU>(T[] arr)
        {
            var res = new TU[arr.Length];

            for (var i = 0; i < arr.Length; i++)
            {
                res[i] = TypeCaster<TU>.Cast(arr[i]);
            }

            return res;
        }
    }
}
