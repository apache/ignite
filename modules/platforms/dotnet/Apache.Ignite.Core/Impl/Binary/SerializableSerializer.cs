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

            // Write additional information in raw mode.
            writer.GetRawWriter();

            WriteFieldNames(writer, serInfo);

            WriteCustomTypeInfo(writer, serInfo, serializable);

            WriteDotNetFields(writer, dotNetFields);

            _serializableTypeDesc.OnSerialized(obj, ctx);
        }

        /** <inheritdoc /> */
        public T ReadBinary<T>(BinaryReader reader, IBinaryTypeDescriptor desc, int pos)
        {
            object res;
            var ctx = GetStreamingContext(reader);
            var callbackPushed = false;

            // Read additional information from raw part.
            var oldPos = reader.Stream.Position;
            reader.SeekToRaw();

            var fieldNames = ReadFieldNames(reader, desc);
            var customType = ReadCustomTypeInfo(reader);
            var dotNetFields = ReadDotNetFields(reader);

            // Restore stream position.
            reader.Stream.Seek(oldPos, SeekOrigin.Begin);

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
                    callbackPushed = true;
                }
                else
                {
                    res = FormatterServices.GetUninitializedObject(desc.Type);

                    _serializableTypeDesc.OnDeserializing(res, ctx);

                    DeserializationCallbackProcessor.Push(res);
                    callbackPushed = true;

                    reader.AddHandle(pos, res);

                    // Read actual data and call constructor.
                    var serInfo = ReadSerializationInfo(reader, fieldNames, desc, dotNetFields);
                    _serializableTypeDesc.SerializationCtorUninitialized(res, serInfo, ctx);
                }

                _serializableTypeDesc.OnDeserialized(res, ctx);
            }
            finally
            {
                if (callbackPushed)
                    DeserializationCallbackProcessor.Pop();
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
        private static void WriteCustomTypeInfo(BinaryWriter writer, SerializationInfo serInfo, 
            ISerializable serializable)
        {
            // ISerializable implementor may call SerializationInfo.SetType() or FullTypeName setter.
            // In that case there is no serialization ctor on objType. 
            // Instead, we should instantiate specified custom type and then call IObjectReference.GetRealObject().
            Type customType = null;

            if (serInfo.IsFullTypeNameSetExplicit)
            {
                customType = new TypeResolver().ResolveType(serInfo.FullTypeName, serInfo.AssemblyName);
            }
            else if (serInfo.ObjectType != serializable.GetType())
            {
                customType = serInfo.ObjectType;
            }

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
            foreach (var entry in serInfo)
            {
                writer.WriteObject(entry.Name, entry.Value);

                var type = entry.ObjectType;

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
