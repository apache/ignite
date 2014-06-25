/* @csharp.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Client.Portable
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.Reflection;
    using GridGain.Client.Portable;

    using PU = GridGain.Client.Impl.Portable.GridClientPortableUilts;

    /**
     * <summary>Serializer which reflectively writes all fields except of transient ones.</summary>
     */ 
    class GridClientPortableReflectiveSerializer : IGridClientPortableSerializer
    {
        /** Cached binding flags. */
        private static readonly BindingFlags FLAGS = BindingFlags.Instance | BindingFlags.Public | 
            BindingFlags.NonPublic | BindingFlags.DeclaredOnly;

        /** Collection type. */
        private static readonly Type TYP_COLLECTION = typeof(ICollection);

        /** Dictionary type. */
        private static readonly Type TYP_DICTIONARY = typeof(IDictionary);

        /** Generic collection type. */
        private static readonly Type TYP_GENERIC_COLLECTION = typeof(ICollection<>);

        /** Generic dictionary type. */
        private static readonly Type TYP_GENERIC_DICTIONARY = typeof(IDictionary<,>);

        /** Method: read generic collection. */
        private static readonly MethodInfo MTHD_READ_GENERIC_COLLECTION = 
            typeof(IGridClientPortableReader).GetMethod("ReadGenericCollection", new Type[] { typeof(string) });

        /** Method: read generic dictionary. */
        private static readonly MethodInfo MTHD_READ_GENERIC_DICTIONARY =
            typeof(IGridClientPortableReader).GetMethod("ReadGenericDictionary", new Type[] { typeof(string) });

        /** Cached type descriptors. */
        private readonly IDictionary<Type, Descriptor> types = new Dictionary<Type, Descriptor>();

        /** <inheritdoc /> */
        public void WritePortable(object obj, IGridClientPortableWriter writer)
        {
            if (obj is IGridClientPortable)
                ((IGridClientPortable)obj).WritePortable(writer);
            else
            {
                Type type = obj.GetType();

                Descriptor desc = types[type];

                if (desc == null)
                    throw new GridClientPortableException("Type is not registered in serializer: " + type.Name);

                desc.Write(obj, writer);
            }
        }

        /** <inheritdoc /> */
        public void ReadPortable(object obj, IGridClientPortableReader reader)
        {
            if (obj is IGridClientPortable)
                ((IGridClientPortable)obj).ReadPortable(reader);
            else
            {
                Type type = obj.GetType();

                Descriptor desc = types[type];

                if (desc == null)
                    throw new GridClientPortableException("Type is not registered in serializer: " + type.Name);

                desc.Read(obj, reader);
            }
        }

        /**
         * <summary>Register type.</summary>
         * <param name="type">Type.</param>
         * <param name="typeId">Type ID.</param>
         * <param name="idMapper">ID mapper.</param>
         */
        public void Register(Type type, int typeId, GridClientPortableIdResolver idMapper)
        {
            if (type.GetInterface(typeof(IGridClientPortable).Name) != null)
                return;

            List<FieldInfo> fields = new List<FieldInfo>();

            Type curType = type;

            while (curType != null)
            {
                foreach (FieldInfo field in curType.GetFields(FLAGS))
                {
                    if (!field.IsNotSerialized)
                        fields.Add(field);
                }

                curType = curType.BaseType;
            }

            IDictionary<int, string> idMap = new Dictionary<int, string>();

            foreach (FieldInfo field in fields)
            {
                int? fieldIdRef = idMapper.FieldId(typeId, field.Name);

                int fieldId = fieldIdRef.HasValue ? fieldIdRef.Value : PU.StringHashCode(field.Name.ToLower());

                if (idMap.ContainsKey(fieldId))
                {
                    throw new GridClientPortableException("Conflicting field IDs [type=" +
                        type.Name + ", field1=" + idMap[fieldId] + ", field2=" + field.Name + 
                        ", fieldId=" + fieldId + ']');
                }
                else
                    idMap[fieldId] = field.Name;
            }

            fields.Sort(compare);

            Descriptor desc = new Descriptor(fields);

            types[type] = desc;
        }

        /**
         * <summary>Compare two FieldInfo instances.</summary>
         */ 
        private int compare(FieldInfo info1, FieldInfo info2) {
            return info1.Name.ToLower().CompareTo(info2.Name.ToLower());
        }

        /**
         * <summary>Type descriptor.</summary>
         */
        private class Descriptor
        {
            /** Write actions to be performed. */
            private readonly ICollection<Action<Object, IGridClientPortableWriter>> wActions;

            /** Read actions to be performed. */
            private readonly ICollection<Action<Object, IGridClientPortableReader>> rActions;

            /**
             * <summary>Constructor.</summary>
             * <param name="fields">Fields.</param>
             */ 
            public Descriptor(List<FieldInfo> fields)
            {
                wActions = new List<Action<Object, IGridClientPortableWriter>>(fields.Count);
                rActions = new List<Action<Object, IGridClientPortableReader>>(fields.Count);

                foreach (FieldInfo field in fields)
                {
                    Type type = field.FieldType;

                    bool nullable = type.IsGenericType && type.GetGenericTypeDefinition() == typeof(Nullable<>);

                    Type nullableType = nullable ? type.GetGenericArguments()[0] : null;

                    string name = field.Name;

                    if (type.IsPrimitive) 
                    {
                        WritePrimitive(field, type, name);
                        ReadPrimitive(field, type, name);
                    }
                    else if (type == typeof(String))
                    {
                        wActions.Add((obj, writer) => { writer.WriteString(name, (String)field.GetValue(obj)); });
                        rActions.Add((obj, reader) => { field.SetValue(obj, reader.ReadString(name)); });
                    }
                    else if (type == typeof(Guid))
                    {
                        wActions.Add((obj, writer) => { writer.WriteGuid(name, (Guid)field.GetValue(obj)); });
                        rActions.Add((obj, reader) => { field.SetValue(obj, reader.ReadGuid(name)); });
                    }
                    else if (nullable && nullableType == typeof(Guid))
                    {
                        wActions.Add((obj, writer) => { writer.WriteGuid(name, (Guid?)field.GetValue(obj)); });
                        rActions.Add((obj, reader) => { field.SetValue(obj, reader.ReadGuid(name)); });
                    }
                    else if (type.IsArray)
                        HandleArray(field, type, name, wActions);
                    else if (type.IsGenericType && type.GetInterface(TYP_GENERIC_DICTIONARY.FullName) != null)
                    {
                        wActions.Add((obj, writer) =>
                        {
                            dynamic val = field.GetValue(obj);

                            writer.WriteGenericDictionary(name, val);
                        });

                        rActions.Add((obj, reader) =>
                        {
                            object val = MTHD_READ_GENERIC_DICTIONARY
                                .MakeGenericMethod(type.GetInterface(TYP_GENERIC_DICTIONARY.FullName)
                                .GetGenericArguments())
                                .Invoke(reader, new object[] { name });

                            field.SetValue(obj, val);
                        });
                    }
                    else if (type.IsGenericType && type.GetInterface(TYP_GENERIC_COLLECTION.FullName) != null)
                    {
                        wActions.Add((obj, writer) =>
                        {
                            dynamic val = field.GetValue(obj);

                            writer.WriteGenericCollection(name, val);
                        });

                        rActions.Add((obj, reader) =>
                        {
                            object val = MTHD_READ_GENERIC_COLLECTION
                                .MakeGenericMethod(type.GetInterface(TYP_GENERIC_COLLECTION.FullName)
                                .GetGenericArguments())
                                .Invoke(reader, new object[] { name });

                            field.SetValue(obj, val);
                        });
                    }
                    else if (type.GetInterface(TYP_DICTIONARY.FullName) != null)
                    {
                        wActions.Add((obj, writer) =>
                        {
                            IDictionary val = (IDictionary)field.GetValue(obj);

                            writer.WriteDictionary(name, val);
                        });

                        rActions.Add((obj, reader) =>
                        {
                            object val = reader.ReadCollection(name);

                            field.SetValue(obj, val);
                        });
                    }
                    else if (type.GetInterface(TYP_COLLECTION.FullName) != null)
                    {
                        wActions.Add((obj, writer) =>
                        {
                            ICollection val = (ICollection)field.GetValue(obj);

                            writer.WriteCollection(name, val);
                        });

                        rActions.Add((obj, reader) =>
                        {
                            object val = reader.ReadCollection(name);

                            field.SetValue(obj, val);
                        });
                    }

                    else if (type.IsGenericType && type.GetInterface(TYP_GENERIC_COLLECTION.Name) != null)
                    {
                        wActions.Add((obj, writer) =>
                        {
                            dynamic val = field.GetValue(obj);

                            writer.WriteGenericCollection(name, val);
                        });

                        rActions.Add((obj, reader) => 
                        {
                            object val = MTHD_READ_GENERIC_COLLECTION.MakeGenericMethod(type.GetGenericArguments()[0]).Invoke(reader, new object[] { name });

                            field.SetValue(obj, val);
                        });
                    }
                    //else if (type is IDictionary)
                    //{
                    //    wActions.Add((obj, writer) =>
                    //    {
                    //        dynamic val = (IDictionary)field.GetValue(obj);

                    //        //writer.WriteMap(name, val);
                    //    });
                    //}
                    else if (type == TYP_COLLECTION || type.GetInterface(TYP_COLLECTION.FullName) != null)
                    {
                        wActions.Add((obj, writer) =>
                        {
                            dynamic val = (ICollection)field.GetValue(obj);

                            writer.WriteCollection(name, val);
                        });

                        rActions.Add((obj, reader) =>
                        {
                            object val = reader.ReadCollection(name);

                            field.SetValue(obj, val);
                        });
                    }
                    else
                    {
                        wActions.Add((obj, writer) => { writer.WriteObject(name, field.GetValue(obj)); });
                        rActions.Add((obj, reader) => { field.SetValue(obj, reader.ReadObject<object>(name)); });
                    }                        
                }
            }

            /**
             * <summary>Handle primitive field write</summary>
             * <param name="field">Field.</param>
             * <param name="type">Field type.</param>
             * <param name="name">Field name.</param>
             */
            private unsafe void WritePrimitive(FieldInfo field, Type type, string name)
            {
                unchecked
                {
                    if (type == typeof(Boolean))
                        wActions.Add((obj, writer) => { writer.WriteBoolean(name, (Boolean)field.GetValue(obj)); });
                    else if (type == typeof(SByte))
                    {
                        wActions.Add((obj, writer) =>
                            {
                                SByte val = (SByte)field.GetValue(obj);

                                writer.WriteByte(name, *(byte*)&val);
                            });
                    }
                    else if (type == typeof(Byte))
                        wActions.Add((obj, writer) => { writer.WriteByte(name, (Byte)field.GetValue(obj)); });
                    else if (type == typeof(Int16))
                        wActions.Add((obj, writer) => { writer.WriteShort(name, (Int16)field.GetValue(obj)); });
                    else if (type == typeof(UInt16))
                    {
                        wActions.Add((obj, writer) =>
                        {
                            UInt16 val = (UInt16)field.GetValue(obj);

                            writer.WriteShort(name, *(Int16*)&val);
                        });
                    }
                    else if (type == typeof(Int32))
                        wActions.Add((obj, writer) => { writer.WriteInt(name, (Int32)field.GetValue(obj)); });
                    else if (type == typeof(UInt32))
                    {
                        wActions.Add((obj, writer) =>
                        {
                            UInt32 val = (UInt32)field.GetValue(obj);

                            writer.WriteInt(name, *(Int32*)&val);
                        });
                    }
                    else if (type == typeof(Int64))
                        wActions.Add((obj, writer) => { writer.WriteLong(name, (Int64)field.GetValue(obj)); });
                    else if (type == typeof(UInt64))
                    {
                        wActions.Add((obj, writer) =>
                        {
                            UInt64 val = (UInt64)field.GetValue(obj);

                            writer.WriteLong(name, *(Int64*)&val);
                        });
                    }
                    else if (type == typeof(Char))
                        wActions.Add((obj, writer) => { writer.WriteChar(name, (Char)field.GetValue(obj)); });
                    else if (type == typeof(Single))
                        wActions.Add((obj, writer) => { writer.WriteFloat(name, (Single)field.GetValue(obj)); });
                    else if (type == typeof(Double))
                        wActions.Add((obj, writer) => { writer.WriteDouble(name, (Double)field.GetValue(obj)); });
                }
            }

            /**
             * <summary>Handle primitive field read</summary>
             * <param name="field">Field.</param>
             * <param name="type">Field type.</param>
             * <param name="name">Field name.</param>
             */
            private unsafe void ReadPrimitive(FieldInfo field, Type type, string name)
            {
                unchecked
                {
                    if (type == typeof(Boolean))
                        rActions.Add((obj, reader) => { field.SetValue(obj, reader.ReadBoolean(name)); });
                    else if (type == typeof(SByte))
                    {
                        rActions.Add((obj, reader) =>
                        {
                            byte val = reader.ReadByte(name);

                            field.SetValue(obj, *(SByte*)&val);
                        });
                    }
                    else if (type == typeof(Byte))
                        rActions.Add((obj, reader) => { field.SetValue(obj, reader.ReadByte(name)); });
                    else if (type == typeof(Int16))
                        rActions.Add((obj, reader) => { field.SetValue(obj, reader.ReadShort(name)); });
                    else if (type == typeof(UInt16))
                    {
                        rActions.Add((obj, reader) =>
                        {
                            short val = reader.ReadShort(name);

                            field.SetValue(obj, *(UInt16*)&val);
                        });
                    }
                    else if (type == typeof(Int32))
                        rActions.Add((obj, reader) => { field.SetValue(obj, reader.ReadInt(name)); });
                    else if (type == typeof(UInt32))
                    {
                        rActions.Add((obj, reader) =>
                        {
                            int val = reader.ReadInt(name);

                            field.SetValue(obj, *(UInt32*)&val);
                        });
                    }
                    else if (type == typeof(Int64))
                        rActions.Add((obj, reader) => { field.SetValue(obj, reader.ReadLong(name)); });
                    else if (type == typeof(UInt64))
                    {
                        rActions.Add((obj, reader) =>
                        {
                            long val = reader.ReadLong(name);

                            field.SetValue(obj, *(UInt64*)&val);
                        });
                    }
                    else if (type == typeof(Char))
                        rActions.Add((obj, reader) => { field.SetValue(obj, reader.ReadChar(name)); });
                    else if (type == typeof(Single))
                        rActions.Add((obj, reader) => { field.SetValue(obj, reader.ReadFloat(name)); });
                    else if (type == typeof(Double))
                        rActions.Add((obj, reader) => { field.SetValue(obj, reader.ReadDouble(name)); });
                }
            }

            /**
             * <summary>Handle array field write</summary>
             * <param name="field">Field.</param>
             * <param name="type">Field type.</param>
             * <param name="name">Field name.</param>
             * <param name="actions">Actions.</param>
             */
            private void HandleArray(FieldInfo field, Type type, string name, 
                ICollection<Action<Object, IGridClientPortableWriter>> actions)
            {
                unchecked
                {
                    Type elemType = type.GetElementType();

                    bool nullable = type.IsGenericTypeDefinition && type.GetGenericTypeDefinition() == typeof(Nullable<>);

                    Type nullableElemType = nullable ? type.GetGenericArguments()[0] : null;

                    if (elemType == typeof(Boolean))
                        actions.Add((obj, writer) => { writer.WriteBooleanArray(name, 
                            (Boolean[])field.GetValue(obj)); });
                    else if (elemType == typeof(Byte) || elemType == typeof(SByte))
                        actions.Add((obj, writer) => { writer.WriteByteArray(name, (Byte[])field.GetValue(obj)); });
                    else if (elemType == typeof(Int16) || elemType == typeof(UInt16))
                        actions.Add((obj, writer) => { writer.WriteShortArray(name, (Int16[])field.GetValue(obj)); });
                    else if (elemType == typeof(Int32) || elemType == typeof(UInt32))
                        actions.Add((obj, writer) => { writer.WriteIntArray(name, (Int32[])field.GetValue(obj)); });
                    else if (elemType == typeof(Int64) || elemType == typeof(UInt64))
                        actions.Add((obj, writer) => { writer.WriteLongArray(name, (Int64[])field.GetValue(obj)); });
                    else if (elemType == typeof(Char))
                        actions.Add((obj, writer) => { writer.WriteCharArray(name, (Char[])field.GetValue(obj)); });
                    else if (elemType == typeof(Single))
                        actions.Add((obj, writer) => { writer.WriteFloatArray(name, 
                            (Single[])field.GetValue(obj)); });
                    else if (elemType == typeof(Double))
                        actions.Add((obj, writer) => { writer.WriteDoubleArray(name, 
                            (Double[])field.GetValue(obj)); });
                    else if (elemType == typeof(String))
                        actions.Add((obj, writer) => { writer.WriteStringArray(name, 
                            (String[])field.GetValue(obj)); });
                    else if (elemType == typeof(Guid) || (nullable && nullableElemType == typeof(Guid)))
                        actions.Add((obj, writer) => { writer.WriteGuidArray(name, (Guid?[])field.GetValue(obj)); });
                    else
                    {
                        actions.Add((obj, writer) =>
                        {
                            dynamic val = field.GetValue(obj);

                            writer.WriteObjectArray(name, val);
                        });
                    }
                }
            }

            /**
             * <summary>Handle generic collection field write</summary>
             * <param name="field">Field.</param>
             * <param name="type">Field type.</param>
             * <param name="name">Field name.</param>
             * <param name="actions">Actions.</param>
             */
            private void HandleGeneric(FieldInfo field, Type type, string name, 
                ICollection<Action<Object, IGridClientPortableWriter>> actions)
            {
                if (type.IsGenericType && type.GetInterface(TYP_GENERIC_DICTIONARY.Name) != null)
                {
                    actions.Add((obj, writer) => 
                    {
                        dynamic val = field.GetValue(obj);

                        //writer.WriteMap(name, val); 
                    });
                }
                else if (type.IsGenericType && type.GetInterface(TYP_GENERIC_COLLECTION.Name) != null)
                {
                    actions.Add((obj, writer) =>
                    {
                        dynamic val = field.GetValue(obj);

                        writer.WriteCollection(name, val);
                    });
                }
            }

            /**
             * <summary>Write object.</summary>
             * <param name="obj">Object.</param>
             * <param name="writer">Portable writer.</param>
             */ 
            public void Write(object obj, IGridClientPortableWriter writer)
            {
                foreach (Action<Object, IGridClientPortableWriter> action in wActions)
                    action.Invoke(obj, writer);
            }

            /**
             * <summary>Read object.</summary>
             * <param name="obj">Object.</param>
             * <param name="reader">Portable reader.</param>
             */
            public void Read(object obj, IGridClientPortableReader reader)
            {
                foreach (Action<Object, IGridClientPortableReader> action in rActions)
                    action.Invoke(obj, reader);
            }
        }
    }
}
