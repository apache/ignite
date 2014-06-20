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

        /** Cached type descriptors. */
        private readonly IDictionary<Type, Descriptor> types = new Dictionary<Type, Descriptor>();

        /** <inheritdoc /> */
        public void WritePortable(object obj, IGridClientPortableWriter writer)
        {
            if (obj is IGridClientPortable)
            {
                IGridClientPortable obj0 = (IGridClientPortable)obj;

                obj0.WritePortable(writer);
            }
            else
            {

                Type type = obj.GetType();

                Descriptor desc = types[type];

                if (desc == null)
                    throw new GridClientPortableException("Type is not registered in reflecting serializer: " + 
                        type.Name);

                desc.Write(obj, writer);
            }
        }

        /** <inheritdoc /> */
        public T ReadPortable<T>(object obj, IGridClientPortableReader reader)
        {
            throw new NotImplementedException();
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
            /** Actions to be performed. */
            private readonly ICollection<Action<Object, IGridClientPortableWriter>> actions = 
                new List<Action<Object, IGridClientPortableWriter>>();

            /**
             * <summary>Constructor.</summary>
             * <param name="fields">Fields.</param>
             */ 
            public Descriptor(List<FieldInfo> fields)
            {
                actions = new List<Action<Object, IGridClientPortableWriter>>(fields.Count);

                foreach (FieldInfo field in fields)
                {
                    Type type = field.FieldType;
                    string name = field.Name;

                    if (type.IsPrimitive)
                        HandlePrimitive(field, type, name, actions);
                    else if (type == typeof(String))
                        actions.Add((obj, writer) => { writer.WriteString(name, (String)field.GetValue(obj)); });
                    else if (type == typeof(Guid))
                        actions.Add((obj, writer) => { writer.WriteGuid(name, (Guid)field.GetValue(obj)); });
                    else if (type.IsEnum)
                        actions.Add((obj, writer) => { writer.WriteEnum(name, (Enum)field.GetValue(obj)); });
                    else if (type.IsArray)
                        HandleArray(field, type, name, actions);
                    else if (type.IsGenericType && type.GetInterface(TYP_GENERIC_DICTIONARY.Name) != null)
                    {
                        actions.Add((obj, writer) =>
                        {
                            dynamic val = field.GetValue(obj);

                            writer.WriteMap(name, val);
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
                    else if (type is IDictionary) 
                    {
                        actions.Add((obj, writer) =>
                        {
                            dynamic val = (IDictionary)field.GetValue(obj);

                            writer.WriteMap(name, val);
                        });
                    }
                    else if (type is ICollection)
                    {
                        actions.Add((obj, writer) =>
                        {
                            dynamic val = (ICollection)field.GetValue(obj);

                            writer.WriteCollection(name, val);
                        });
                    }
                    else
                        actions.Add((obj, writer) => { writer.WriteObject(name, field.GetValue(obj)); });
                }
            }

            /**
             * <summary>Handle primitive field write</summary>
             * <param name="field">Field.</param>
             * <param name="type">Field type.</param>
             * <param name="name">Field name.</param>
             * <param name="actions">Actions.</param>
             */
            private void HandlePrimitive(FieldInfo field, Type type, string name, 
                ICollection<Action<Object, IGridClientPortableWriter>> actions)
            {
                unchecked
                {
                    if (type == typeof(Boolean))
                        actions.Add((obj, writer) => { writer.WriteBoolean(name, (Boolean)field.GetValue(obj)); });
                    else if (type == typeof(Byte) || type == typeof(SByte))
                        actions.Add((obj, writer) => { writer.WriteByte(name, (Byte)field.GetValue(obj)); });
                    else if (type == typeof(Int16) || type == typeof(UInt16))
                        actions.Add((obj, writer) => { writer.WriteShort(name, (Int16)field.GetValue(obj)); });
                    else if (type == typeof(Int32) || type == typeof(UInt32))
                        actions.Add((obj, writer) => { writer.WriteInt(name, (Int32)field.GetValue(obj)); });
                    else if (type == typeof(Int64) || type == typeof(UInt64))
                        actions.Add((obj, writer) => { writer.WriteLong(name, (Int64)field.GetValue(obj)); });
                    else if (type == typeof(Char))
                        actions.Add((obj, writer) => { writer.WriteChar(name, (Char)field.GetValue(obj)); });
                    else if (type == typeof(Single))
                        actions.Add((obj, writer) => { writer.WriteFloat(name, (Single)field.GetValue(obj)); });
                    else if (type == typeof(Double))
                        actions.Add((obj, writer) => { writer.WriteDouble(name, (Double)field.GetValue(obj)); });
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
                    else if (elemType == typeof(Guid))
                        actions.Add((obj, writer) => { writer.WriteGuidArray(name, (Guid[])field.GetValue(obj)); });
                    else if (elemType == typeof(Enum))
                        actions.Add((obj, writer) => { writer.WriteEnumArray(name, (Enum[])field.GetValue(obj)); });
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

                        writer.WriteMap(name, val); 
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
                foreach (Action<Object, IGridClientPortableWriter> action in actions)
                    action.Invoke(obj, writer);
            }
        }
    }
}
