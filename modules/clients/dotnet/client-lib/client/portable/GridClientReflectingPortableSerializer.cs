using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace GridGain.Client.Portable
{
    using System.Collections;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Reflection;

    /**
     * <summary>Serializer which reflectively writes all fields except of transient ones.</summary>
     */ 
    class GridClientReflectingPortableSerializer : IGridClientPortableSerializer
    {
        /** Cached binding flags. */
        private static readonly BindingFlags FLAGS = BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.DeclaredOnly;
        
        /** Cached type descriptors. */
        private readonly ConcurrentDictionary<Type, Descriptor> types = new ConcurrentDictionary<Type, Descriptor>();

        /** <inheritdoc /> */
        public void WritePortable(object obj, IGridClientPortableWriter writer)
        {
            Type type = obj.GetType();

            Descriptor desc = types[type];

            if (desc == null) 
                desc = types.GetOrAdd(type, Descriptor(type));

            desc.Write(obj, writer);
        }

        /** <inheritdoc /> */
        public T ReadPortable<T>(IGridClientPortableReader reader)
        {
            throw new NotImplementedException();
        }

        /**
         * <summary>Create descriptor for the given type.</summary>
         */ 
        private Descriptor Descriptor(Type type)
        {
            List<FieldInfo> fields = new List<FieldInfo>();

            Type curType = type;

            while (curType != null) 
            {
                foreach(FieldInfo fieldInfo in curType.GetFields(FLAGS)) 
                {
                    if (!fieldInfo.IsNotSerialized)
                        fields.Add(fieldInfo);
                }

                fields.AddRange(curType.GetFields(FLAGS));

                curType = curType.BaseType;
            }

            fields.Sort(compare);

            return new Descriptor(fields);
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
            private readonly ICollection<Action<Object, IGridClientPortableWriter>> actions;

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
                    string name = field.Name.ToLower();

                    if (type == typeof(Boolean))                    
                        actions.Add((obj, writer) => { writer.WriteBoolean(name, (Boolean)field.GetValue(obj)); });                    
                    else if (type == typeof(Byte))
                        throw new GridClientPortableInvalidFieldException("Cannot serialize Byte field (change it to singed type or use custom serialization): " + name);
                    else if (type == typeof(SByte))
                        actions.Add((obj, writer) => { writer.WriteByte(name, (SByte)field.GetValue(obj)); });
                    else if (type == typeof(UInt16))
                        throw new GridClientPortableInvalidFieldException("Cannot serialize UInt16 field (change it to singed type or use custom serialization): " + name);
                    else if (type == typeof(Int16))
                        actions.Add((obj, writer) => { writer.WriteShort(name, (Int16)field.GetValue(obj)); });
                    else if (type == typeof(UInt32))
                        throw new GridClientPortableInvalidFieldException("Cannot serialize UInt32 field (change it to singed type or use custom serialization): " + name);
                    else if (type == typeof(Int32))
                        actions.Add((obj, writer) => { writer.WriteInt(name, (Int32)field.GetValue(obj)); });
                    else if (type == typeof(UInt64))
                        throw new GridClientPortableInvalidFieldException("Cannot serialize UInt64 field (change it to singed type or use custom serialization): " + name);
                    else if (type == typeof(Int64))
                        actions.Add((obj, writer) => { writer.WriteLong(name, (Int64)field.GetValue(obj)); });
                    else if (type == typeof(Char))
                        actions.Add((obj, writer) => { writer.WriteChar(name, (Char)field.GetValue(obj)); });
                    else if (type == typeof(Single))
                        actions.Add((obj, writer) => { writer.WriteFloat(name, (Single)field.GetValue(obj)); });
                    else if (type == typeof(Double))
                        actions.Add((obj, writer) => { writer.WriteDouble(name, (Double)field.GetValue(obj)); });
                    else if (type == typeof(String))
                        actions.Add((obj, writer) => { writer.WriteString(name, (String)field.GetValue(obj)); });
                    else if (type == typeof(Guid))
                        actions.Add((obj, writer) => { writer.WriteGuid(name, (Guid)field.GetValue(obj)); });
                    else if (type.IsGenericType) {
                        // TODO                                
                    }
                    else if (type is IDictionary) 
                    {
                        actions.Add((obj, writer) => 
                            { 
                                dynamic val = field.GetValue(obj);  

                                writer.WriteMap(name, val); 
                            });
                    }
                    else if (type is ICollection) 
                    { 
                        actions.Add((obj, writer) => 
                            { 
                                dynamic val = field.GetValue(obj);  

                                writer.WriteCollection(name, val); 
                            });
                    }
                    else if (type.IsArray)
                    { 
                        // TODO
                    }
                    else 
                    {
                        // TODO: The rest types.
                    }
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
