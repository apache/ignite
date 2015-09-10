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

namespace Apache.Ignite.Core.Impl.Portable
{
    using System;
    using System.Collections.Generic;
    using System.Reflection;
    using Apache.Ignite.Core.Portable;

    /// <summary>
    /// Portable serializer which reflectively writes all fields except of ones with 
    /// <see cref="System.NonSerializedAttribute"/>.
    /// <para />
    /// Note that Java platform stores dates as a difference between current time 
    /// and predefined absolute UTC date. Therefore, this difference is always the 
    /// same for all time zones. .Net, in contrast, stores dates as a difference 
    /// between current time and some predefined date relative to the current time 
    /// zone. It means that this difference will be different as you change time zones. 
    /// To overcome this discrepancy Ignite always converts .Net date to UTC form 
    /// before serializing and allows user to decide whether to deserialize them 
    /// in UTC or local form using <c>ReadDate(..., true/false)</c> methods in 
    /// <see cref="IPortableReader"/> and <see cref="IPortableRawReader"/>.
    /// This serializer always read dates in UTC form. It means that if you have
    /// local date in any field/property, it will be implicitly converted to UTC
    /// form after the first serialization-deserialization cycle. 
    /// </summary>
    internal class PortableReflectiveSerializer : IPortableSerializer
    {
        /** Cached binding flags. */
        private static readonly BindingFlags Flags = BindingFlags.Instance | BindingFlags.Public |
            BindingFlags.NonPublic | BindingFlags.DeclaredOnly;

        /** Cached type descriptors. */
        private readonly IDictionary<Type, Descriptor> _types = new Dictionary<Type, Descriptor>();

        /// <summary>
        /// Write portalbe object.
        /// </summary>
        /// <param name="obj">Object.</param>
        /// <param name="writer">Portable writer.</param>
        /// <exception cref="PortableException">Type is not registered in serializer:  + type.Name</exception>
        public void WritePortable(object obj, IPortableWriter writer)
        {
            var portableMarshalAware = obj as IPortableMarshalAware;

            if (portableMarshalAware != null)
                portableMarshalAware.WritePortable(writer);
            else
                GetDescriptor(obj).Write(obj, writer);
        }

        /// <summary>
        /// Read portable object.
        /// </summary>
        /// <param name="obj">Instantiated empty object.</param>
        /// <param name="reader">Portable reader.</param>
        /// <exception cref="PortableException">Type is not registered in serializer:  + type.Name</exception>
        public void ReadPortable(object obj, IPortableReader reader)
        {
            var portableMarshalAware = obj as IPortableMarshalAware;
            
            if (portableMarshalAware != null)
                portableMarshalAware.ReadPortable(reader);
            else
                GetDescriptor(obj).Read(obj, reader);
        }

        /// <summary>Register type.</summary>
        /// <param name="type">Type.</param>
        /// <param name="typeId">Type ID.</param>
        /// <param name="converter">Name converter.</param>
        /// <param name="idMapper">ID mapper.</param>
        public void Register(Type type, int typeId, IPortableNameMapper converter,
            IPortableIdMapper idMapper)
        {
            if (type.GetInterface(typeof(IPortableMarshalAware).Name) != null)
                return;

            List<FieldInfo> fields = new List<FieldInfo>();

            Type curType = type;

            while (curType != null)
            {
                foreach (FieldInfo field in curType.GetFields(Flags))
                {
                    if (!field.IsNotSerialized)
                        fields.Add(field);
                }

                curType = curType.BaseType;
            }

            IDictionary<int, string> idMap = new Dictionary<int, string>();

            foreach (FieldInfo field in fields)
            {
                string fieldName = PortableUtils.CleanFieldName(field.Name);

                int fieldId = PortableUtils.FieldId(typeId, fieldName, converter, idMapper);

                if (idMap.ContainsKey(fieldId))
                {
                    throw new PortableException("Conflicting field IDs [type=" +
                        type.Name + ", field1=" + idMap[fieldId] + ", field2=" + fieldName +
                        ", fieldId=" + fieldId + ']');
                }
                
                idMap[fieldId] = fieldName;
            }

            fields.Sort(Compare);

            Descriptor desc = new Descriptor(fields);

            _types[type] = desc;
        }

        /// <summary>
        /// Gets the descriptor for an object.
        /// </summary>
        private Descriptor GetDescriptor(object obj)
        {
            var type = obj.GetType();

            Descriptor desc;

            if (!_types.TryGetValue(type, out desc))
                throw new PortableException("Type is not registered in serializer: " + type.Name);

            return desc;
        }
        
        /// <summary>
        /// Compare two FieldInfo instances. 
        /// </summary>
        private static int Compare(FieldInfo info1, FieldInfo info2) {
            string name1 = PortableUtils.CleanFieldName(info1.Name);
            string name2 = PortableUtils.CleanFieldName(info2.Name);

            return string.Compare(name1, name2, StringComparison.OrdinalIgnoreCase);
        }

        /// <summary>
        /// Type descriptor. 
        /// </summary>
        private class Descriptor
        {
            /** Write actions to be performed. */
            private readonly List<PortableReflectiveWriteAction> _wActions;

            /** Read actions to be performed. */
            private readonly List<PortableReflectiveReadAction> _rActions;

            /// <summary>
            /// Constructor.
            /// </summary>
            /// <param name="fields">Fields.</param>
            public Descriptor(List<FieldInfo> fields)
            {
                _wActions = new List<PortableReflectiveWriteAction>(fields.Count);
                _rActions = new List<PortableReflectiveReadAction>(fields.Count);

                foreach (FieldInfo field in fields)
                {
                    PortableReflectiveWriteAction writeAction;
                    PortableReflectiveReadAction readAction;

                    PortableReflectiveActions.TypeActions(field, out writeAction, out readAction);

                    _wActions.Add(writeAction);
                    _rActions.Add(readAction);
                }
            }

            /// <summary>
            /// Write object.
            /// </summary>
            /// <param name="obj">Object.</param>
            /// <param name="writer">Portable writer.</param>
            public void Write(object obj, IPortableWriter writer)
            {
                int cnt = _wActions.Count;

                for (int i = 0; i < cnt; i++)
                    _wActions[i](obj, writer);                   
            }

            /// <summary>
            /// Read object.
            /// </summary>
            /// <param name="obj">Object.</param>
            /// <param name="reader">Portable reader.</param>
            public void Read(object obj, IPortableReader reader)
            {
                int cnt = _rActions.Count;

                for (int i = 0; i < cnt; i++ )
                    _rActions[i](obj, reader);
            }
        }
    }
}
