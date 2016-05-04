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

namespace Apache.Ignite.Core.Binary
{
    using System;
    using System.Collections.Generic;
    using System.Reflection;
    using Apache.Ignite.Core.Impl.Binary;

    /// <summary>
    /// Binary serializer which reflectively writes all fields except of ones with 
    /// <see cref="System.NonSerializedAttribute"/>.
    /// <para />
    /// Note that Java platform stores dates as a difference between current time 
    /// and predefined absolute UTC date. Therefore, this difference is always the 
    /// same for all time zones. .NET, in contrast, stores dates as a difference 
    /// between current time and some predefined date relative to the current time 
    /// zone. It means that this difference will be different as you change time zones. 
    /// To overcome this discrepancy Ignite always converts .Net date to UTC form 
    /// before serializing and allows user to decide whether to deserialize them 
    /// in UTC or local form using <c>ReadTimestamp(..., true/false)</c> methods in 
    /// <see cref="IBinaryReader"/> and <see cref="IBinaryRawReader"/>.
    /// This serializer always read dates in UTC form. It means that if you have
    /// local date in any field/property, it will be implicitly converted to UTC
    /// form after the first serialization-deserialization cycle. 
    /// </summary>
    public sealed class BinaryReflectiveSerializer : IBinarySerializer
    {
        /** Cached binding flags. */
        private const BindingFlags Flags = BindingFlags.Instance | BindingFlags.Public | 
            BindingFlags.NonPublic | BindingFlags.DeclaredOnly;

        /** Cached type descriptors. */
        private readonly IDictionary<Type, Descriptor> _types = new Dictionary<Type, Descriptor>();

        /** Raw mode flag. */
        private bool _rawMode;

        /// <summary>
        /// Gets or value indicating whether raw mode serialization should be used.
        /// <para />
        /// Raw mode does not include field names, improving performance and memory usage.
        /// However, queries do not support raw objects.
        /// </summary>
        public bool RawMode
        {
            get { return _rawMode; }
            set
            {
                if (_types.Count > 0)
                    throw new InvalidOperationException(typeof (BinarizableSerializer).Name +
                        ".RawMode cannot be changed after first serialization.");

                _rawMode = value;
            }
        }

        /// <summary>
        /// Write portalbe object.
        /// </summary>
        /// <param name="obj">Object.</param>
        /// <param name="writer">Writer.</param>
        /// <exception cref="BinaryObjectException">Type is not registered in serializer:  + type.Name</exception>
        public void WriteBinary(object obj, IBinaryWriter writer)
        {
            var binarizable = obj as IBinarizable;

            if (binarizable != null)
                binarizable.WriteBinary(writer);
            else
                GetDescriptor(obj).Write(obj, writer);
        }

        /// <summary>
        /// Read binary object.
        /// </summary>
        /// <param name="obj">Instantiated empty object.</param>
        /// <param name="reader">Reader.</param>
        /// <exception cref="BinaryObjectException">Type is not registered in serializer:  + type.Name</exception>
        public void ReadBinary(object obj, IBinaryReader reader)
        {
            var binarizable = obj as IBinarizable;
            
            if (binarizable != null)
                binarizable.ReadBinary(reader);
            else
                GetDescriptor(obj).Read(obj, reader);
        }

        /// <summary>Register type.</summary>
        /// <param name="type">Type.</param>
        /// <param name="typeId">Type ID.</param>
        /// <param name="converter">Name converter.</param>
        /// <param name="idMapper">ID mapper.</param>
        internal void Register(Type type, int typeId, IBinaryNameMapper converter,
            IBinaryIdMapper idMapper)
        {
            if (type.GetInterface(typeof(IBinarizable).Name) != null)
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
                string fieldName = BinaryUtils.CleanFieldName(field.Name);

                int fieldId = BinaryUtils.FieldId(typeId, fieldName, converter, idMapper);

                if (idMap.ContainsKey(fieldId))
                {
                    throw new BinaryObjectException("Conflicting field IDs [type=" +
                        type.Name + ", field1=" + idMap[fieldId] + ", field2=" + fieldName +
                        ", fieldId=" + fieldId + ']');
                }
                
                idMap[fieldId] = fieldName;
            }

            fields.Sort(Compare);

            Descriptor desc = new Descriptor(fields, _rawMode);

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
                throw new BinaryObjectException("Type is not registered in serializer: " + type.Name);

            return desc;
        }
        
        /// <summary>
        /// Compare two FieldInfo instances. 
        /// </summary>
        private static int Compare(FieldInfo info1, FieldInfo info2) {
            string name1 = BinaryUtils.CleanFieldName(info1.Name);
            string name2 = BinaryUtils.CleanFieldName(info2.Name);

            return string.Compare(name1, name2, StringComparison.OrdinalIgnoreCase);
        }

        /// <summary>
        /// Type descriptor. 
        /// </summary>
        private class Descriptor
        {
            /** Write actions to be performed. */
            private readonly List<BinaryReflectiveWriteAction> _wActions;

            /** Read actions to be performed. */
            private readonly List<BinaryReflectiveReadAction> _rActions;

            /// <summary>
            /// Constructor.
            /// </summary>
            /// <param name="fields">Fields.</param>
            /// <param name="raw">Raw mode.</param>
            public Descriptor(List<FieldInfo> fields, bool raw)
            {
                _wActions = new List<BinaryReflectiveWriteAction>(fields.Count);
                _rActions = new List<BinaryReflectiveReadAction>(fields.Count);

                foreach (FieldInfo field in fields)
                {
                    BinaryReflectiveWriteAction writeAction;
                    BinaryReflectiveReadAction readAction;

                    BinaryReflectiveActions.GetTypeActions(field, out writeAction, out readAction, raw);

                    _wActions.Add(writeAction);
                    _rActions.Add(readAction);
                }
            }

            /// <summary>
            /// Write object.
            /// </summary>
            /// <param name="obj">Object.</param>
            /// <param name="writer">Writer.</param>
            public void Write(object obj, IBinaryWriter writer)
            {
                int cnt = _wActions.Count;

                for (int i = 0; i < cnt; i++)
                    _wActions[i](obj, writer);                   
            }

            /// <summary>
            /// Read object.
            /// </summary>
            /// <param name="obj">Object.</param>
            /// <param name="reader">Reader.</param>
            public void Read(object obj, IBinaryReader reader)
            {
                int cnt = _rActions.Count;

                for (int i = 0; i < cnt; i++ )
                    _rActions[i](obj, reader);
            }
        }
    }
}
