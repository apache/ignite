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
    using System.Diagnostics;
    using System.Reflection;
    using System.Runtime.Serialization;
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
        /** Raw mode flag. */
        private bool _rawMode;

        /** In use flag. */
        private bool _isInUse;

        /** <inheritdoc /> */
        public void WriteBinary(object obj, IBinaryWriter writer)
        {
            throw new NotSupportedException(GetType() + ".WriteBinary should not be called directly.");
        }

        /** <inheritdoc /> */
        public void ReadBinary(object obj, IBinaryReader reader)
        {
            throw new NotSupportedException(GetType() + ".ReadBinary should not be called directly.");
        }

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
                if (_isInUse)
                    throw new InvalidOperationException(typeof(BinarizableSerializer).Name +
                        ".RawMode cannot be changed after first serialization.");

                _rawMode = value;
            }
        }

        internal IBinarySerializerInternal Register(Type type, int typeId, IBinaryNameMapper converter,
            IBinaryIdMapper idMapper)
        {
            _isInUse = true;

            return new BinaryReflectiveSerializerInternal(_rawMode).Register(type, typeId, converter, idMapper);
        }

    }


    /// <summary>
    /// Internal reflective serializer.
    /// </summary>
    internal sealed class BinaryReflectiveSerializerInternal : IBinarySerializerInternal
    {
        /** Cached binding flags. */
        private const BindingFlags Flags = BindingFlags.Instance | BindingFlags.Public |
            BindingFlags.NonPublic | BindingFlags.DeclaredOnly;

        /** Raw mode flag. */
        private readonly bool _rawMode;

        /** Write actions to be performed. */
        private readonly BinaryReflectiveWriteAction[] _wActions;

        /** Read actions to be performed. */
        private readonly BinaryReflectiveReadAction[] _rActions;

        /// <summary>
        /// Initializes a new instance of the <see cref="BinaryReflectiveSerializer"/> class.
        /// </summary>
        public BinaryReflectiveSerializerInternal(bool raw)
        {
            _rawMode = raw;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="BinaryReflectiveSerializer"/> class.
        /// </summary>
        private BinaryReflectiveSerializerInternal(BinaryReflectiveWriteAction[] wActions, BinaryReflectiveReadAction[] rActions, bool raw)
        {
            Debug.Assert(wActions != null);
            Debug.Assert(rActions != null);

            _wActions = wActions;
            _rActions = rActions;
            _rawMode = raw;
        }

        /** <inheritdoc /> */
        void IBinarySerializerInternal.WriteBinary<T>(T obj, BinaryWriter writer)
        {
            Debug.Assert(_wActions != null);

            foreach (var action in _wActions)
                action(obj, writer);
        }

        /** <inheritdoc /> */
        T IBinarySerializerInternal.ReadBinary<T>(BinaryReader reader, Type type, Action<int, object> addHandle)
        {
            Debug.Assert(_rActions != null);

            var obj = FormatterServices.GetUninitializedObject(type);

            foreach (var action in _rActions)
                action(obj, reader);

            return (T)obj;
        }

        /** <inheritdoc /> */
        bool IBinarySerializerInternal.SupportsHandles
        {
            get { return true; }
        }

        /// <summary>Register type.</summary>
        /// <param name="type">Type.</param>
        /// <param name="typeId">Type ID.</param>
        /// <param name="converter">Name converter.</param>
        /// <param name="idMapper">ID mapper.</param>
        internal BinaryReflectiveSerializerInternal Register(Type type, int typeId, IBinaryNameMapper converter,
            IBinaryIdMapper idMapper)
        {
            Debug.Assert(_wActions == null && _rActions == null);

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

            var wActions = new BinaryReflectiveWriteAction[fields.Count];
            var rActions = new BinaryReflectiveReadAction[fields.Count];

            for (int i = 0; i < fields.Count; i++)
            {
                BinaryReflectiveWriteAction writeAction;
                BinaryReflectiveReadAction readAction;

                BinaryReflectiveActions.GetTypeActions(fields[i], out writeAction, out readAction, _rawMode);

                wActions[i] = writeAction;
                rActions[i] = readAction;
            }

            return new BinaryReflectiveSerializerInternal(wActions, rActions, _rawMode);
        }

        /// <summary>
        /// Compare two FieldInfo instances. 
        /// </summary>
        private static int Compare(FieldInfo info1, FieldInfo info2)
        {
            string name1 = BinaryUtils.CleanFieldName(info1.Name);
            string name2 = BinaryUtils.CleanFieldName(info2.Name);

            return string.Compare(name1, name2, StringComparison.OrdinalIgnoreCase);
        }
    }
}
