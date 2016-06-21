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
    using System.Diagnostics;
    using System.Reflection;
    using System.Runtime.Serialization;
    using Apache.Ignite.Core.Binary;

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
        T IBinarySerializerInternal.ReadBinary<T>(BinaryReader reader, Type type, int pos)
        {
            Debug.Assert(_rActions != null);

            var obj = FormatterServices.GetUninitializedObject(type);

            reader.AddHandle(pos, obj);

            foreach (var action in _rActions)
                action(obj, reader);

            return (T) obj;
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