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
    using System.Linq;
    using System.Reflection;
    using System.Runtime.Serialization;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Impl.Common;

    /// <summary>
    /// Internal reflective serializer.
    /// </summary>
    internal sealed class BinaryReflectiveSerializerInternal : IBinarySerializerInternal
    {
        /** Raw mode flag. */
        private readonly bool _rawMode;

        /** Write actions to be performed. */
        private readonly BinaryReflectiveWriteAction[] _wActions;

        /** Read actions to be performed. */
        private readonly BinaryReflectiveReadAction[] _rActions;

        /** Callback type descriptor. */
        private readonly SerializableTypeDescriptor _serializableDescriptor;

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
        private BinaryReflectiveSerializerInternal(BinaryReflectiveWriteAction[] wActions, 
            BinaryReflectiveReadAction[] rActions, bool raw, SerializableTypeDescriptor serializableDescriptor)
        {
            Debug.Assert(wActions != null);
            Debug.Assert(rActions != null);
            Debug.Assert(serializableDescriptor != null);

            _wActions = wActions;
            _rActions = rActions;
            _rawMode = raw;
            _serializableDescriptor = serializableDescriptor;
        }

        /** <inheritdoc /> */
        void IBinarySerializerInternal.WriteBinary<T>(T obj, BinaryWriter writer)
        {
            Debug.Assert(_wActions != null);
            Debug.Assert(writer != null);

            var ctx = GetStreamingContext(writer);

            _serializableDescriptor.OnSerializing(obj, ctx);

            foreach (var action in _wActions)
                action(obj, writer);

            _serializableDescriptor.OnSerialized(obj, ctx);
        }

        /** <inheritdoc /> */
        T IBinarySerializerInternal.ReadBinary<T>(BinaryReader reader, IBinaryTypeDescriptor desc, int pos,
            Type typeOverride)
        {
            Debug.Assert(_rActions != null);
            Debug.Assert(reader != null);
            Debug.Assert(desc != null);

            var obj = FormatterServices.GetUninitializedObject(typeOverride ?? desc.Type);

            var ctx = GetStreamingContext(reader);

            _serializableDescriptor.OnDeserializing(obj, ctx);

            DeserializationCallbackProcessor.Push(obj);

            try
            {
                reader.AddHandle(pos, obj);

                foreach (var action in _rActions)
                    action(obj, reader);

                _serializableDescriptor.OnDeserialized(obj, ctx);
                
                DeserializationCallbackProcessor.Pop();
            }
            catch (Exception)
            {
                // Clear callbacks on exception to avoid dangling objects.
                DeserializationCallbackProcessor.Clear();
                throw;
            }

            return (T) obj;
        }

        /** <inheritdoc /> */
        bool IBinarySerializerInternal.SupportsHandles
        {
            get { return true; }
        }

        /// <summary>
        /// Register type.
        /// </summary>
        /// <param name="type">Type.</param>
        /// <param name="typeId">Type ID.</param>
        /// <param name="converter">Name converter.</param>
        /// <param name="idMapper">ID mapper.</param>
        /// <param name="forceTimestamp">Force timestamp serialization for DateTime fields..</param>
        /// <returns>Resulting serializer.</returns>
        internal BinaryReflectiveSerializerInternal Register(Type type, int typeId, IBinaryNameMapper converter,
            IBinaryIdMapper idMapper, bool forceTimestamp)
        {
            Debug.Assert(_wActions == null && _rActions == null);

            var fields = ReflectionUtils.GetAllFields(type).Where(x => !x.IsNotSerialized).ToList();

            IDictionary<int, string> idMap = new Dictionary<int, string>();

            foreach (FieldInfo field in fields)
            {
                string fieldName = BinaryUtils.CleanFieldName(field.Name);

                int fieldId = BinaryUtils.FieldId(typeId, fieldName, converter, idMapper);

                if (idMap.ContainsKey(fieldId))
                {
                    if (fieldName == idMap[fieldId])
                    {
                        string baseClassName = field.DeclaringType != null ? field.DeclaringType.Name : null;
                        throw new BinaryObjectException(string.Format(
                            "{0} derives from {1} and hides field {2} from the base class. " +
                            "Ignite can not serialize two fields with the same name.", type.Name, baseClassName, fieldName));
                    }

                    throw new BinaryObjectException(string.Format(
                        "Conflicting field IDs [type={0}, field1={1}, field2={2}, fieldId={3}])",
                        type.Name, idMap[fieldId], fieldName, fieldId));
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

                BinaryReflectiveActions.GetTypeActions(fields[i], out writeAction, out readAction, _rawMode, forceTimestamp);

                wActions[i] = writeAction;
                rActions[i] = readAction;
            }

            var serDesc = SerializableTypeDescriptor.Get(type);

            return new BinaryReflectiveSerializerInternal(wActions, rActions, _rawMode, serDesc);
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
    }
}