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
    using System.IO;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Impl.Binary.IO;
    using Apache.Ignite.Core.Impl.Common;

    /// <summary>
    /// Binary implementation.
    /// </summary>
    internal class Binary : IBinary
    {
        /** Owning grid. */
        private readonly Marshaller _marsh;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="marsh">Marshaller.</param>
        internal Binary(Marshaller marsh)
        {
            _marsh = marsh;
        }

        /** <inheritDoc /> */
        public T ToBinary<T>(object obj)
        {
            if (obj is IBinaryObject)
                return (T)obj;

            using (var stream = new BinaryHeapStream(1024))
            {
                // Serialize.
                BinaryWriter writer = _marsh.StartMarshal(stream);

                try
                {
                    writer.Write(obj);
                }
                finally
                {
                    // Save metadata.
                    _marsh.FinishMarshal(writer);
                }

                // Deserialize.
                stream.Seek(0, SeekOrigin.Begin);

                return _marsh.Unmarshal<T>(stream, BinaryMode.ForceBinary);
            }
        }

        /** <inheritDoc /> */
        public IBinaryObjectBuilder GetBuilder(Type type)
        {
            IgniteArgumentCheck.NotNull(type, "type");

            IBinaryTypeDescriptor desc = _marsh.GetDescriptor(type);

            if (desc == null)
                throw new IgniteException("Type is not binary (add it to BinaryConfiguration): " + 
                    type.FullName);

            return Builder0(null, null, desc);
        }

        /** <inheritDoc /> */
        public IBinaryObjectBuilder GetBuilder(string typeName)
        {
            IgniteArgumentCheck.NotNullOrEmpty(typeName, "typeName");

            IBinaryTypeDescriptor desc = _marsh.GetDescriptor(typeName);
            
            return Builder0(null, null, desc);
        }

        /** <inheritDoc /> */
        public IBinaryObjectBuilder GetBuilder(IBinaryObject obj)
        {
            IgniteArgumentCheck.NotNull(obj, "obj");

            BinaryObject obj0 = obj as BinaryObject;

            if (obj0 == null)
                throw new ArgumentException("Unsupported object type: " + obj.GetType());

            IBinaryTypeDescriptor desc = _marsh.GetDescriptor(true, obj0.TypeId);
            
            return Builder0(null, obj0, desc);
        }

        /** <inheritDoc /> */
        public int GetTypeId(string typeName)
        {
            IgniteArgumentCheck.NotNullOrEmpty(typeName, "typeName");

            return Marshaller.GetDescriptor(typeName).TypeId;
        }

        /** <inheritDoc /> */
        public ICollection<IBinaryType> GetBinaryTypes()
        {
            return Marshaller.Ignite.BinaryProcessor.GetBinaryTypes();
        }

        /** <inheritDoc /> */
        public IBinaryType GetBinaryType(int typeId)
        {
            return Marshaller.GetBinaryType(typeId);
        }

        /** <inheritDoc /> */
        public IBinaryType GetBinaryType(string typeName)
        {
            IgniteArgumentCheck.NotNullOrEmpty(typeName, "typeName");

            return GetBinaryType(GetTypeId(typeName));
        }

        /** <inheritDoc /> */
        public IBinaryType GetBinaryType(Type type)
        {
            IgniteArgumentCheck.NotNull(type, "type");

            var desc = Marshaller.GetDescriptor(type);

            return desc == null ? null : Marshaller.GetBinaryType(desc.TypeId);
        }

        /** <inheritDoc /> */
        public IBinaryObject BuildEnum(string typeName, int value)
        {
            IgniteArgumentCheck.NotNullOrEmpty(typeName, "typeName");

            var desc = Marshaller.GetDescriptor(typeName);

            IgniteArgumentCheck.Ensure(desc.IsEnum, "typeName", "Type should be an Enum.");

            _marsh.PutBinaryType(desc);

            return new BinaryEnum(desc.TypeId, value, Marshaller);
        }

        /** <inheritDoc /> */
        public IBinaryObject BuildEnum(Type type, int value)
        {
            IgniteArgumentCheck.NotNull(type, "type");
            IgniteArgumentCheck.Ensure(BinaryUtils.IsIgniteEnum(type), "type", 
                "Type should be an Enum.");
            
            var desc = Marshaller.GetDescriptor(type);

            IgniteArgumentCheck.Ensure(desc.IsEnum, "typeName", "Type should be an Enum.");

            _marsh.PutBinaryType(desc);

            return new BinaryEnum(desc.TypeId, value, Marshaller);
        }

        /** <inheritDoc /> */
        public IBinaryObject BuildEnum(string typeName, string valueName)
        {
            IgniteArgumentCheck.NotNullOrEmpty(typeName, "typeName");
            IgniteArgumentCheck.NotNullOrEmpty(valueName, "valueName");

            var desc = Marshaller.GetDescriptor(typeName);

            IgniteArgumentCheck.Ensure(desc.IsEnum, "typeName", "Type should be an Enum.");

            _marsh.PutBinaryType(desc);

            var value = GetEnumValueAsInt(typeName, valueName, desc);

            return new BinaryEnum(desc.TypeId, value, Marshaller);
        }

        /** <inheritDoc /> */
        public IBinaryObject BuildEnum(Type type, string valueName)
        {
            IgniteArgumentCheck.NotNull(type, "type");
            IgniteArgumentCheck.NotNullOrEmpty(valueName, "valueName");

            var desc = Marshaller.GetDescriptor(type);

            IgniteArgumentCheck.Ensure(desc.IsEnum, "typeName", "Type should be an Enum.");

            _marsh.PutBinaryType(desc);

            var value = GetEnumValueAsInt(type.ToString(), valueName, desc);

            return new BinaryEnum(desc.TypeId, value, Marshaller);
        }

        /** <inheritDoc /> */
        public IBinaryType RegisterEnum(string typeName, IEnumerable<KeyValuePair<string, int>> values)
        {
            IgniteArgumentCheck.NotNullOrEmpty(typeName, "typeName");

            return Marshaller.Ignite.BinaryProcessor.RegisterEnum(typeName, values);
        }

        /// <summary>
        /// Marshaller.
        /// </summary>
        internal Marshaller Marshaller
        {
            get { return _marsh; }
        }

        /// <summary>
        /// Internal builder creation routine.
        /// </summary>
        /// <param name="parent">Parent builder.</param>
        /// <param name="obj">binary object.</param>
        /// <param name="desc">Type descriptor.</param>
        /// <returns>Builder.</returns>
        private BinaryObjectBuilder Builder0(BinaryObjectBuilder parent, BinaryObject obj, 
            IBinaryTypeDescriptor desc)
        {
            return new BinaryObjectBuilder(this, parent, obj, desc);
        }

        /// <summary>
        /// Gets the enum value as int.
        /// </summary>
        private int GetEnumValueAsInt(string typeName, string valueName, IBinaryTypeDescriptor desc)
        {
            var type = Marshaller.GetBinaryType(desc.TypeId);

            var value = type.GetEnumValue(valueName);

            IgniteArgumentCheck.Ensure(value != null, "valueName",
                string.Format("Enum '{0}' does not have a value {1}", typeName, valueName));

            Debug.Assert(value.HasValue);

            return value.Value;
        }
    }
}
