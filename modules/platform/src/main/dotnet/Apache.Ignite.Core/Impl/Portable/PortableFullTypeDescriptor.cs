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
    using Apache.Ignite.Core.Portable;

    /// <summary>
    /// Full type descriptor.
    /// </summary> 
    public class PortableFullTypeDescriptor : IPortableTypeDescriptor
    {
        /** Type. */
        private readonly Type type;

        /** Type ID. */
        private readonly int typeId;

        /** Type name. */
        private readonly string typeName;

        /** User type flag. */
        private readonly bool userType;

        /** Name converter. */
        private readonly IPortableNameMapper nameConverter;

        /** Mapper. */
        private readonly IPortableIdMapper mapper;

        /** Serializer. */
        private readonly IPortableSerializer serializer;

        /** Metadata enabled flag. */
        private readonly bool metaEnabled;

        /** Whether to cache deserialized value in IPortableObject */
        private readonly bool keepDeserialized;

        /** Affinity field key name. */
        private readonly string affKeyFieldName;

        /** Typed handler. */
        private readonly object typedHandler;

        /** Untyped handler. */
        private readonly PortableSystemWriteDelegate untypedHandler;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="type">Type.</param>
        /// <param name="typeId">Type ID.</param>
        /// <param name="typeName">Type name.</param>
        /// <param name="userType">User type flag.</param>
        /// <param name="nameConverter">Name converter.</param>
        /// <param name="mapper">Mapper.</param>
        /// <param name="serializer">Serializer.</param>
        /// <param name="metaEnabled">Metadata enabled flag.</param>
        /// <param name="keepDeserialized">Whether to cache deserialized value in IPortableObject</param>
        /// <param name="affKeyFieldName">Affinity field key name.</param>
        /// <param name="typedHandler">Typed handler.</param>
        /// <param name="untypedHandler">Untyped handler.</param>
        public PortableFullTypeDescriptor(
            Type type, 
            int typeId, 
            string typeName, 
            bool userType, 
            IPortableNameMapper nameConverter, 
            IPortableIdMapper mapper, 
            IPortableSerializer serializer, 
            bool metaEnabled, 
            bool keepDeserialized, 
            string affKeyFieldName, 
            object typedHandler,
            PortableSystemWriteDelegate untypedHandler)
        {
            this.type = type;
            this.typeId = typeId;
            this.typeName = typeName;
            this.userType = userType;
            this.nameConverter = nameConverter;
            this.mapper = mapper;
            this.serializer = serializer;
            this.metaEnabled = metaEnabled;
            this.keepDeserialized = keepDeserialized;
            this.affKeyFieldName = affKeyFieldName;
            this.typedHandler = typedHandler;
            this.untypedHandler = untypedHandler;
        }

        /// <summary>
        /// Type.
        /// </summary>
        public Type Type
        {
            get { return type; }
        }

        /// <summary>
        /// Type ID.
        /// </summary>
        public int TypeId
        {
            get { return typeId; }
        }

        /// <summary>
        /// Type name.
        /// </summary>
        public string TypeName
        {
            get { return typeName; }
        }

        /// <summary>
        /// User type flag.
        /// </summary>
        public bool UserType
        {
            get { return userType; }
        }

        /// <summary>
        /// Metadata enabled flag.
        /// </summary>
        public bool MetadataEnabled
        {
            get { return metaEnabled; }
        }

        /// <summary>
        /// Whether to cache deserialized value in IPortableObject
        /// </summary>
        public bool KeepDeserialized
        {
            get { return keepDeserialized; }
        }

        /// <summary>
        /// Name converter.
        /// </summary>
        public IPortableNameMapper NameConverter
        {
            get { return nameConverter; }
        }

        /// <summary>
        /// Mapper.
        /// </summary>
        public IPortableIdMapper Mapper
        {
            get { return mapper; }
        }

        /// <summary>
        /// Serializer.
        /// </summary>
        public IPortableSerializer Serializer
        {
            get { return serializer; }
        }

        /// <summary>
        /// Affinity key field name.
        /// </summary>
        public string AffinityKeyFieldName
        {
            get { return affKeyFieldName; }
        }

        /// <summary>
        /// Typed handler.
        /// </summary>
        public object TypedHandler
        {
            get { return typedHandler; }
        }

        /// <summary>
        /// Untyped handler.
        /// </summary>
        public PortableSystemWriteDelegate UntypedHandler
        {
            get { return untypedHandler; }
        }
    }
}
