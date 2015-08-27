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
    using System.Diagnostics;
    using System.Linq;
    using Apache.Ignite.Core;
    using Apache.Ignite.Core.Impl.Portable.IO;
    using Apache.Ignite.Core.Impl.Portable.Metadata;
    using Apache.Ignite.Core.Portable;

    /// <summary>
    /// Portable marshaller implementation.
    /// </summary>
    public class PortableMarshaller
    {
        /** Portable configuration. */
        private readonly PortableConfiguration cfg;

        /** Ignite context. */
        private readonly IIgniteContext igniteContext;

        /** Type to descriptor map. */
        private readonly IDictionary<Type, IPortableTypeDescriptor> typeToDesc =
            new Dictionary<Type, IPortableTypeDescriptor>();

        /** Type name to descriptor map. */
        private readonly IDictionary<string, IPortableTypeDescriptor> typeNameToDesc =
            new Dictionary<string, IPortableTypeDescriptor>();

        /** ID to descriptor map. */
        private readonly IDictionary<long, IPortableTypeDescriptor> idToDesc =
            new Dictionary<long, IPortableTypeDescriptor>();

        /** Cached metadatas. */
        private volatile IDictionary<int, PortableMetadataHolder> metas =
            new Dictionary<int, PortableMetadataHolder>();

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="cfg">Configurtaion.</param>
        /// <param name="igniteContext">The context.</param>
        public PortableMarshaller(PortableConfiguration cfg, IIgniteContext igniteContext)
        {
            Debug.Assert(igniteContext != null);

            this.igniteContext = igniteContext;

            // Validation.
            if (cfg == null)
                cfg = new PortableConfiguration();

            if (cfg.TypeConfigurations == null)
                cfg.TypeConfigurations = new List<PortableTypeConfiguration>();

            foreach (PortableTypeConfiguration typeCfg in cfg.TypeConfigurations)
            {
                if (string.IsNullOrEmpty(typeCfg.TypeName))
                    throw igniteContext.ConvertException(new PortableException("Type name cannot be null or empty: " + typeCfg));

                if (typeCfg.AssemblyName != null && typeCfg.AssemblyName.Length == 0)
                    throw igniteContext.ConvertException(new PortableException("Assembly name cannot be empty string: " + typeCfg));
            }

            // Define predefined types.
            AddPredefinedType(typeof(bool), PortableUtils.TYPE_BOOL, PortableSystemHandlers.WRITE_HND_BOOL_TYPED, PortableSystemHandlers.WRITE_HND_BOOL);
            AddPredefinedType(typeof(byte), PortableUtils.TYPE_BYTE, PortableSystemHandlers.WRITE_HND_BYTE_TYPED, PortableSystemHandlers.WRITE_HND_BYTE);
            AddPredefinedType(typeof(short), PortableUtils.TYPE_SHORT, PortableSystemHandlers.WRITE_HND_SHORT_TYPED, PortableSystemHandlers.WRITE_HND_SHORT);
            AddPredefinedType(typeof(char), PortableUtils.TYPE_CHAR, PortableSystemHandlers.WRITE_HND_CHAR_TYPED, PortableSystemHandlers.WRITE_HND_CHAR);
            AddPredefinedType(typeof(int), PortableUtils.TYPE_INT, PortableSystemHandlers.WRITE_HND_INT_TYPED, PortableSystemHandlers.WRITE_HND_INT);
            AddPredefinedType(typeof(long), PortableUtils.TYPE_LONG, PortableSystemHandlers.WRITE_HND_LONG_TYPED, PortableSystemHandlers.WRITE_HND_LONG);
            AddPredefinedType(typeof(float), PortableUtils.TYPE_FLOAT, PortableSystemHandlers.WRITE_HND_FLOAT_TYPED, PortableSystemHandlers.WRITE_HND_FLOAT);
            AddPredefinedType(typeof(double), PortableUtils.TYPE_DOUBLE, PortableSystemHandlers.WRITE_HND_DOUBLE_TYPED, PortableSystemHandlers.WRITE_HND_DOUBLE);
            AddPredefinedType(typeof(string), PortableUtils.TYPE_STRING, PortableSystemHandlers.WRITE_HND_STRING_TYPED, PortableSystemHandlers.WRITE_HND_STRING);
            AddPredefinedType(typeof(decimal), PortableUtils.TYPE_DECIMAL, PortableSystemHandlers.WRITE_HND_DECIMAL_TYPED, PortableSystemHandlers.WRITE_HND_DECIMAL);
            AddPredefinedType(typeof(DateTime), PortableUtils.TYPE_DATE, PortableSystemHandlers.WRITE_HND_DATE_TYPED, PortableSystemHandlers.WRITE_HND_DATE);
            AddPredefinedType(typeof(Guid), PortableUtils.TYPE_GUID, PortableSystemHandlers.WRITE_HND_GUID_TYPED, PortableSystemHandlers.WRITE_HND_GUID);

            AddPredefinedType(typeof(PortableUserObject), PortableUtils.TYPE_PORTABLE, PortableSystemHandlers.WRITE_HND_PORTABLE_TYPED, 
                PortableSystemHandlers.WRITE_HND_PORTABLE);

            AddPredefinedType(typeof(bool[]), PortableUtils.TYPE_ARRAY_BOOL, PortableSystemHandlers.WRITE_HND_BOOL_ARRAY_TYPED,
                PortableSystemHandlers.WRITE_HND_BOOL_ARRAY);
            AddPredefinedType(typeof(byte[]), PortableUtils.TYPE_ARRAY_BYTE, PortableSystemHandlers.WRITE_HND_BYTE_ARRAY_TYPED,
                PortableSystemHandlers.WRITE_HND_BYTE_ARRAY);
            AddPredefinedType(typeof(short[]), PortableUtils.TYPE_ARRAY_SHORT, PortableSystemHandlers.WRITE_HND_SHORT_ARRAY_TYPED,
                PortableSystemHandlers.WRITE_HND_SHORT_ARRAY);
            AddPredefinedType(typeof(char[]), PortableUtils.TYPE_ARRAY_CHAR, PortableSystemHandlers.WRITE_HND_CHAR_ARRAY_TYPED,
                PortableSystemHandlers.WRITE_HND_CHAR_ARRAY);
            AddPredefinedType(typeof(int[]), PortableUtils.TYPE_ARRAY_INT, PortableSystemHandlers.WRITE_HND_INT_ARRAY_TYPED,
                PortableSystemHandlers.WRITE_HND_INT_ARRAY);
            AddPredefinedType(typeof(long[]), PortableUtils.TYPE_ARRAY_LONG, PortableSystemHandlers.WRITE_HND_LONG_ARRAY_TYPED,
                PortableSystemHandlers.WRITE_HND_LONG_ARRAY);
            AddPredefinedType(typeof(float[]), PortableUtils.TYPE_ARRAY_FLOAT, PortableSystemHandlers.WRITE_HND_FLOAT_ARRAY_TYPED,
                PortableSystemHandlers.WRITE_HND_FLOAT_ARRAY);
            AddPredefinedType(typeof(double[]), PortableUtils.TYPE_ARRAY_DOUBLE, PortableSystemHandlers.WRITE_HND_DOUBLE_ARRAY_TYPED,
                PortableSystemHandlers.WRITE_HND_DOUBLE_ARRAY);
            AddPredefinedType(typeof(decimal[]), PortableUtils.TYPE_ARRAY_DECIMAL, PortableSystemHandlers.WRITE_HND_DECIMAL_ARRAY_TYPED,
                PortableSystemHandlers.WRITE_HND_DECIMAL_ARRAY);
            AddPredefinedType(typeof(string[]), PortableUtils.TYPE_ARRAY_STRING, PortableSystemHandlers.WRITE_HND_STRING_ARRAY_TYPED,
                PortableSystemHandlers.WRITE_HND_STRING_ARRAY);
            AddPredefinedType(typeof(DateTime?[]), PortableUtils.TYPE_ARRAY_DATE, PortableSystemHandlers.WRITE_HND_DATE_ARRAY_TYPED,
                PortableSystemHandlers.WRITE_HND_DATE_ARRAY);
            AddPredefinedType(typeof(Guid?[]), PortableUtils.TYPE_ARRAY_GUID, PortableSystemHandlers.WRITE_HND_GUID_ARRAY_TYPED,
                PortableSystemHandlers.WRITE_HND_GUID_ARRAY);

            // 2. Define user types.
            var dfltSerializer = cfg.DefaultSerializer == null ? new PortableReflectiveSerializer() : null;

            var typeResolver = new TypeResolver();

            ICollection<PortableTypeConfiguration> typeCfgs = cfg.TypeConfigurations;

            if (typeCfgs != null)
                foreach (PortableTypeConfiguration typeCfg in typeCfgs)
                    AddUserType(cfg, typeCfg, typeResolver, dfltSerializer);

            ICollection<string> types = cfg.Types;

            if (types != null)
                foreach (string type in types)
                    AddUserType(cfg, new PortableTypeConfiguration(type), typeResolver, dfltSerializer);

            if (cfg.DefaultSerializer == null)
                cfg.DefaultSerializer = dfltSerializer;

            this.cfg = cfg;
        }

        /// <summary>
        /// Marshal object.
        /// </summary>
        /// <param name="val">Value.</param>
        /// <returns>Serialized data as byte array.</returns>
        public byte[] Marshal(object val)
        {
            PortableHeapStream stream = new PortableHeapStream(128);

            Marshal(val, stream);

            return stream.ArrayCopy();
        }

        /// <summary>
        /// Marshal object.
        /// </summary>
        /// <param name="val">Value.</param>
        /// <param name="stream">Output stream.</param>
        /// <returns>Collection of metadatas (if any).</returns>
        private void Marshal<T>(T val, IPortableStream stream)
        {
            var writer = StartMarshal(stream);

            writer.Write(val);

            FinishMarshal(writer);
        }

        /// <summary>
        /// Start marshal session.
        /// </summary>
        /// <param name="stream">Stream.</param>
        /// <returns>Writer.</returns>
        public IPortableWriterEx StartMarshal(IPortableStream stream)
        {
            return new PortableWriterImpl(this, stream);
        }

        /// <summary>
        /// Finish marshal session.
        /// </summary>
        /// <param name="writer">Writer.</param>
        /// <returns>Dictionary with metadata.</returns>
        public virtual void FinishMarshal(IPortableWriterEx writer)
        {
            // No-op.
        }

        /// <summary>
        /// Unmarshal object.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="data">Data array.</param>
        /// <param name="keepPortable">Whether to keep portables as portables.</param>
        /// <returns>
        /// Object.
        /// </returns>
        public T Unmarshal<T>(byte[] data, bool keepPortable)
        {
            return Unmarshal<T>(new PortableHeapStream(data), keepPortable);
        }

        /// <summary>
        /// Unmarshal object.
        /// </summary>
        /// <param name="data">Data array.</param>
        /// <param name="mode">The mode.</param>
        /// <returns>
        /// Object.
        /// </returns>
        public T Unmarshal<T>(byte[] data, PortableMode mode = PortableMode.DESERIALIZE)
        {
            return Unmarshal<T>(new PortableHeapStream(data), mode);
        }

        /// <summary>
        /// Unmarshal object.
        /// </summary>
        /// <param name="stream">Stream over underlying byte array with correct position.</param>
        /// <param name="keepPortable">Whether to keep portables as portables.</param>
        /// <returns>
        /// Object.
        /// </returns>
        public T Unmarshal<T>(IPortableStream stream, bool keepPortable)
        {
            return Unmarshal<T>(stream, keepPortable ? PortableMode.KEEP_PORTABLE : PortableMode.DESERIALIZE, null);
        }

        /// <summary>
        /// Unmarshal object.
        /// </summary>
        /// <param name="stream">Stream over underlying byte array with correct position.</param>
        /// <param name="mode">The mode.</param>
        /// <returns>
        /// Object.
        /// </returns>
        public T Unmarshal<T>(IPortableStream stream, PortableMode mode = PortableMode.DESERIALIZE)
        {
            return Unmarshal<T>(stream, mode, null);
        }

        /// <summary>
        /// Unmarshal object.
        /// </summary>
        /// <param name="stream">Stream over underlying byte array with correct position.</param>
        /// <param name="mode">The mode.</param>
        /// <param name="builder">Builder.</param>
        /// <returns>
        /// Object.
        /// </returns>
        public T Unmarshal<T>(IPortableStream stream, PortableMode mode, PortableBuilderImpl builder)
        {
            return new PortableReaderImpl(this, idToDesc, stream, mode, builder).Deserialize<T>();
        }

        /// <summary>
        /// Start unmarshal session.
        /// </summary>
        /// <param name="stream">Stream.</param>
        /// <param name="keepPortable">Whether to keep portables as portables.</param>
        /// <returns>
        /// Reader.
        /// </returns>
        public IPortableReaderEx StartUnmarshal(IPortableStream stream, bool keepPortable)
        {
            return new PortableReaderImpl(this, idToDesc, stream,
                keepPortable ? PortableMode.KEEP_PORTABLE : PortableMode.DESERIALIZE, null);
        }

        /// <summary>
        /// Start unmarshal session.
        /// </summary>
        /// <param name="stream">Stream.</param>
        /// <param name="mode">The mode.</param>
        /// <returns>Reader.</returns>
        public IPortableReaderEx StartUnmarshal(IPortableStream stream, PortableMode mode = PortableMode.DESERIALIZE)
        {
            return new PortableReaderImpl(this, idToDesc, stream, mode, null);
        }
        
        /// <summary>
        /// Gets metadata for the given type ID.
        /// </summary>
        /// <param name="typeId">Type ID.</param>
        /// <returns>Metadata or null.</returns>
        public virtual IPortableMetadata Metadata(int typeId)
        {
            return PortableMetadataImpl.EMPTY_META;
        }

        /// <summary>
        /// Gets metadata handler for the given type ID.
        /// </summary>
        /// <param name="desc">Type descriptor.</param>
        /// <returns>Metadata handler.</returns>
        public IPortableMetadataHandler MetadataHandler(IPortableTypeDescriptor desc)
        {
            PortableMetadataHolder holder;

            if (!metas.TryGetValue(desc.TypeId, out holder))
            {
                lock (this)
                {
                    if (!metas.TryGetValue(desc.TypeId, out holder))
                    {
                        IDictionary<int, PortableMetadataHolder> metas0 =
                            new Dictionary<int, PortableMetadataHolder>(metas);

                        holder = desc.MetadataEnabled ? new PortableMetadataHolder(desc.TypeId,
                            desc.TypeName, desc.AffinityKeyFieldName) : null;

                        metas0[desc.TypeId] = holder;

                        metas = metas0;
                    }
                }
            }

            if (holder != null)
            {
                ICollection<int> ids = holder.FieldIds();

                bool newType = ids.Count == 0 && !holder.Saved();

                return new PortableHashsetMetadataHandler(ids, newType);
            }
            else
                return null;
        }

        /// <summary>
        /// Callback invoked when metadata has been sent to the server and acknowledged by it.
        /// </summary>
        /// <param name="newMetas"></param>
        public void OnMetadataSent(IDictionary<int, IPortableMetadata> newMetas)
        {
            foreach (KeyValuePair<int, IPortableMetadata> metaEntry in newMetas)
            {
                PortableMetadataImpl meta = (PortableMetadataImpl) metaEntry.Value;

                IDictionary<int, Tuple<string, int>> mergeInfo =
                    new Dictionary<int, Tuple<string, int>>(meta.FieldsMap().Count);

                foreach (KeyValuePair<string, int> fieldMeta in meta.FieldsMap())
                {
                    int fieldId = PortableUtils.FieldId(metaEntry.Key, fieldMeta.Key, null, null);

                    mergeInfo[fieldId] = new Tuple<string, int>(fieldMeta.Key, fieldMeta.Value);
                }

                metas[metaEntry.Key].Merge(mergeInfo);
            }
        }
        
        /// <summary>
        /// Gets descriptor for type.
        /// </summary>
        /// <param name="type">Type.</param>
        /// <returns>Descriptor.</returns>
        public IPortableTypeDescriptor Descriptor(Type type)
        {
            IPortableTypeDescriptor desc;

            typeToDesc.TryGetValue(type, out desc);

            return desc;
        }

        /// <summary>
        /// Gets descriptor for type name.
        /// </summary>
        /// <param name="typeName">Type name.</param>
        /// <returns>Descriptor.</returns>
        public IPortableTypeDescriptor Descriptor(string typeName)
        {
            IPortableTypeDescriptor desc;

            return typeNameToDesc.TryGetValue(typeName, out desc) ? desc : 
                new PortableSurrogateTypeDescriptor(cfg, typeName);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="userType"></param>
        /// <param name="typeId"></param>
        /// <returns></returns>
        public IPortableTypeDescriptor Descriptor(bool userType, int typeId)
        {
            IPortableTypeDescriptor desc;

            return idToDesc.TryGetValue(PortableUtils.TypeKey(userType, typeId), out desc) ? desc :
                userType ? new PortableSurrogateTypeDescriptor(cfg, typeId) : null;
        }

        /// <summary>
        /// Check whether the given object is portable, i.e. it can be 
        /// serialized with portable marshaller.
        /// </summary>
        /// <param name="obj">Object.</param>
        /// <returns>True if portable.</returns>
        public bool IsPortable(object obj)
        {
            if (obj != null)
            {
                // We assume object as portable only in case it has descriptor.
                // Collections, Enums and non-primitive arrays do not have descriptors
                // and this is fine here because we cannot know whether their members
                // are portable.
                return Descriptor(obj.GetType()) != null;
            }

            return true;
        }


        /// <summary>
        /// Add user type.
        /// </summary>
        /// <param name="cfg">Configuration.</param>
        /// <param name="typeCfg">Type configuration.</param>
        /// <param name="typeResolver">The type resolver.</param>
        /// <param name="dfltSerializer">The default serializer.</param>
        private void AddUserType(PortableConfiguration cfg, PortableTypeConfiguration typeCfg, 
            TypeResolver typeResolver, IPortableSerializer dfltSerializer)
        {
            // Get converter/mapper/serializer.
            IPortableNameMapper nameMapper = typeCfg.NameMapper ?? cfg.DefaultNameMapper;

            IPortableIdMapper idMapper = typeCfg.IdMapper ?? cfg.DefaultIdMapper;

            bool metaEnabled = typeCfg.MetadataEnabled ?? cfg.DefaultMetadataEnabled;

            bool keepDeserialized = typeCfg.KeepDeserialized ?? cfg.DefaultKeepDeserialized;

            // Try resolving type.
            Type type = typeResolver.ResolveType(typeCfg.TypeName, typeCfg.AssemblyName);

            if (type != null)
            {
                // Type is found.
                var typeName = GetTypeName(type);

                int typeId = PortableUtils.TypeId(typeName, nameMapper, idMapper);

                var serializer = typeCfg.Serializer ?? cfg.DefaultSerializer
                                 ?? GetPortableMarshalAwareSerializer(type) ?? dfltSerializer;

                var refSerializer = serializer as PortableReflectiveSerializer;

                if (refSerializer != null)
                    refSerializer.Register(type, typeId, nameMapper, idMapper);

                AddType(type, typeId, typeName, true, metaEnabled, keepDeserialized, nameMapper, idMapper, serializer,
                    typeCfg.AffinityKeyFieldName, null, null);
            }
            else
            {
                // Type is not found.
                string typeName = PortableUtils.SimpleTypeName(typeCfg.TypeName);

                int typeId = PortableUtils.TypeId(typeName, nameMapper, idMapper);

                AddType(null, typeId, typeName, true, metaEnabled, keepDeserialized, nameMapper, idMapper, null,
                    typeCfg.AffinityKeyFieldName, null, null);
            }
        }

        /// <summary>
        /// Gets the <see cref="PortableMarshalAwareSerializer"/> for a type if it is compatible.
        /// </summary>
        /// <param name="type">The type.</param>
        /// <returns>Resulting <see cref="PortableMarshalAwareSerializer"/>, or null.</returns>
        private static IPortableSerializer GetPortableMarshalAwareSerializer(Type type)
        {
            return type.GetInterfaces().Contains(typeof (IPortableMarshalAware)) 
                ? PortableMarshalAwareSerializer.INSTANCE 
                : null;
        }

        /// <summary>
        /// Add predefined type.
        /// </summary>
        /// <param name="type">Type.</param>
        /// <param name="typeId">Type ID.</param>
        /// <param name="typedHandler">Typed handler.</param>
        /// <param name="untypedHandler">Untyped handler.</param>
        private void AddPredefinedType(Type type, int typeId, object typedHandler,
            PortableSystemWriteDelegate untypedHandler)
        {
            AddType(type, typeId, GetTypeName(type), false, false, false, null, null, null, null, typedHandler,
                untypedHandler);
        }

        /// <summary>
        /// Add type.
        /// </summary>
        /// <param name="type">Type.</param>
        /// <param name="typeId">Type ID.</param>
        /// <param name="typeName">Type name.</param>
        /// <param name="userType">User type flag.</param>
        /// <param name="metaEnabled">Metadata enabled flag.</param>
        /// <param name="keepDeserialized">Whether to cache deserialized value in IPortableObject</param>
        /// <param name="nameMapper">Name mapper.</param>
        /// <param name="idMapper">ID mapper.</param>
        /// <param name="serializer">Serializer.</param>
        /// <param name="affKeyFieldName">Affinity key field name.</param>
        /// <param name="typedHandler">Typed handler.</param>
        /// <param name="untypedHandler">Untyped handler.</param>
        private void AddType(Type type, int typeId, string typeName, bool userType, bool metaEnabled,
            bool keepDeserialized, IPortableNameMapper nameMapper, IPortableIdMapper idMapper,
            IPortableSerializer serializer, string affKeyFieldName, object typedHandler,
            PortableSystemWriteDelegate untypedHandler)
        {
            long typeKey = PortableUtils.TypeKey(userType, typeId);

            if (idToDesc.ContainsKey(typeKey))
            {
                string type1 = idToDesc[typeKey].Type != null ? idToDesc[typeKey].Type.AssemblyQualifiedName : null;
                string type2 = type != null ? type.AssemblyQualifiedName : null;

                throw igniteContext.ConvertException(new PortableException("Conflicting type IDs [type1=" + type1 + ", type2=" + type2 +
                    ", typeId=" + typeId + ']'));
            }

            if (userType && typeNameToDesc.ContainsKey(typeName))
                throw igniteContext.ConvertException(new PortableException("Conflicting type name: " + typeName));

            IPortableTypeDescriptor descriptor =
                new PortableFullTypeDescriptor(type, typeId, typeName, userType, nameMapper, idMapper, serializer,
                    metaEnabled, keepDeserialized, affKeyFieldName, typedHandler, untypedHandler);

            if (type != null)
                typeToDesc[type] = descriptor;

            if (userType)
                typeNameToDesc[typeName] = descriptor;

            idToDesc[typeKey] = descriptor;            
        }

        /// <summary>
        /// Adds a predefined system type.
        /// </summary>
        protected void AddSystemType<T>(byte typeId, Func<IPortableReaderEx, T> ctor) where T : IPortableWriteAware
        {
            var type = typeof(T);

            var serializer = new PortableSystemTypeSerializer<T>(ctor);

            AddType(type, typeId, GetTypeName(type), false, false, false, null, null, serializer, null, null, null);
        }

        /// <summary>
        /// Gets the name of the type.
        /// </summary>
        /// <param name="type">The type.</param>
        /// <returns>
        /// Simple type name for non-generic types; simple type name with appended generic arguments for generic types.
        /// </returns>
        private static string GetTypeName(Type type)
        {
            if (!type.IsGenericType)
                return type.Name;

            var args = type.GetGenericArguments().Select(GetTypeName).Aggregate((x, y) => x + "," + y);

            return string.Format("{0}[{1}]", type.Name, args);
        }
    }
}
