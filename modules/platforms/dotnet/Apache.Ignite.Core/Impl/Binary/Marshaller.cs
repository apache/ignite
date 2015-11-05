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
    using System.Globalization;
    using System.Linq;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Impl.Binary.IO;
    using Apache.Ignite.Core.Impl.Binary.Metadata;
    using Apache.Ignite.Core.Impl.Cache;
    using Apache.Ignite.Core.Impl.Cache.Query.Continuous;
    using Apache.Ignite.Core.Impl.Compute;
    using Apache.Ignite.Core.Impl.Compute.Closure;
    using Apache.Ignite.Core.Impl.Datastream;
    using Apache.Ignite.Core.Impl.Messaging;

    /// <summary>
    /// Marshaller implementation.
    /// </summary>
    internal class Marshaller
    {
        /** Portable configuration. */
        private readonly BinaryConfiguration _cfg;

        /** Type to descriptor map. */
        private readonly IDictionary<Type, IBinaryTypeDescriptor> _typeToDesc =
            new Dictionary<Type, IBinaryTypeDescriptor>();

        /** Type name to descriptor map. */
        private readonly IDictionary<string, IBinaryTypeDescriptor> _typeNameToDesc =
            new Dictionary<string, IBinaryTypeDescriptor>();

        /** ID to descriptor map. */
        private readonly IDictionary<long, IBinaryTypeDescriptor> _idToDesc =
            new Dictionary<long, IBinaryTypeDescriptor>();

        /** Cached metadatas. */
        private volatile IDictionary<int, BinaryTypeHolder> _metas =
            new Dictionary<int, BinaryTypeHolder>();

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="cfg">Configurtaion.</param>
        public Marshaller(BinaryConfiguration cfg)
        {
            // Validation.
            if (cfg == null)
                cfg = new BinaryConfiguration();

            if (cfg.TypeConfigurations == null)
                cfg.TypeConfigurations = new List<BinaryTypeConfiguration>();

            foreach (BinaryTypeConfiguration typeCfg in cfg.TypeConfigurations)
            {
                if (string.IsNullOrEmpty(typeCfg.TypeName))
                    throw new BinaryObjectException("Type name cannot be null or empty: " + typeCfg);
            }

            // Define system types. They use internal reflective stuff, so configuration doesn't affect them.
            AddSystemTypes();

            // 2. Define user types.
            var dfltSerializer = cfg.DefaultSerializer == null ? new BinaryReflectiveSerializer() : null;

            var typeResolver = new TypeResolver();

            ICollection<BinaryTypeConfiguration> typeCfgs = cfg.TypeConfigurations;

            if (typeCfgs != null)
                foreach (BinaryTypeConfiguration typeCfg in typeCfgs)
                    AddUserType(cfg, typeCfg, typeResolver, dfltSerializer);

            ICollection<string> types = cfg.Types;

            if (types != null)
                foreach (string type in types)
                    AddUserType(cfg, new BinaryTypeConfiguration(type), typeResolver, dfltSerializer);

            if (cfg.DefaultSerializer == null)
                cfg.DefaultSerializer = dfltSerializer;

            _cfg = cfg;
        }

        /// <summary>
        /// Gets or sets the backing grid.
        /// </summary>
        public Ignite Ignite { get; set; }

        /// <summary>
        /// Marshal object.
        /// </summary>
        /// <param name="val">Value.</param>
        /// <returns>Serialized data as byte array.</returns>
        public byte[] Marshal<T>(T val)
        {
            PortableHeapStream stream = new PortableHeapStream(128);

            Marshal(val, stream);

            return stream.GetArrayCopy();
        }

        /// <summary>
        /// Marshal object.
        /// </summary>
        /// <param name="val">Value.</param>
        /// <param name="stream">Output stream.</param>
        /// <returns>Collection of metadatas (if any).</returns>
        private void Marshal<T>(T val, IPortableStream stream)
        {
            BinaryWriterImpl writer = StartMarshal(stream);

            writer.Write(val);

            FinishMarshal(writer);
        }

        /// <summary>
        /// Start marshal session.
        /// </summary>
        /// <param name="stream">Stream.</param>
        /// <returns>Writer.</returns>
        public BinaryWriterImpl StartMarshal(IPortableStream stream)
        {
            return new BinaryWriterImpl(this, stream);
        }

        /// <summary>
        /// Finish marshal session.
        /// </summary>
        /// <param name="writer">Writer.</param>
        /// <returns>Dictionary with metadata.</returns>
        public void FinishMarshal(IBinaryWriter writer)
        {
            var meta = ((BinaryWriterImpl) writer).Metadata();

            var ignite = Ignite;

            if (ignite != null && meta != null && meta.Count > 0)
                ignite.PutMetadata(meta);
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
        public T Unmarshal<T>(byte[] data, PortableMode mode = PortableMode.Deserialize)
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
            return Unmarshal<T>(stream, keepPortable ? PortableMode.KeepPortable : PortableMode.Deserialize, null);
        }

        /// <summary>
        /// Unmarshal object.
        /// </summary>
        /// <param name="stream">Stream over underlying byte array with correct position.</param>
        /// <param name="mode">The mode.</param>
        /// <returns>
        /// Object.
        /// </returns>
        public T Unmarshal<T>(IPortableStream stream, PortableMode mode = PortableMode.Deserialize)
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
            return new BinaryReaderImpl(this, _idToDesc, stream, mode, builder).Deserialize<T>();
        }

        /// <summary>
        /// Start unmarshal session.
        /// </summary>
        /// <param name="stream">Stream.</param>
        /// <param name="keepPortable">Whether to keep portables as portables.</param>
        /// <returns>
        /// Reader.
        /// </returns>
        public BinaryReaderImpl StartUnmarshal(IPortableStream stream, bool keepPortable)
        {
            return new BinaryReaderImpl(this, _idToDesc, stream,
                keepPortable ? PortableMode.KeepPortable : PortableMode.Deserialize, null);
        }

        /// <summary>
        /// Start unmarshal session.
        /// </summary>
        /// <param name="stream">Stream.</param>
        /// <param name="mode">The mode.</param>
        /// <returns>Reader.</returns>
        public BinaryReaderImpl StartUnmarshal(IPortableStream stream, PortableMode mode = PortableMode.Deserialize)
        {
            return new BinaryReaderImpl(this, _idToDesc, stream, mode, null);
        }
        
        /// <summary>
        /// Gets metadata for the given type ID.
        /// </summary>
        /// <param name="typeId">Type ID.</param>
        /// <returns>Metadata or null.</returns>
        public IBinaryType GetMetadata(int typeId)
        {
            if (Ignite != null)
            {
                IBinaryType meta = Ignite.GetMetadata(typeId);

                if (meta != null)
                    return meta;
            }

            return BinaryType.EmptyMeta;
        }

        /// <summary>
        /// Gets metadata handler for the given type ID.
        /// </summary>
        /// <param name="desc">Type descriptor.</param>
        /// <returns>Metadata handler.</returns>
        public IPortableMetadataHandler GetMetadataHandler(IBinaryTypeDescriptor desc)
        {
            BinaryTypeHolder holder;

            if (!_metas.TryGetValue(desc.TypeId, out holder))
            {
                lock (this)
                {
                    if (!_metas.TryGetValue(desc.TypeId, out holder))
                    {
                        IDictionary<int, BinaryTypeHolder> metas0 =
                            new Dictionary<int, BinaryTypeHolder>(_metas);

                        holder = new BinaryTypeHolder(desc.TypeId, desc.TypeName, desc.AffinityKeyFieldName);

                        metas0[desc.TypeId] = holder;

                        _metas = metas0;
                    }
                }
            }

            if (holder != null)
            {
                ICollection<int> ids = holder.FieldIds();

                bool newType = ids.Count == 0 && !holder.Saved();

                return new PortableHashsetMetadataHandler(ids, newType);
            }

            return null;
        }

        /// <summary>
        /// Callback invoked when metadata has been sent to the server and acknowledged by it.
        /// </summary>
        /// <param name="newMetas"></param>
        public void OnMetadataSent(IDictionary<int, IBinaryType> newMetas)
        {
            foreach (KeyValuePair<int, IBinaryType> metaEntry in newMetas)
            {
                BinaryType meta = (BinaryType) metaEntry.Value;

                IDictionary<int, Tuple<string, int>> mergeInfo =
                    new Dictionary<int, Tuple<string, int>>(meta.FieldsMap().Count);

                foreach (KeyValuePair<string, int> fieldMeta in meta.FieldsMap())
                {
                    int fieldId = BinaryUtils.FieldId(metaEntry.Key, fieldMeta.Key, null, null);

                    mergeInfo[fieldId] = new Tuple<string, int>(fieldMeta.Key, fieldMeta.Value);
                }

                _metas[metaEntry.Key].Merge(mergeInfo);
            }
        }
        
        /// <summary>
        /// Gets descriptor for type.
        /// </summary>
        /// <param name="type">Type.</param>
        /// <returns>Descriptor.</returns>
        public IBinaryTypeDescriptor GetDescriptor(Type type)
        {
            IBinaryTypeDescriptor desc;

            _typeToDesc.TryGetValue(type, out desc);

            return desc;
        }

        /// <summary>
        /// Gets descriptor for type name.
        /// </summary>
        /// <param name="typeName">Type name.</param>
        /// <returns>Descriptor.</returns>
        public IBinaryTypeDescriptor GetDescriptor(string typeName)
        {
            IBinaryTypeDescriptor desc;

            return _typeNameToDesc.TryGetValue(typeName, out desc) ? desc : 
                new BinarySurrogateTypeDescriptor(_cfg, typeName);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="userType"></param>
        /// <param name="typeId"></param>
        /// <returns></returns>
        public IBinaryTypeDescriptor GetDescriptor(bool userType, int typeId)
        {
            IBinaryTypeDescriptor desc;

            return _idToDesc.TryGetValue(BinaryUtils.TypeKey(userType, typeId), out desc) ? desc :
                userType ? new BinarySurrogateTypeDescriptor(_cfg, typeId) : null;
        }

        /// <summary>
        /// Add user type.
        /// </summary>
        /// <param name="cfg">Configuration.</param>
        /// <param name="typeCfg">Type configuration.</param>
        /// <param name="typeResolver">The type resolver.</param>
        /// <param name="dfltSerializer">The default serializer.</param>
        private void AddUserType(BinaryConfiguration cfg, BinaryTypeConfiguration typeCfg, 
            TypeResolver typeResolver, IBinarySerializer dfltSerializer)
        {
            // Get converter/mapper/serializer.
            INameMapper nameMapper = typeCfg.NameMapper ?? cfg.DefaultNameMapper;

            IIdMapper idMapper = typeCfg.IdMapper ?? cfg.DefaultIdMapper;

            bool keepDeserialized = typeCfg.KeepDeserialized ?? cfg.DefaultKeepDeserialized;

            // Try resolving type.
            Type type = typeResolver.ResolveType(typeCfg.TypeName);

            if (type != null)
            {
                // Type is found.
                var typeName = GetTypeName(type);

                int typeId = BinaryUtils.TypeId(typeName, nameMapper, idMapper);

                var serializer = typeCfg.Serializer ?? cfg.DefaultSerializer
                                 ?? GetPortableMarshalAwareSerializer(type) ?? dfltSerializer;

                var refSerializer = serializer as BinaryReflectiveSerializer;

                if (refSerializer != null)
                    refSerializer.Register(type, typeId, nameMapper, idMapper);

                AddType(type, typeId, typeName, true, keepDeserialized, nameMapper, idMapper, serializer,
                    typeCfg.AffinityKeyFieldName);
            }
            else
            {
                // Type is not found.
                string typeName = BinaryUtils.SimpleTypeName(typeCfg.TypeName);

                int typeId = BinaryUtils.TypeId(typeName, nameMapper, idMapper);

                AddType(null, typeId, typeName, true, keepDeserialized, nameMapper, idMapper, null,
                    typeCfg.AffinityKeyFieldName);
            }
        }

        /// <summary>
        /// Gets the <see cref="BinaryMarshalAwareSerializer"/> for a type if it is compatible.
        /// </summary>
        /// <param name="type">The type.</param>
        /// <returns>Resulting <see cref="BinaryMarshalAwareSerializer"/>, or null.</returns>
        private static IBinarySerializer GetPortableMarshalAwareSerializer(Type type)
        {
            return type.GetInterfaces().Contains(typeof (IBinarizable)) 
                ? BinaryMarshalAwareSerializer.Instance 
                : null;
        }
        
        /// <summary>
        /// Add type.
        /// </summary>
        /// <param name="type">Type.</param>
        /// <param name="typeId">Type ID.</param>
        /// <param name="typeName">Type name.</param>
        /// <param name="userType">User type flag.</param>
        /// <param name="keepDeserialized">Whether to cache deserialized value in IPortableObject</param>
        /// <param name="nameMapper">Name mapper.</param>
        /// <param name="idMapper">ID mapper.</param>
        /// <param name="serializer">Serializer.</param>
        /// <param name="affKeyFieldName">Affinity key field name.</param>
        private void AddType(Type type, int typeId, string typeName, bool userType, 
            bool keepDeserialized, INameMapper nameMapper, IIdMapper idMapper,
            IBinarySerializer serializer, string affKeyFieldName)
        {
            long typeKey = BinaryUtils.TypeKey(userType, typeId);

            if (_idToDesc.ContainsKey(typeKey))
            {
                string type1 = _idToDesc[typeKey].Type != null ? _idToDesc[typeKey].Type.AssemblyQualifiedName : null;
                string type2 = type != null ? type.AssemblyQualifiedName : null;

                throw new BinaryObjectException("Conflicting type IDs [type1=" + type1 + ", type2=" + type2 +
                    ", typeId=" + typeId + ']');
            }

            if (userType && _typeNameToDesc.ContainsKey(typeName))
                throw new BinaryObjectException("Conflicting type name: " + typeName);

            IBinaryTypeDescriptor descriptor =
                new BinaryFullTypeDescriptor(type, typeId, typeName, userType, nameMapper, idMapper, serializer,
                    keepDeserialized, affKeyFieldName);

            if (type != null)
                _typeToDesc[type] = descriptor;

            if (userType)
                _typeNameToDesc[typeName] = descriptor;

            _idToDesc[typeKey] = descriptor;            
        }

        /// <summary>
        /// Adds a predefined system type.
        /// </summary>
        private void AddSystemType<T>(byte typeId, Func<BinaryReaderImpl, T> ctor) where T : IPortableWriteAware
        {
            var type = typeof(T);

            var serializer = new BinarySystemTypeSerializer<T>(ctor);

            AddType(type, typeId, GetTypeName(type), false, false, null, null, serializer, null);
        }

        /// <summary>
        /// Adds predefined system types.
        /// </summary>
        private void AddSystemTypes()
        {
            AddSystemType(BinaryUtils.TypeNativeJobHolder, w => new ComputeJobHolder(w));
            AddSystemType(BinaryUtils.TypeComputeJobWrapper, w => new ComputeJobWrapper(w));
            AddSystemType(BinaryUtils.TypeIgniteProxy, w => new IgniteProxy());
            AddSystemType(BinaryUtils.TypeComputeOutFuncJob, w => new ComputeOutFuncJob(w));
            AddSystemType(BinaryUtils.TypeComputeOutFuncWrapper, w => new ComputeOutFuncWrapper(w));
            AddSystemType(BinaryUtils.TypeComputeFuncWrapper, w => new ComputeFuncWrapper(w));
            AddSystemType(BinaryUtils.TypeComputeFuncJob, w => new ComputeFuncJob(w));
            AddSystemType(BinaryUtils.TypeComputeActionJob, w => new ComputeActionJob(w));
            AddSystemType(BinaryUtils.TypeContinuousQueryRemoteFilterHolder, w => new ContinuousQueryFilterHolder(w));
            AddSystemType(BinaryUtils.TypeSerializableHolder, w => new SerializableObjectHolder(w));
            AddSystemType(BinaryUtils.TypeDateTimeHolder, w => new DateTimeHolder(w));
            AddSystemType(BinaryUtils.TypeCacheEntryProcessorHolder, w => new CacheEntryProcessorHolder(w));
            AddSystemType(BinaryUtils.TypeCacheEntryPredicateHolder, w => new CacheEntryFilterHolder(w));
            AddSystemType(BinaryUtils.TypeMessageListenerHolder, w => new MessageListenerHolder(w));
            AddSystemType(BinaryUtils.TypeStreamReceiverHolder, w => new StreamReceiverHolder(w));
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

            return string.Format(CultureInfo.InvariantCulture, "{0}[{1}]", type.Name, args);
        }
    }
}
