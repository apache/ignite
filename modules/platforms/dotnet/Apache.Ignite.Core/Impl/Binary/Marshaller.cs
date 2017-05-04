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
    using System.Runtime.Serialization;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Cache.Affinity;
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Impl.Binary.IO;
    using Apache.Ignite.Core.Impl.Binary.Metadata;
    using Apache.Ignite.Core.Impl.Cache;
    using Apache.Ignite.Core.Impl.Cache.Query.Continuous;
    using Apache.Ignite.Core.Impl.Common;
    using Apache.Ignite.Core.Impl.Compute;
    using Apache.Ignite.Core.Impl.Compute.Closure;
    using Apache.Ignite.Core.Impl.Datastream;
    using Apache.Ignite.Core.Impl.Messaging;
    using Apache.Ignite.Core.Log;

    /// <summary>
    /// Marshaller implementation.
    /// </summary>
    internal class Marshaller
    {
        /** Binary configuration. */
        private readonly BinaryConfiguration _cfg;

        /** Type to descriptor map. */
        private readonly CopyOnWriteConcurrentDictionary<Type, BinaryFullTypeDescriptor> _typeToDesc =
            new CopyOnWriteConcurrentDictionary<Type, BinaryFullTypeDescriptor>();

        /** Type name to descriptor map. */
        private readonly CopyOnWriteConcurrentDictionary<string, BinaryFullTypeDescriptor> _typeNameToDesc =
            new CopyOnWriteConcurrentDictionary<string, BinaryFullTypeDescriptor>();

        /** ID to descriptor map. */
        private readonly CopyOnWriteConcurrentDictionary<long, BinaryFullTypeDescriptor> _idToDesc =
            new CopyOnWriteConcurrentDictionary<long, BinaryFullTypeDescriptor>();

        /** Cached binary types. */
        private volatile IDictionary<int, BinaryTypeHolder> _metas = new Dictionary<int, BinaryTypeHolder>();

        /** */
        private volatile Ignite _ignite;

        /** */
        private readonly ILogger _log;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="cfg">Configuration.</param>
        /// <param name="log"></param>
        public Marshaller(BinaryConfiguration cfg, ILogger log = null)
        {
            _cfg = cfg ?? new BinaryConfiguration();

            _log = log;

            CompactFooter = _cfg.CompactFooter;

            if (_cfg.TypeConfigurations == null)
                _cfg.TypeConfigurations = new List<BinaryTypeConfiguration>();

            foreach (BinaryTypeConfiguration typeCfg in _cfg.TypeConfigurations)
            {
                if (string.IsNullOrEmpty(typeCfg.TypeName))
                    throw new BinaryObjectException("Type name cannot be null or empty: " + typeCfg);
            }

            // Define system types. They use internal reflective stuff, so configuration doesn't affect them.
            AddSystemTypes();

            // 2. Define user types.
            var typeResolver = new TypeResolver();

            ICollection<BinaryTypeConfiguration> typeCfgs = _cfg.TypeConfigurations;

            if (typeCfgs != null)
                foreach (BinaryTypeConfiguration typeCfg in typeCfgs)
                    AddUserType(cfg, typeCfg, typeResolver);

            var typeNames = _cfg.Types;

            if (typeNames != null)
                foreach (string typeName in typeNames)
                    AddUserType(cfg, new BinaryTypeConfiguration(typeName), typeResolver);
        }

        /// <summary>
        /// Gets or sets the backing grid.
        /// </summary>
        public Ignite Ignite
        {
            get { return _ignite; }
            set
            {
                Debug.Assert(value != null);

                _ignite = value;
            }
        }

        /// <summary>
        /// Gets the compact footer flag.
        /// </summary>
        public bool CompactFooter { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether type registration is disabled.
        /// This may be desirable for static system marshallers where everything is written in unregistered mode.
        /// </summary>
        public bool RegistrationDisabled { get; set; }

        /// <summary>
        /// Marshal object.
        /// </summary>
        /// <param name="val">Value.</param>
        /// <returns>Serialized data as byte array.</returns>
        public byte[] Marshal<T>(T val)
        {
            using (var stream = new BinaryHeapStream(128))
            {
                Marshal(val, stream);

                return stream.GetArrayCopy();
            }
        }

        /// <summary>
        /// Marshals an object.
        /// </summary>
        /// <param name="val">Value.</param>
        /// <param name="stream">Output stream.</param>
        private void Marshal<T>(T val, IBinaryStream stream)
        {
            BinaryWriter writer = StartMarshal(stream);

            writer.Write(val);

            FinishMarshal(writer);
        }

        /// <summary>
        /// Start marshal session.
        /// </summary>
        /// <param name="stream">Stream.</param>
        /// <returns>Writer.</returns>
        public BinaryWriter StartMarshal(IBinaryStream stream)
        {
            return new BinaryWriter(this, stream);
        }

        /// <summary>
        /// Finish marshal session.
        /// </summary>
        /// <param name="writer">Writer.</param>
        /// <returns>Dictionary with metadata.</returns>
        public void FinishMarshal(BinaryWriter writer)
        {
            var metas = writer.GetBinaryTypes();

            var ignite = Ignite;

            if (ignite != null && metas != null && metas.Count > 0)
            {
                ignite.BinaryProcessor.PutBinaryTypes(metas);
            }
        }

        /// <summary>
        /// Unmarshal object.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="data">Data array.</param>
        /// <param name="keepBinary">Whether to keep binarizable as binary.</param>
        /// <returns>
        /// Object.
        /// </returns>
        public T Unmarshal<T>(byte[] data, bool keepBinary)
        {
            using (var stream = new BinaryHeapStream(data))
            {
                return Unmarshal<T>(stream, keepBinary);
            }
        }

        /// <summary>
        /// Unmarshal object.
        /// </summary>
        /// <param name="data">Data array.</param>
        /// <param name="mode">The mode.</param>
        /// <returns>
        /// Object.
        /// </returns>
        public T Unmarshal<T>(byte[] data, BinaryMode mode = BinaryMode.Deserialize)
        {
            using (var stream = new BinaryHeapStream(data))
            {
                return Unmarshal<T>(stream, mode);
            }
        }

        /// <summary>
        /// Unmarshal object.
        /// </summary>
        /// <param name="stream">Stream over underlying byte array with correct position.</param>
        /// <param name="keepBinary">Whether to keep binary objects in binary form.</param>
        /// <returns>
        /// Object.
        /// </returns>
        public T Unmarshal<T>(IBinaryStream stream, bool keepBinary)
        {
            return Unmarshal<T>(stream, keepBinary ? BinaryMode.KeepBinary : BinaryMode.Deserialize, null);
        }

        /// <summary>
        /// Unmarshal object.
        /// </summary>
        /// <param name="stream">Stream over underlying byte array with correct position.</param>
        /// <param name="mode">The mode.</param>
        /// <returns>
        /// Object.
        /// </returns>
        public T Unmarshal<T>(IBinaryStream stream, BinaryMode mode = BinaryMode.Deserialize)
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
        public T Unmarshal<T>(IBinaryStream stream, BinaryMode mode, BinaryObjectBuilder builder)
        {
            return new BinaryReader(this, stream, mode, builder).Deserialize<T>();
        }

        /// <summary>
        /// Start unmarshal session.
        /// </summary>
        /// <param name="stream">Stream.</param>
        /// <param name="keepBinary">Whether to keep binarizable as binary.</param>
        /// <returns>
        /// Reader.
        /// </returns>
        public BinaryReader StartUnmarshal(IBinaryStream stream, bool keepBinary)
        {
            return new BinaryReader(this, stream, keepBinary ? BinaryMode.KeepBinary : BinaryMode.Deserialize, null);
        }

        /// <summary>
        /// Start unmarshal session.
        /// </summary>
        /// <param name="stream">Stream.</param>
        /// <param name="mode">The mode.</param>
        /// <returns>Reader.</returns>
        public BinaryReader StartUnmarshal(IBinaryStream stream, BinaryMode mode = BinaryMode.Deserialize)
        {
            return new BinaryReader(this, stream, mode, null);
        }
        
        /// <summary>
        /// Gets metadata for the given type ID.
        /// </summary>
        /// <param name="typeId">Type ID.</param>
        /// <returns>Metadata or null.</returns>
        public IBinaryType GetBinaryType(int typeId)
        {
            if (Ignite != null)
            {
                IBinaryType meta = Ignite.BinaryProcessor.GetBinaryType(typeId);

                if (meta != null)
                    return meta;
            }

            return BinaryType.Empty;
        }

        /// <summary>
        /// Puts the binary type metadata to Ignite.
        /// </summary>
        /// <param name="desc">Descriptor.</param>
        public void PutBinaryType(IBinaryTypeDescriptor desc)
        {
            Debug.Assert(desc != null);

            GetBinaryTypeHandler(desc);  // ensure that handler exists

            if (Ignite != null)
            {
                ICollection<BinaryType> metas = new[] {new BinaryType(desc)};
                Ignite.BinaryProcessor.PutBinaryTypes(metas);
            }
        }

        /// <summary>
        /// Gets binary type handler for the given type ID.
        /// </summary>
        /// <param name="desc">Type descriptor.</param>
        /// <returns>Binary type handler.</returns>
        public IBinaryTypeHandler GetBinaryTypeHandler(IBinaryTypeDescriptor desc)
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

                        holder = new BinaryTypeHolder(desc.TypeId, desc.TypeName, desc.AffinityKeyFieldName, desc.IsEnum);

                        metas0[desc.TypeId] = holder;

                        _metas = metas0;
                    }
                }
            }

            if (holder != null)
            {
                ICollection<int> ids = holder.GetFieldIds();

                bool newType = ids.Count == 0 && !holder.Saved();

                return new BinaryTypeHashsetHandler(ids, newType);
            }

            return null;
        }

        /// <summary>
        /// Callback invoked when metadata has been sent to the server and acknowledged by it.
        /// </summary>
        /// <param name="newMetas">Binary types.</param>
        public void OnBinaryTypesSent(IEnumerable<BinaryType> newMetas)
        {
            foreach (var meta in newMetas)
            {
                var mergeInfo = new Dictionary<int, Tuple<string, BinaryField>>(meta.GetFieldsMap().Count);

                foreach (var fieldMeta in meta.GetFieldsMap())
                {
                    int fieldId = BinaryUtils.FieldId(meta.TypeId, fieldMeta.Key, null, null);

                    mergeInfo[fieldId] = new Tuple<string, BinaryField>(fieldMeta.Key, fieldMeta.Value);
                }

                _metas[meta.TypeId].Merge(mergeInfo);
            }
        }

        /// <summary>
        /// Gets descriptor for type.
        /// </summary>
        /// <param name="type">Type.</param>
        /// <returns>
        /// Descriptor.
        /// </returns>
        public IBinaryTypeDescriptor GetDescriptor(Type type)
        {
            BinaryFullTypeDescriptor desc;

            if (!_typeToDesc.TryGetValue(type, out desc) || !desc.IsRegistered)
                desc = RegisterType(type, desc);

            return desc;
        }

        /// <summary>
        /// Gets descriptor for type name.
        /// </summary>
        /// <param name="typeName">Type name.</param>
        /// <returns>Descriptor.</returns>
        public IBinaryTypeDescriptor GetDescriptor(string typeName)
        {
            BinaryFullTypeDescriptor desc;

            return _typeNameToDesc.TryGetValue(typeName, out desc)
                ? (IBinaryTypeDescriptor) desc
                : new BinarySurrogateTypeDescriptor(_cfg,
                    GetTypeId(typeName, _cfg.IdMapper), typeName);
        }

        /// <summary>
        /// Gets descriptor for a type id.
        /// </summary>
        /// <param name="userType">User type flag.</param>
        /// <param name="typeId">Type id.</param>
        /// <param name="requiresType">
        /// If set to true, resulting descriptor must have Type property populated.
        /// <para />
        /// When working in binary mode, we don't need Type. And there is no Type at all in some cases.
        /// So we should not attempt to call BinaryProcessor right away.
        /// Only when we really deserialize the value, requiresType is set to true
        /// and we attempt to resolve the type by all means.
        /// </param>
        /// <returns>
        /// Descriptor.
        /// </returns>
        public IBinaryTypeDescriptor GetDescriptor(bool userType, int typeId, bool requiresType = false)
        {
            BinaryFullTypeDescriptor desc;

            var typeKey = BinaryUtils.TypeKey(userType, typeId);

            if (_idToDesc.TryGetValue(typeKey, out desc) && (!requiresType || desc.Type != null))
                return desc;

            if (!userType)
                return null;

            if (requiresType && _ignite != null)
            {
                // Check marshaller context for dynamically registered type.
                var typeName = _ignite.BinaryProcessor.GetTypeName(typeId);

                if (typeName != null)
                {
                    var type = new TypeResolver().ResolveType(typeName, nameMapper: 
                        _cfg.NameMapper ?? GetDefaultNameMapper());

                    if (type != null)
                    {
                        return AddUserType(type, typeId, GetTypeName(type), true, desc);
                    }
                }
            }

            var meta = GetBinaryType(typeId);

            if (meta != BinaryType.Empty)
            {
                desc = new BinaryFullTypeDescriptor(null, meta.TypeId, meta.TypeName, true, null, null, null, false,
                    meta.AffinityKeyFieldName, meta.IsEnum);

                _idToDesc.GetOrAdd(typeKey, _ => desc);

                return desc;
            }

            return new BinarySurrogateTypeDescriptor(_cfg, typeId, null);
        }

        /// <summary>
        /// Registers the type.
        /// </summary>
        /// <param name="type">The type.</param>
        /// <param name="desc">Existing descriptor.</param>
        private BinaryFullTypeDescriptor RegisterType(Type type, BinaryFullTypeDescriptor desc)
        {
            Debug.Assert(type != null);

            var typeName = GetTypeName(type);
            var typeId = GetTypeId(typeName, _cfg.IdMapper);

            var registered = _ignite != null && _ignite.BinaryProcessor.RegisterType(typeId, typeName);

            return AddUserType(type, typeId, typeName, registered, desc);
        }

        /// <summary>
        /// Add user type.
        /// </summary>
        /// <param name="type">The type.</param>
        /// <param name="typeId">The type id.</param>
        /// <param name="typeName">Name of the type.</param>
        /// <param name="registered">Registered flag.</param>
        /// <param name="desc">Existing descriptor.</param>
        /// <returns>Descriptor.</returns>
        private BinaryFullTypeDescriptor AddUserType(Type type, int typeId, string typeName, bool registered,
            BinaryFullTypeDescriptor desc)
        {
            Debug.Assert(type != null);
            Debug.Assert(typeName != null);

            var ser = GetSerializer(_cfg, null, type, typeId, null, null, _log);

            desc = desc == null
                ? new BinaryFullTypeDescriptor(type, typeId, typeName, true, _cfg.NameMapper,
                    _cfg.IdMapper, ser, false, null, type.IsEnum, registered)
                : new BinaryFullTypeDescriptor(desc, type, ser, registered);

            if (RegistrationDisabled)
                return desc;

            var typeKey = BinaryUtils.TypeKey(true, typeId);

            var desc0 = _idToDesc.GetOrAdd(typeKey, x => desc);
            if (desc0.Type != null && desc0.Type.FullName != type.FullName)
                ThrowConflictingTypeError(type, desc0.Type, typeId);

            desc0 = _typeNameToDesc.GetOrAdd(typeName, x => desc);
            if (desc0.Type != null && desc0.Type.FullName != type.FullName)
                ThrowConflictingTypeError(type, desc0.Type, typeId);

            _typeToDesc.Set(type, desc);

            return desc;
        }

        /// <summary>
        /// Throws the conflicting type error.
        /// </summary>
        private static void ThrowConflictingTypeError(object type1, object type2, int typeId)
        {
            throw new BinaryObjectException(string.Format("Conflicting type IDs [type1='{0}', " +
                                                          "type2='{1}', typeId={2}]", type1, type2, typeId));
        }

        /// <summary>
        /// Add user type.
        /// </summary>
        /// <param name="cfg">The binary configuration.</param>
        /// <param name="typeCfg">Type configuration.</param>
        /// <param name="typeResolver">The type resolver.</param>
        /// <exception cref="BinaryObjectException"></exception>
        private void AddUserType(BinaryConfiguration cfg, BinaryTypeConfiguration typeCfg, TypeResolver typeResolver)
        {
            // Get converter/mapper/serializer.
            IBinaryNameMapper nameMapper = typeCfg.NameMapper ?? _cfg.NameMapper ?? GetDefaultNameMapper();

            IBinaryIdMapper idMapper = typeCfg.IdMapper ?? _cfg.IdMapper;

            bool keepDeserialized = typeCfg.KeepDeserialized ?? _cfg.KeepDeserialized;

            // Try resolving type.
            Type type = typeResolver.ResolveType(typeCfg.TypeName);

            if (type != null)
            {
                ValidateUserType(type);

                if (typeCfg.IsEnum != type.IsEnum)
                {
                    throw new BinaryObjectException(
                        string.Format(
                            "Invalid IsEnum flag in binary type configuration. " +
                            "Configuration value: IsEnum={0}, actual type: IsEnum={1}",
                            typeCfg.IsEnum, type.IsEnum));
                }

                // Type is found.
                var typeName = GetTypeName(type, nameMapper);
                int typeId = GetTypeId(typeName, idMapper);
                var affKeyFld = typeCfg.AffinityKeyFieldName ?? GetAffinityKeyFieldNameFromAttribute(type);
                var serializer = GetSerializer(cfg, typeCfg, type, typeId, nameMapper, idMapper, _log);

                AddType(type, typeId, typeName, true, keepDeserialized, nameMapper, idMapper, serializer,
                    affKeyFld, type.IsEnum);
            }
            else
            {
                // Type is not found.
                string typeName = GetTypeName(typeCfg.TypeName, nameMapper);

                int typeId = GetTypeId(typeName, idMapper);

                AddType(null, typeId, typeName, true, keepDeserialized, nameMapper, idMapper, null,
                    typeCfg.AffinityKeyFieldName, typeCfg.IsEnum);
            }
        }

        /// <summary>
        /// Gets the serializer.
        /// </summary>
        private static IBinarySerializerInternal GetSerializer(BinaryConfiguration cfg, 
            BinaryTypeConfiguration typeCfg, Type type, int typeId, IBinaryNameMapper nameMapper,
            IBinaryIdMapper idMapper, ILogger log)
        {
            var serializer = (typeCfg != null ? typeCfg.Serializer : null) ??
                             (cfg != null ? cfg.Serializer : null);

            if (serializer == null)
            {
                if (type.GetInterfaces().Contains(typeof(IBinarizable)))
                    return BinarizableSerializer.Instance;

                if (type.GetInterfaces().Contains(typeof(ISerializable)))
                {
                    LogSerializableWarning(type, log);

                    return new SerializableSerializer(type);
                }

                serializer = new BinaryReflectiveSerializer();
            }

            var refSerializer = serializer as BinaryReflectiveSerializer;

            return refSerializer != null
                ? refSerializer.Register(type, typeId, nameMapper, idMapper)
                : new UserSerializerProxy(serializer);
        }

        /// <summary>
        /// Gets the affinity key field name from attribute.
        /// </summary>
        private static string GetAffinityKeyFieldNameFromAttribute(Type type)
        {
            var res = type.GetMembers()
                .Where(x => x.GetCustomAttributes(false).OfType<AffinityKeyMappedAttribute>().Any())
                .Select(x => x.Name).ToArray();

            if (res.Length > 1)
            {
                throw new BinaryObjectException(string.Format("Multiple '{0}' attributes found on type '{1}'. " +
                    "There can be only one affinity field.", typeof (AffinityKeyMappedAttribute).Name, type));
            }

            return res.SingleOrDefault();
        }

        /// <summary>
        /// Add type.
        /// </summary>
        /// <param name="type">Type.</param>
        /// <param name="typeId">Type ID.</param>
        /// <param name="typeName">Type name.</param>
        /// <param name="userType">User type flag.</param>
        /// <param name="keepDeserialized">Whether to cache deserialized value in IBinaryObject</param>
        /// <param name="nameMapper">Name mapper.</param>
        /// <param name="idMapper">ID mapper.</param>
        /// <param name="serializer">Serializer.</param>
        /// <param name="affKeyFieldName">Affinity key field name.</param>
        /// <param name="isEnum">Enum flag.</param>
        private void AddType(Type type, int typeId, string typeName, bool userType,
            bool keepDeserialized, IBinaryNameMapper nameMapper, IBinaryIdMapper idMapper,
            IBinarySerializerInternal serializer, string affKeyFieldName, bool isEnum)
        {
            long typeKey = BinaryUtils.TypeKey(userType, typeId);

            BinaryFullTypeDescriptor conflictingType;

            if (_idToDesc.TryGetValue(typeKey, out conflictingType))
            {
                var type1 = conflictingType.Type != null
                    ? conflictingType.Type.AssemblyQualifiedName
                    : conflictingType.TypeName;

                var type2 = type != null ? type.AssemblyQualifiedName : typeName;

                ThrowConflictingTypeError(type1, type2, typeId);
            }

            if (userType && _typeNameToDesc.ContainsKey(typeName))
                throw new BinaryObjectException("Conflicting type name: " + typeName);

            var descriptor = new BinaryFullTypeDescriptor(type, typeId, typeName, userType, nameMapper, idMapper, 
                serializer, keepDeserialized, affKeyFieldName, isEnum);

            if (type != null)
                _typeToDesc.GetOrAdd(type, x => descriptor);

            if (userType)
                _typeNameToDesc.GetOrAdd(typeName, x => descriptor);

            _idToDesc.GetOrAdd(typeKey, _ => descriptor);
        }

        /// <summary>
        /// Adds a predefined system type.
        /// </summary>
        private void AddSystemType<T>(int typeId, Func<BinaryReader, T> ctor, string affKeyFldName = null,
            IBinarySerializerInternal serializer = null)
            where T : IBinaryWriteAware
        {
            var type = typeof(T);

            serializer = serializer ?? new BinarySystemTypeSerializer<T>(ctor);

            // System types always use simple name mapper.
            var typeName = type.Name;

            if (typeId == 0)
            {
                typeId = BinaryUtils.GetStringHashCode(typeName);
            }

            AddType(type, typeId, typeName, false, false, null, null, serializer, affKeyFldName, false);
        }

        /// <summary>
        /// Adds predefined system types.
        /// </summary>
        private void AddSystemTypes()
        {
            AddSystemType(BinaryUtils.TypeNativeJobHolder, r => new ComputeJobHolder(r));
            AddSystemType(BinaryUtils.TypeComputeJobWrapper, r => new ComputeJobWrapper(r));
            AddSystemType(BinaryUtils.TypeComputeOutFuncJob, r => new ComputeOutFuncJob(r));
            AddSystemType(BinaryUtils.TypeComputeOutFuncWrapper, r => new ComputeOutFuncWrapper(r));
            AddSystemType(BinaryUtils.TypeComputeFuncWrapper, r => new ComputeFuncWrapper(r));
            AddSystemType(BinaryUtils.TypeComputeFuncJob, r => new ComputeFuncJob(r));
            AddSystemType(BinaryUtils.TypeComputeActionJob, r => new ComputeActionJob(r));
            AddSystemType(BinaryUtils.TypeContinuousQueryRemoteFilterHolder, r => new ContinuousQueryFilterHolder(r));
            AddSystemType(BinaryUtils.TypeCacheEntryProcessorHolder, r => new CacheEntryProcessorHolder(r));
            AddSystemType(BinaryUtils.TypeCacheEntryPredicateHolder, r => new CacheEntryFilterHolder(r));
            AddSystemType(BinaryUtils.TypeMessageListenerHolder, r => new MessageListenerHolder(r));
            AddSystemType(BinaryUtils.TypeStreamReceiverHolder, r => new StreamReceiverHolder(r));
            AddSystemType(0, r => new AffinityKey(r), "affKey");
            AddSystemType(BinaryUtils.TypePlatformJavaObjectFactoryProxy, r => new PlatformJavaObjectFactoryProxy());
            AddSystemType(0, r => new ObjectInfoHolder(r));
            AddSystemType(BinaryUtils.TypeIgniteUuid, r => new IgniteGuid(r));
        }

        /// <summary>
        /// Logs the warning about ISerializable pitfalls.
        /// </summary>
        private static void LogSerializableWarning(Type type, ILogger log)
        {
            if (log == null)
                return;

            log.GetLogger(typeof(Marshaller).Name)
                .Warn("Type '{0}' implements '{1}'. It will be written in Ignite binary format, however, " +
                      "the following limitations apply: " +
                      "DateTime fields would not work in SQL; " +
                      "sbyte, ushort, uint, ulong fields would not work in DML.", type, typeof(ISerializable));
        }

        /// <summary>
        /// Validates binary type.
        /// </summary>
        // ReSharper disable once UnusedParameter.Local
        private static void ValidateUserType(Type type)
        {
            Debug.Assert(type != null);

            if (type.IsGenericTypeDefinition)
            {
                throw new BinaryObjectException(
                    "Open generic types (Type.IsGenericTypeDefinition == true) are not allowed " +
                    "in BinaryConfiguration: " + type.AssemblyQualifiedName);
            }

            if (type.IsAbstract)
            {
                throw new BinaryObjectException(
                    "Abstract types and interfaces are not allowed in BinaryConfiguration: " +
                    type.AssemblyQualifiedName);
            }
        }

        /// <summary>
        /// Gets the name of the type.
        /// </summary>
        private string GetTypeName(Type type, IBinaryNameMapper mapper = null)
        {
            return GetTypeName(type.AssemblyQualifiedName, mapper);
        }

        /// <summary>
        /// Gets the name of the type.
        /// </summary>
        private string GetTypeName(string fullTypeName, IBinaryNameMapper mapper = null)
        {
            mapper = mapper ?? _cfg.NameMapper ?? GetDefaultNameMapper();

            var typeName = mapper.GetTypeName(fullTypeName);

            if (typeName == null)
            {
                throw new BinaryObjectException("IBinaryNameMapper returned null name for type [typeName=" +
                                                fullTypeName + ", mapper=" + mapper + "]");
            }

            return typeName;
        }

        /// <summary>
        /// Resolve type ID.
        /// </summary>
        /// <param name="typeName">Type name.</param>
        /// <param name="idMapper">ID mapper.</param>
        private static int GetTypeId(string typeName, IBinaryIdMapper idMapper)
        {
            Debug.Assert(typeName != null);

            int id = 0;

            if (idMapper != null)
            {
                try
                {
                    id = idMapper.GetTypeId(typeName);
                }
                catch (Exception e)
                {
                    throw new BinaryObjectException("Failed to resolve type ID due to ID mapper exception " +
                                                    "[typeName=" + typeName + ", idMapper=" + idMapper + ']', e);
                }
            }

            if (id == 0)
            {
                id = BinaryUtils.GetStringHashCode(typeName);
            }

            return id;
        }

        /// <summary>
        /// Gets the default name mapper.
        /// </summary>
        private static IBinaryNameMapper GetDefaultNameMapper()
        {
            return BinaryBasicNameMapper.FullNameInstance;
        }
    }
}
