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
    using System.Linq;
    using Apache.Ignite.Core.Impl.Cache;
    using Apache.Ignite.Core.Impl.Cache.Query.Continuous;
    using Apache.Ignite.Core.Impl.Common;
    using Apache.Ignite.Core.Impl.Compute;
    using Apache.Ignite.Core.Impl.Compute.Closure;
    using Apache.Ignite.Core.Impl.Datastream;
    using Apache.Ignite.Core.Impl.Interop;
    using Apache.Ignite.Core.Impl.Messaging;
    using Apache.Ignite.Core.Impl.Portable.IO;
    using Apache.Ignite.Core.Impl.Portable.Metadata;
    using Apache.Ignite.Core.Portable;

    /// <summary>
    /// Portable marshaller implementation.
    /// </summary>
    internal class PortableMarshaller
    {
        /** Portable configuration. */
        private readonly PortableConfiguration _cfg;

        /** Type to descriptor map. */
        private readonly IDictionary<Type, IPortableTypeDescriptor> _typeToDesc =
            new Dictionary<Type, IPortableTypeDescriptor>();

        /** Type name to descriptor map. */
        private readonly IDictionary<string, IPortableTypeDescriptor> _typeNameToDesc =
            new Dictionary<string, IPortableTypeDescriptor>();

        /** ID to descriptor map. */
        private readonly IDictionary<long, IPortableTypeDescriptor> _idToDesc =
            new Dictionary<long, IPortableTypeDescriptor>();

        /** Cached metadatas. */
        private volatile IDictionary<int, PortableMetadataHolder> _metas =
            new Dictionary<int, PortableMetadataHolder>();

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="cfg">Configurtaion.</param>
        public PortableMarshaller(PortableConfiguration cfg)
        {
            // Validation.
            if (cfg == null)
                cfg = new PortableConfiguration();

            if (cfg.TypeConfigurations == null)
                cfg.TypeConfigurations = new List<PortableTypeConfiguration>();

            foreach (PortableTypeConfiguration typeCfg in cfg.TypeConfigurations)
            {
                if (string.IsNullOrEmpty(typeCfg.TypeName))
                    throw new PortableException("Type name cannot be null or empty: " + typeCfg);

                if (typeCfg.AssemblyName != null && typeCfg.AssemblyName.Length == 0)
                    throw new PortableException("Assembly name cannot be empty string: " + typeCfg);
            }

            // Define predefined types.
            AddPredefinedType(typeof(bool), PortableUtils.TypeBool, PortableSystemHandlers.WriteHndBoolTyped, PortableSystemHandlers.WriteHndBool);
            AddPredefinedType(typeof(byte), PortableUtils.TypeByte, PortableSystemHandlers.WriteHndByteTyped, PortableSystemHandlers.WriteHndByte);
            AddPredefinedType(typeof(short), PortableUtils.TypeShort, PortableSystemHandlers.WriteHndShortTyped, PortableSystemHandlers.WriteHndShort);
            AddPredefinedType(typeof(char), PortableUtils.TypeChar, PortableSystemHandlers.WriteHndCharTyped, PortableSystemHandlers.WriteHndChar);
            AddPredefinedType(typeof(int), PortableUtils.TypeInt, PortableSystemHandlers.WriteHndIntTyped, PortableSystemHandlers.WriteHndInt);
            AddPredefinedType(typeof(long), PortableUtils.TypeLong, PortableSystemHandlers.WriteHndLongTyped, PortableSystemHandlers.WriteHndLong);
            AddPredefinedType(typeof(float), PortableUtils.TypeFloat, PortableSystemHandlers.WriteHndFloatTyped, PortableSystemHandlers.WriteHndFloat);
            AddPredefinedType(typeof(double), PortableUtils.TypeDouble, PortableSystemHandlers.WriteHndDoubleTyped, PortableSystemHandlers.WriteHndDouble);
            AddPredefinedType(typeof(string), PortableUtils.TypeString, PortableSystemHandlers.WriteHndStringTyped, PortableSystemHandlers.WriteHndString);
            AddPredefinedType(typeof(decimal), PortableUtils.TypeDecimal, PortableSystemHandlers.WriteHndDecimalTyped, PortableSystemHandlers.WriteHndDecimal);
            AddPredefinedType(typeof(DateTime), PortableUtils.TypeDate, PortableSystemHandlers.WriteHndDateTyped, PortableSystemHandlers.WriteHndDate);
            AddPredefinedType(typeof(Guid), PortableUtils.TypeGuid, PortableSystemHandlers.WriteHndGuidTyped, PortableSystemHandlers.WriteHndGuid);

            // TODO: Remove this registration
            AddPredefinedType(typeof(PortableUserObject), PortableUtils.TypePortable, PortableSystemHandlers.WriteHndPortableTyped, 
                PortableSystemHandlers.WriteHndPortable);

            AddPredefinedType(typeof(bool[]), PortableUtils.TypeArrayBool, PortableSystemHandlers.WriteHndBoolArrayTyped,
                PortableSystemHandlers.WriteHndBoolArray);
            AddPredefinedType(typeof(byte[]), PortableUtils.TypeArrayByte, PortableSystemHandlers.WriteHndByteArrayTyped,
                PortableSystemHandlers.WriteHndByteArray);
            AddPredefinedType(typeof(short[]), PortableUtils.TypeArrayShort, PortableSystemHandlers.WriteHndShortArrayTyped,
                PortableSystemHandlers.WriteHndShortArray);
            AddPredefinedType(typeof(char[]), PortableUtils.TypeArrayChar, PortableSystemHandlers.WriteHndCharArrayTyped,
                PortableSystemHandlers.WriteHndCharArray);
            AddPredefinedType(typeof(int[]), PortableUtils.TypeArrayInt, PortableSystemHandlers.WriteHndIntArrayTyped,
                PortableSystemHandlers.WriteHndIntArray);
            AddPredefinedType(typeof(long[]), PortableUtils.TypeArrayLong, PortableSystemHandlers.WriteHndLongArrayTyped,
                PortableSystemHandlers.WriteHndLongArray);
            AddPredefinedType(typeof(float[]), PortableUtils.TypeArrayFloat, PortableSystemHandlers.WriteHndFloatArrayTyped,
                PortableSystemHandlers.WriteHndFloatArray);
            AddPredefinedType(typeof(double[]), PortableUtils.TypeArrayDouble, PortableSystemHandlers.WriteHndDoubleArrayTyped,
                PortableSystemHandlers.WriteHndDoubleArray);
            AddPredefinedType(typeof(decimal[]), PortableUtils.TypeArrayDecimal, PortableSystemHandlers.WriteHndDecimalArrayTyped,
                PortableSystemHandlers.WriteHndDecimalArray);
            AddPredefinedType(typeof(string[]), PortableUtils.TypeArrayString, PortableSystemHandlers.WriteHndStringArrayTyped,
                PortableSystemHandlers.WriteHndStringArray);
            AddPredefinedType(typeof(DateTime?[]), PortableUtils.TypeArrayDate, PortableSystemHandlers.WriteHndDateArrayTyped,
                PortableSystemHandlers.WriteHndDateArray);
            AddPredefinedType(typeof(Guid?[]), PortableUtils.TypeArrayGuid, PortableSystemHandlers.WriteHndGuidArrayTyped,
                PortableSystemHandlers.WriteHndGuidArray);

            // Define system types. They use internal reflective stuff, so configuration doesn't affect them.
            AddSystemTypes();

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
            PortableWriterImpl writer = StartMarshal(stream);

            writer.Write(val);

            FinishMarshal(writer);
        }

        /// <summary>
        /// Start marshal session.
        /// </summary>
        /// <param name="stream">Stream.</param>
        /// <returns>Writer.</returns>
        public PortableWriterImpl StartMarshal(IPortableStream stream)
        {
            return new PortableWriterImpl(this, stream);
        }

        /// <summary>
        /// Finish marshal session.
        /// </summary>
        /// <param name="writer">Writer.</param>
        /// <returns>Dictionary with metadata.</returns>
        public void FinishMarshal(IPortableWriter writer)
        {
            var meta = ((PortableWriterImpl) writer).Metadata();

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
            return new PortableReaderImpl(this, _idToDesc, stream, mode, builder).Deserialize<T>();
        }

        /// <summary>
        /// Start unmarshal session.
        /// </summary>
        /// <param name="stream">Stream.</param>
        /// <param name="keepPortable">Whether to keep portables as portables.</param>
        /// <returns>
        /// Reader.
        /// </returns>
        public PortableReaderImpl StartUnmarshal(IPortableStream stream, bool keepPortable)
        {
            return new PortableReaderImpl(this, _idToDesc, stream,
                keepPortable ? PortableMode.KeepPortable : PortableMode.Deserialize, null);
        }

        /// <summary>
        /// Start unmarshal session.
        /// </summary>
        /// <param name="stream">Stream.</param>
        /// <param name="mode">The mode.</param>
        /// <returns>Reader.</returns>
        public PortableReaderImpl StartUnmarshal(IPortableStream stream, PortableMode mode = PortableMode.Deserialize)
        {
            return new PortableReaderImpl(this, _idToDesc, stream, mode, null);
        }
        
        /// <summary>
        /// Gets metadata for the given type ID.
        /// </summary>
        /// <param name="typeId">Type ID.</param>
        /// <returns>Metadata or null.</returns>
        public IPortableMetadata Metadata(int typeId)
        {
            if (Ignite != null)
            {
                IPortableMetadata meta = Ignite.Metadata(typeId);

                if (meta != null)
                    return meta;
            }

            return PortableMetadataImpl.EmptyMeta;
        }

        /// <summary>
        /// Gets metadata handler for the given type ID.
        /// </summary>
        /// <param name="desc">Type descriptor.</param>
        /// <returns>Metadata handler.</returns>
        public IPortableMetadataHandler MetadataHandler(IPortableTypeDescriptor desc)
        {
            PortableMetadataHolder holder;

            if (!_metas.TryGetValue(desc.TypeId, out holder))
            {
                lock (this)
                {
                    if (!_metas.TryGetValue(desc.TypeId, out holder))
                    {
                        IDictionary<int, PortableMetadataHolder> metas0 =
                            new Dictionary<int, PortableMetadataHolder>(_metas);

                        holder = desc.MetadataEnabled ? new PortableMetadataHolder(desc.TypeId,
                            desc.TypeName, desc.AffinityKeyFieldName) : null;

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

                _metas[metaEntry.Key].Merge(mergeInfo);
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

            _typeToDesc.TryGetValue(type, out desc);

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

            return _typeNameToDesc.TryGetValue(typeName, out desc) ? desc : 
                new PortableSurrogateTypeDescriptor(_cfg, typeName);
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

            return _idToDesc.TryGetValue(PortableUtils.TypeKey(userType, typeId), out desc) ? desc :
                userType ? new PortableSurrogateTypeDescriptor(_cfg, typeId) : null;
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
                ? PortableMarshalAwareSerializer.Instance 
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

            if (_idToDesc.ContainsKey(typeKey))
            {
                string type1 = _idToDesc[typeKey].Type != null ? _idToDesc[typeKey].Type.AssemblyQualifiedName : null;
                string type2 = type != null ? type.AssemblyQualifiedName : null;

                throw new PortableException("Conflicting type IDs [type1=" + type1 + ", type2=" + type2 +
                    ", typeId=" + typeId + ']');
            }

            if (userType && _typeNameToDesc.ContainsKey(typeName))
                throw new PortableException("Conflicting type name: " + typeName);

            IPortableTypeDescriptor descriptor =
                new PortableFullTypeDescriptor(type, typeId, typeName, userType, nameMapper, idMapper, serializer,
                    metaEnabled, keepDeserialized, affKeyFieldName, typedHandler, untypedHandler);

            if (type != null)
                _typeToDesc[type] = descriptor;

            if (userType)
                _typeNameToDesc[typeName] = descriptor;

            _idToDesc[typeKey] = descriptor;            
        }

        /// <summary>
        /// Adds a predefined system type.
        /// </summary>
        private void AddSystemType<T>(byte typeId, Func<PortableReaderImpl, T> ctor) where T : IPortableWriteAware
        {
            var type = typeof(T);

            var serializer = new PortableSystemTypeSerializer<T>(ctor);

            AddType(type, typeId, GetTypeName(type), false, false, false, null, null, serializer, null, null, null);
        }

        /// <summary>
        /// Adds predefined system types.
        /// </summary>
        private void AddSystemTypes()
        {
            AddSystemType(PortableUtils.TypeNativeJobHolder, w => new ComputeJobHolder(w));
            AddSystemType(PortableUtils.TypeComputeJobWrapper, w => new ComputeJobWrapper(w));
            AddSystemType(PortableUtils.TypePortableJobResHolder, w => new PortableResultWrapper(w));
            AddSystemType(PortableUtils.TypeDotNetCfg, w => new InteropDotNetConfiguration(w));
            AddSystemType(PortableUtils.TypeDotNetPortableCfg, w => new InteropDotNetPortableConfiguration(w));
            AddSystemType(PortableUtils.TypeDotNetPortableTypCfg, w => new InteropDotNetPortableTypeConfiguration(w));
            AddSystemType(PortableUtils.TypeIgniteProxy, w => new IgniteProxy());
            AddSystemType(PortableUtils.TypeComputeOutFuncJob, w => new ComputeOutFuncJob(w));
            AddSystemType(PortableUtils.TypeComputeOutFuncWrapper, w => new ComputeOutFuncWrapper(w));
            AddSystemType(PortableUtils.TypeComputeFuncWrapper, w => new ComputeFuncWrapper(w));
            AddSystemType(PortableUtils.TypeComputeFuncJob, w => new ComputeFuncJob(w));
            AddSystemType(PortableUtils.TypeComputeActionJob, w => new ComputeActionJob(w));
            AddSystemType(PortableUtils.TypeContinuousQueryRemoteFilterHolder, w => new ContinuousQueryFilterHolder(w));
            AddSystemType(PortableUtils.TypeSerializableHolder, w => new SerializableObjectHolder(w));
            AddSystemType(PortableUtils.TypeCacheEntryProcessorHolder, w => new CacheEntryProcessorHolder(w));
            AddSystemType(PortableUtils.TypeCacheEntryPredicateHolder, w => new CacheEntryFilterHolder(w));
            AddSystemType(PortableUtils.TypeMessageFilterHolder, w => new MessageFilterHolder(w));
            AddSystemType(PortableUtils.TypePortableOrSerializableHolder, w => new PortableOrSerializableObjectHolder(w));
            AddSystemType(PortableUtils.TypeStreamReceiverHolder, w => new StreamReceiverHolder(w));
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
