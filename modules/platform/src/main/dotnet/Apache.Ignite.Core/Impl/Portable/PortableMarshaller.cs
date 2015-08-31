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
        private readonly PortableConfiguration _cfg;

        /** Ignite context. */
        private readonly IIgniteContext _igniteContext;

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
        /// Initializes a new instance of the <see cref="PortableMarshaller"/> class.
        /// </summary>
        internal PortableMarshaller(PortableConfiguration cfg = null)
            : this(cfg, typeof(PortableUserObject), new IgniteContext())
        {
            // No-op.
        }

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="cfg">Configurtaion.</param>
        /// <param name="portableObjectType">Type of the portable object.</param>
        /// <param name="igniteContext">The context.</param>
        /// <param name="defaultSerializer">The default serializer.</param>
        /// <exception cref="PortableException">
        /// Type name cannot be null or empty:  + typeCfg
        /// or
        /// Assembly name cannot be empty string:  + typeCfg
        /// </exception>
        public PortableMarshaller(PortableConfiguration cfg, Type portableObjectType, IIgniteContext igniteContext, 
            IPortableSerializerEx defaultSerializer = null)
        {
            Debug.Assert(portableObjectType != null);
            Debug.Assert(igniteContext != null);

            _igniteContext = igniteContext;

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

            AddPredefinedType(portableObjectType, PortableUtils.TypePortable, PortableSystemHandlers.WriteHndPortableTyped, 
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

            // 2. Define user types.
            defaultSerializer = defaultSerializer ?? new PortableReflectiveSerializer(_igniteContext);
            var dfltSerializer = cfg.DefaultSerializer == null ? defaultSerializer : null;

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
        /// Gets the Ignite context.
        /// </summary>
        public IIgniteContext IgniteContext
        {
            get { return _igniteContext; }
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
            return CreateWriter(stream);
        }

        /// <summary>
        /// Finish marshal session.
        /// </summary>
        /// <param name="writer">Writer.</param>
        /// <returns>Dictionary with metadata.</returns>
        public virtual void FinishMarshal(IPortableWriterEx writer)
        {
            var meta = writer.Metadata;

            if (meta != null && meta.Count > 0)
                // TODO: Send metadata
                OnMetadataSent(meta);
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
        public T Unmarshal<T>(IPortableStream stream, PortableMode mode, IPortableBuilderEx builder)
        {
            return CreateReader(_idToDesc, stream, mode, builder).Deserialize<T>();
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
            return CreateReader(_idToDesc, stream, keepPortable ? PortableMode.KeepPortable : PortableMode.Deserialize,
                null);
        }

        /// <summary>
        /// Start unmarshal session.
        /// </summary>
        /// <param name="stream">Stream.</param>
        /// <param name="mode">The mode.</param>
        /// <returns>Reader.</returns>
        public IPortableReaderEx StartUnmarshal(IPortableStream stream, PortableMode mode = PortableMode.Deserialize)
        {
            return CreateReader(_idToDesc, stream, mode, null);
        }
        /// <summary>
        /// Gets metadata for the given type ID.
        /// </summary>
        /// <param name="typeId">Type ID.</param>
        /// <returns>Metadata or null.</returns>
        public virtual IPortableMetadata GetMetadata(int typeId)
        {
            PortableMetadataHolder result;

            // TODO: Get from grid.
            if (_metas.TryGetValue(typeId, out result))
                return result.Metadata();

            // Empty meta:
            return new PortableMetadataImpl(PortableUtils.TypeObject, PortableTypeNames.TypeNameObject, null, null,
                _igniteContext);
        }

        /// <summary>
        /// Gets all metadata.
        /// </summary>
        internal ICollection<IPortableMetadata> GetAllMetadata()
        {
            // TODO: This is a workaround for tests. Remove as soon as Grid is merged to Ignite.
            return _metas.Values.Where(x => x != null).Select(x => x.Metadata()).ToArray();
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
                            desc.TypeName, desc.AffinityKeyFieldName, _igniteContext) : null;

                        metas0[desc.TypeId] = holder;

                        _metas = metas0;
                    }
                }
            }

            if (holder == null) 
                return null;
            
            var ids = holder.FieldIds();

            bool newType = ids.Count == 0 && !holder.Saved();

            return new PortableHashsetMetadataHandler(ids, newType);
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
                    int fieldId = PortableUtils.FieldId(metaEntry.Key, fieldMeta.Key, null, null, _igniteContext);

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
                new PortableSurrogateTypeDescriptor(_cfg, typeName, _igniteContext);
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

            return _idToDesc.TryGetValue(PortableUtils.GetTypeKey(userType, typeId), out desc) ? desc :
                userType ? new PortableSurrogateTypeDescriptor(_cfg, typeId) : null;
        }

        /// <summary>
        /// Check whether the given object is portable, i.e. it can be 
        /// serialized with portable marshaller.
        /// </summary>
        /// <param name="obj">Object.</param>
        /// <returns>True if portable.</returns>
        public bool IsPortable(object obj)
        {
            if (obj == null)
                return true;

            // We assume object as portable only in case it has descriptor.
            // Collections, Enums and non-primitive arrays do not have descriptors
            // and this is fine here because we cannot know whether their members
            // are portable.
            return Descriptor(obj.GetType()) != null;
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

                int typeId = PortableUtils.TypeId(typeName, nameMapper, idMapper, _igniteContext);

                var serializer = typeCfg.Serializer ?? cfg.DefaultSerializer
                                 ?? GetPortableMarshalAwareSerializer(type) ?? dfltSerializer;

                var refSerializer = serializer as IPortableSerializerEx;

                if (refSerializer != null)
                    refSerializer.Register(type, typeId, nameMapper, idMapper);

                AddType(type, typeId, typeName, true, metaEnabled, keepDeserialized, nameMapper, idMapper, serializer,
                    typeCfg.AffinityKeyFieldName, null, null);
            }
            else
            {
                // Type is not found.
                string typeName = PortableUtils.SimpleTypeName(typeCfg.TypeName);

                int typeId = PortableUtils.TypeId(typeName, nameMapper, idMapper, _igniteContext);

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
            long typeKey = PortableUtils.GetTypeKey(userType, typeId);

            if (_idToDesc.ContainsKey(typeKey))
            {
                string type1 = _idToDesc[typeKey].Type != null ? _idToDesc[typeKey].Type.AssemblyQualifiedName : null;
                string type2 = type != null ? type.AssemblyQualifiedName : null;

                throw _igniteContext.ConvertException(new PortableException("Conflicting type IDs [type1=" + type1 + ", type2=" + type2 +
                    ", typeId=" + typeId + ']'));
            }

            if (userType && _typeNameToDesc.ContainsKey(typeName))
                throw _igniteContext.ConvertException(new PortableException("Conflicting type name: " + typeName));

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
        protected void AddSystemType<T>(byte typeId, Func<IPortableReaderEx, T> ctor) where T : IPortableWriteAware
        {
            var type = typeof(T);

            var serializer = new PortableSystemTypeSerializer<T>(ctor);

            AddType(type, typeId, GetTypeName(type), false, false, false, null, null, serializer, null, null, null);
        }

        /// <summary>
        /// Creates reader for unmarshalling.
        /// </summary>
        /// <param name="descs">The descs.</param>
        /// <param name="stream">The stream.</param>
        /// <param name="mode">The mode.</param>
        /// <param name="builder">The builder.</param>
        /// <returns>Reader.</returns>
        protected virtual IPortableReaderEx CreateReader(IDictionary<long, IPortableTypeDescriptor> descs,
            IPortableStream stream, PortableMode mode, IPortableBuilderEx builder)
        {
            return new PortableReaderImpl(this, _idToDesc, stream, mode, builder);
        }

        /// <summary>
        /// Creates writer for marshalling.
        /// </summary>
        /// <param name="stream">The stream.</param>
        /// <returns>Writer.</returns>
        protected virtual IPortableWriterEx CreateWriter(IPortableStream stream)
        {
            return new PortableWriterImpl(this, stream);
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
