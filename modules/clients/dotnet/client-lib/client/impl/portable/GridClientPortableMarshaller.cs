/* @csharp.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Client.Impl.Portable
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.IO;
    using System.Reflection;
    using GridGain.Client.Impl.Message;
    using GridGain.Client.Portable;    

    using PU = GridGain.Client.Impl.Portable.GridClientPortableUilts;

    /** <summary>Portable marshaller implementation.</summary> */
    internal class GridClientPortableMarshaller
    {
        /** Predefeined system types. */
        private static readonly ISet<Type> SYS_TYPES = new HashSet<Type>();

        /** Type to descriptor map. */
        private IDictionary<Type, GridClientPortableTypeDescriptor> typeToDesc = 
            new Dictionary<Type, GridClientPortableTypeDescriptor>();

        /** ID to descriptor map. */
        private IDictionary<long, GridClientPortableTypeDescriptor> idToDesc = 
            new Dictionary<long, GridClientPortableTypeDescriptor>();

        /**
         * <summary>Static initializer.</summary>
         */
        static GridClientPortableMarshaller()
        {
            SYS_TYPES.Add(typeof(GridClientAuthenticationRequest));
            SYS_TYPES.Add(typeof(GridClientTopologyRequest));
            SYS_TYPES.Add(typeof(GridClientTaskRequest));
            SYS_TYPES.Add(typeof(GridClientCacheRequest));
            SYS_TYPES.Add(typeof(GridClientLogRequest));
            SYS_TYPES.Add(typeof(GridClientResponse));
            SYS_TYPES.Add(typeof(GridClientNodeBean));
            SYS_TYPES.Add(typeof(GridClientNodeMetricsBean));
            SYS_TYPES.Add(typeof(GridClientTaskResultBean));
        }

        /**
         * <summary>Constructor.</summary>
         * <param name="cfg">Configurtaion.</param>
         */
        public GridClientPortableMarshaller(GridClientPortableConfiguration cfg) 
        {
            // 1. Validation.
            if (cfg == null)
                cfg = new GridClientPortableConfiguration();

            if (cfg.TypeConfigurations == null)
                cfg.TypeConfigurations = new List<GridClientPortableTypeConfiguration>();

            foreach (GridClientPortableTypeConfiguration typeCfg in cfg.TypeConfigurations)
            {
                if (typeCfg.TypeName == null || typeCfg.TypeName.Length == 0)
                    throw new GridClientPortableException("Type name cannot be null or empty: " + typeCfg);

                if (typeCfg.AssemblyName != null && typeCfg.AssemblyName.Length == 0)
                    throw new GridClientPortableException("Assembly name cannot be empty: " + typeCfg);

                if (typeCfg.AssemblyVersion != null && typeCfg.AssemblyVersion.Length == 0)
                    throw new GridClientPortableException("Assembly version cannot be empty: " + typeCfg);

                if (typeCfg.AssemblyVersion != null && typeCfg.AssemblyName == null)
                    throw new GridClientPortableException("Assembly version cannot be set when assembly " + 
                        "name is null: " + typeCfg);
            }

            // 2. Create default serializers and mappers.
            GridClientPortableReflectiveIdResolver refMapper = new GridClientPortableReflectiveIdResolver();
            GridClientPortableReflectiveSerializer refSerializer = new GridClientPortableReflectiveSerializer();

            if (cfg.DefaultIdMapper == null)
                cfg.DefaultIdMapper = refMapper;

            if (cfg.DefaultSerializer == null)
                cfg.DefaultSerializer = refSerializer;

            // 1. Define system types. They use internal reflective serializer, so configuration doesn't affect them.
            foreach (Type sysType in SYS_TYPES)
                addSystemType(sysType, refMapper, refSerializer);
            
            // 2. Define user types.
            ICollection<GridClientPortableTypeConfiguration> typeCfgs = cfg.TypeConfigurations;

            if (typeCfgs != null)
            {
                foreach (GridClientPortableTypeConfiguration typeCfg in typeCfgs)
                    addUserType(cfg, typeCfg, refMapper, refSerializer);
            }
        }

        /**
         * <summary>Marhshal object</summary>
         * <param name="val">Value.</param>
         * <returns>Serialized data as byte array.</returns>
         */
        public byte[] Marshal(object val)
        {
            MemoryStream stream = new MemoryStream();

            Marshal(val, stream);

            return stream.ToArray();
        }

        /**
         * <summary>Marhshal object</summary>
         * <param name="val">Value.</param>
         * <param name="stream">Output stream.</param>
         */
        public void Marshal(object val, Stream stream)
        {
            new GridClientPortableWriteContext(typeToDesc, stream).Write(val);
        }

        /**
         * <summary>Unmarshal object.</summary>
         * <param name="data">Raw data.</param>
         * <returns>Portable object.</returns>
         */
        public IGridClientPortableObject Unmarshal(byte[] data)
        {
            return Unmarshal(new MemoryStream(data));
        }

        /**
         * <summary>Unmarshal object.</summary>
         * <param name="input">Stream.</param>
         * <returns>Unmarshalled object.</returns>
         */ 
        public IGridClientPortableObject Unmarshal(MemoryStream input)
        {
            long pos = input.Position;

            byte hdr = (byte)input.ReadByte();

            if (hdr == PU.HDR_NULL)
                return null;
            else if (hdr == PU.HDR_META)
            {
                throw new NotImplementedException();
            }
            
            // Reading full object.
            if (hdr != PU.HDR_FULL)
                throw new GridClientPortableException("Unexpected header: " + hdr);

            // Read header.
            bool userType = PU.ReadBoolean(input);
            int typeId = PU.ReadInt(input);
            int hashCode = PU.ReadInt(input);
            int len = PU.ReadInt(input);
            int rawDataOffset = PU.ReadInt(input);

            // Read fields.
            Dictionary<int, int> fields = null;

            if (userType) 
            { 
                long curPos = input.Position;
                long endPos = pos + (rawDataOffset == 0 ? len : rawDataOffset);

                while (curPos < endPos)
                {
                    int fieldPos = (int)(input.Position - pos);
                    int fieldId = PU.ReadInt(input);
                    int fieldLen = PU.ReadInt(input);

                    Console.WriteLine("Read field: " + fieldId + " " + fieldLen + " " + fieldPos);

                    if (fields == null)
                        fields = new Dictionary<int, int>();

                    if (fields.ContainsKey(fieldId))
                        throw new GridClientPortableException("Object contains duplicate field IDs [userType=" + 
                            userType + ", typeId=" + typeId + ", fieldId=" + fieldId + ']');

                    fields[fieldId] = fieldPos;

                    input.Seek(fieldLen, SeekOrigin.Current);

                    curPos = input.Position;
                }
            }

            input.Seek(pos + len, SeekOrigin.Begin); // Position input after read data.
            
            return new GridClientPortableObjectImpl(this, input.ToArray(), (int)pos, len, userType, typeId, 
                hashCode, rawDataOffset, fields);
        }

        /**
         * <summary>ID to descriptor map.</summary>
         */ 
        public IDictionary<long, GridClientPortableTypeDescriptor> IdToDescriptor
        {
            get { return idToDesc; }
        }

        /**
         * <summary>Add system type.</summary>
         * <param name="type">Type.</param>
         * <param name="refMapper">Reflective mapper.</param>
         * <param name="refSerializer">Reflective serializer.</param>
         */
        private void addSystemType(Type type, GridClientPortableReflectiveIdResolver refMapper, 
            GridClientPortableReflectiveSerializer refSerializer)
        {
            refMapper.Register(type);

            int typeId = refMapper.TypeId(type).Value;

            refSerializer.Register(type, typeId, refMapper);

            addType(type, typeId, false, refMapper, refSerializer);
        }

        /**
         * <summary>Add user type.</summary>
         * <param name="cfg">Configuration.</param>
         * <param name="typeCfg">Type configuration.</param>
         * <param name="refMapper">Reflective mapper.</param>
         * <param name="refSerializer">Reflective serializer.</param>
         */
        private void addUserType(GridClientPortableConfiguration cfg, GridClientPortableTypeConfiguration typeCfg, 
            GridClientPortableReflectiveIdResolver refMapper, GridClientPortableReflectiveSerializer refSerializer) 
        {
            // 1. Get type.
            Type type = GetType(typeCfg);
            
            // 2. Detect mapper and serializer.
            GridClientPortableIdResolver mapper = null;
            IGridClientPortableSerializer serializer = null;
            
            // 2.1. Type configuration has the highest priority.
            if (typeCfg.IdMapper != null) 
                mapper = typeCfg.IdMapper;
            if (typeCfg.Serializer != null)
                serializer = typeCfg.Serializer;

            // 2.2. Try checking annotation.
            if (mapper == null || serializer == null)
            {
                object[] attrs = type.GetCustomAttributes(typeof(GridClientPortableType), false);

                if (attrs.Length > 0)
                {
                    GridClientPortableType typeDesc = (GridClientPortableType)attrs[0];

                    if (mapper == null && typeDesc.IdMapperClass != null)
                    {
                        try
                        {
                            mapper = (GridClientPortableIdResolver)Activator.CreateInstance(
                                Type.GetType(typeDesc.IdMapperClass));
                        }
                        catch (Exception e)
                        {
                            throw new GridClientPortableException("Failed to instantiate ID mapper [type=" +
                                typeCfg.TypeName + ", idMapperClass=" + typeDesc.IdMapperClass + ']', e);
                        }
                    }

                    if (serializer == null && typeDesc.SerializerClass != null)
                    {
                        try
                        {
                            serializer = (IGridClientPortableSerializer)Activator.CreateInstance(
                                Type.GetType(typeDesc.SerializerClass));
                        }
                        catch (Exception e)
                        {
                            throw new GridClientPortableException("Failed to instantiate serializer [type=" +
                                typeCfg.TypeName + ", idMapperClass=" + typeDesc.SerializerClass + ']', e);
                        }
                    }
                }
            }

            // 2.3. Delegate to defaults if necessary.
            if (mapper == null)
                mapper = cfg.DefaultIdMapper;

            if (serializer == null)
                serializer = cfg.DefaultSerializer;

            // 2.4. Merge reflective stuff if necessary.
            if (mapper is GridClientPortableReflectiveIdResolver)
            {                
                refMapper.Register(type);

                mapper = refMapper;
            }

            int? typeIdRef = mapper.TypeId(type);

            int typeId = typeIdRef.HasValue ? typeIdRef.Value : PU.StringHashCode(typeCfg.TypeName.ToLower());

            if (serializer is GridClientPortableReflectiveSerializer)
            {
                refSerializer.Register(type, typeId, mapper);
                
                serializer = refSerializer;
            }

            addType(type, typeId, true, mapper, serializer); 
        }

        /**
         * <summary>Add type.</summary>
         * <param name="type">Type.</param>
         * <param name="typeId">Type ID.</param>
         * <param name="userType">User type flag.</param>
         * <param name="mapper">ID mapper.</param>
         * <param name="serializer">Serializer.</param>
         */
        private void addType(Type type, int typeId, bool userType, GridClientPortableIdResolver mapper, 
            IGridClientPortableSerializer serializer)
        {
            long typeKey = PU.TypeKey(userType, typeId);

            if (idToDesc.ContainsKey(typeKey))
                throw new GridClientPortableException("Conflicting type IDs [type1=" +
                        idToDesc[typeKey].Type.AssemblyQualifiedName + ", type2=" + type.AssemblyQualifiedName +
                        ", typeId=" + typeId + ']');

            GridClientPortableTypeDescriptor descriptor = 
                new GridClientPortableTypeDescriptor(type, typeId, userType, mapper, serializer);

            typeToDesc[type] = descriptor;
            idToDesc[typeKey] = descriptor;
        }

        /**
         * <summary>Gets type for type configuration.</summary>
         * <param name="typeCfg">Type configuration.</param>
         * <returns>Type.</returns>
         */
        private Type GetType(GridClientPortableTypeConfiguration typeCfg)
        {
            foreach (Assembly assembly in AppDomain.CurrentDomain.GetAssemblies())
            {
                if (typeCfg.AssemblyName != null && !typeCfg.AssemblyName.Equals(assembly.GetName().Name))
                    continue;

                if (typeCfg.AssemblyVersion != null && !typeCfg.AssemblyVersion.Equals(assembly.GetName().Version.ToString()))
                    continue;

                Type type = assembly.GetType(typeCfg.TypeName, false, false);

                if (type != null)
                    return type;
            }

            throw new GridClientPortableException("Cannot find type for type configuration: " + typeCfg);
        }
    }    
}
