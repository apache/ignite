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
    using GridGain.Client.Impl.Message;
    using GridGain.Client.Portable;    

    using PU = GridGain.Client.Impl.Portable.GridClientPortableUilts;

    /** <summary>Portable marshaller implementation.</summary> */
    internal class GridClientPortableMarshaller
    {
        /** Header of NULL object. */
        public const byte HDR_NULL = 0x80;

        /** Header of object handle. */
        public const byte HDR_HND = 0x81;

        /** Header of object in fully serialized form. */
        public const byte HDR_FULL = 0x82;

        /** Header of object in fully serailized form with metadata. */
        public const byte HDR_META = 0x83;

        /** Byte array of size 8. */
        private static readonly byte[] BYTE_8;

        /** Predefeined system types. */
        private static readonly ISet<Type> SYS_TYPES = new HashSet<Type>();

        /** Descriptors. */
        public IDictionary<string, GridClientPortableTypeDescriptor> descs = new Dictionary<string, GridClientPortableTypeDescriptor>();

        /**
         * <summary>Static initializer.</summary>
         */
        static GridClientPortableMarshaller()
        {
            BYTE_8 = new byte[8];

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
         * <summary>Add system type.</summary>
         * <param name="type">Type.</param>
         * <param name="refMapper">Reflective mapper.</param>
         * <param name="refSerializer">Reflective serializer.</param>
         */
        private void addSystemType(Type type, GridClientPortableReflectiveIdResolver refMapper, 
            GridClientPortableReflectiveSerializer refSerializer)
        {
            refMapper.Register(type.FullName);

            int typeId = refMapper.TypeId(type.FullName).Value;

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
            // 1. Validation.
            if (typeCfg.TypeName == null)
                throw new GridClientPortableException("Type name cannot be null: " + typeCfg);

            Type type;

            try
            {
                type = Type.GetType(typeCfg.TypeName);
            }
            catch (Exception e)
            {
                throw new GridClientPortableException("Type with the given name is not found: " + typeCfg.TypeName, e);
            }

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
                refMapper.Register(typeCfg.TypeName);

                mapper = refMapper;
            }

            int? typeIdRef = mapper.TypeId(typeCfg.TypeName);

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
        private void addType(Type type, int typeId, bool userType, GridClientPortableIdResolver mapper, IGridClientPortableSerializer serializer)
        {
            foreach (KeyValuePair<string, GridClientPortableTypeDescriptor> desc in descs)
            {
                if (desc.Value.TypeId == typeId)
                    throw new GridClientPortableException("Conflicting type IDs [type1=" + desc.Key +
                        ", type2=" + type.FullName + ", typeId=" + typeId + ']');
            }

            descs[type.FullName] = new GridClientPortableTypeDescriptor(typeId, false, mapper, serializer);
        }

        /**
         * <summary>Marhshal object</summary>
         * <param name="val">Value.</param>
         * <param name="ctx">Serialization context.</param>
         * <returns>Serialized data as byte array.</returns>
         */
        public byte[] Marshal(object val, GridClientPortableSerializationContext ctx)
        {
            MemoryStream stream = new MemoryStream();

            Marshal(val, stream, ctx);

            return stream.ToArray();
        }

        /**
         * <summary>Marhshal object</summary>
         * <param name="val">Value.</param>
         * <param name="stream">Output stream.</param>
         * <param name="ctx">Serialization context.</param>
         */
        public void Marshal(object val, Stream stream, GridClientPortableSerializationContext ctx)
        {
            new WriteContext(ctx, descs, stream).Write(val);
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

            if (hdr == HDR_NULL)
                return null;
            else if (hdr == HDR_META)
            {
                throw new NotImplementedException();
            }
            
            // Reading full object.
            if (hdr != HDR_FULL)
                throw new GridClientPortableException("Unexpected header: " + hdr);

            // Read header.
            bool userType = PU.ReadBoolean(input);
            int typeId = PU.ReadInt(input);
            int hashCode = PU.ReadInt(input);
            int len = PU.ReadInt(input);
            int rawDataOffset = PU.ReadInt(input);

            // Read fields.
            HashSet<int> fields = null;

            if (!userType) 
            { 
                long curPos = input.Position;
                long endPos = pos + (rawDataOffset == 0 ? len : rawDataOffset);

                while (curPos < endPos)
                {
                    int fieldId = PU.ReadInt(input);
                    int fieldLen = PU.ReadInt(input);

                    if (fields == null)
                        fields = new HashSet<int>();

                    fields.Add(fieldId);

                    input.Seek(fieldLen, SeekOrigin.Current);
                }
            }

            input.Seek(pos + len, SeekOrigin.Begin); // Position input after read data.

            return new GridClientPortableObjectImpl(this, input.GetBuffer(), (int)pos, len, userType, typeId, hashCode, rawDataOffset, fields);
        }

        /**
         * <summary>Gets type name by ID.</summary>
         * <param name="typeId">Type ID.</param>
         * <param name="userType">User type flag.</param>
         */ 
        public string TypeName(int typeId, bool userType)
        {
            throw new NotImplementedException();
        }

        

        /**
         * <summary>Write context.</summary>
         */ 
        private class WriteContext 
        {
            /** Per-connection serialization context. */
            private readonly GridClientPortableSerializationContext ctx;

            /** Type descriptors. */
            private readonly IDictionary<string, GridClientPortableTypeDescriptor> descs;

            /** Output. */
            private readonly Stream stream;

            /** Wrier. */
            private readonly Writer writer;
                        
            /** Handles. */
            private IDictionary<GridClientPortableObjectHandle, int> hnds;
                        
            /** Current object handle number. */
            private int curHndNum;

            /**
             * <summary>Constructor.</summary>
             * <param name="ctx">Client connection context.</param>
             * <param name="descs">Type descriptors.</param>
             * <param name="stream">Output stream.</param>
             */
            public WriteContext(GridClientPortableSerializationContext ctx, IDictionary<string, GridClientPortableTypeDescriptor> descs, Stream stream)
            {
                this.ctx = ctx;
                this.descs = descs;
                this.stream = stream;

                writer = new Writer(this);
            }

            /**
             * <summary>Write object to the context.</summary>
             * <param name="obj">Object.</param>
             */ 
            public void Write(object obj)
            {
                // 1. Write null.
                if (obj == null)
                {
                    stream.WriteByte(HDR_NULL);

                    return;
                }
                
                // 2. Write primitive.
                Type type = obj.GetType();

                int typeId = PU.PrimitiveTypeId(type);

                if (typeId != 0)
                {
                    stream.WriteByte(HDR_FULL);

                    PU.WriteBoolean(false, stream);
                    PU.WriteInt(typeId, stream);
                    PU.WritePrimitive(typeId, obj, stream);

                    return;
                }

                // 3. Try interpreting object as handle.
                if (hnds == null)
                    hnds = new Dictionary<GridClientPortableObjectHandle, int>();

                GridClientPortableObjectHandle hnd = new GridClientPortableObjectHandle(obj);

                int hndNum;
                
                if (hnds.TryGetValue(hnd, out hndNum)) {
                    stream.WriteByte(HDR_HND);

                    PU.WriteInt(hndNum, stream);

                    return;
                }
                else 
                    hnds.Add(hnd, curHndNum++);

                // 4. Complex object with handle, remember position.
                long pos = stream.Position; 

                // 4. Write string.
                dynamic obj0 = obj;

                if (type == typeof(string)) 
                {
                    stream.WriteByte(HDR_FULL);
                    PU.WriteBoolean(false, stream);                    
                    PU.WriteInt(PU.TYPE_STRING, stream);
                    PU.WriteInt(PU.StringHashCode(obj0), stream);
                    PU.WriteByteArray(BYTE_8, stream);
                    PU.WriteString(obj0, stream);

                    WriteLength(stream, pos, stream.Position, 0);

                    return;
                }

                // 5. Write GUID.
                if (type == typeof(Guid))
                {
                    stream.WriteByte(HDR_FULL);
                    PU.WriteBoolean(false, stream);    
                    PU.WriteInt(PU.TYPE_GUID, stream);
                    PU.WriteInt(PU.GuidHashCode(obj0), stream);
                    PU.WriteByteArray(BYTE_8, stream);
                    PU.WriteGuid(obj0, stream);

                    WriteLength(stream, pos, stream.Position, 0);

                    return;
                }

                // 6. Write enum.

                // 7. Write primitive array.
                typeId = PU.PrimitiveArrayTypeId(type);

                if (typeId != 0) 
                {
                    stream.WriteByte(HDR_FULL);
                    PU.WriteBoolean(false, stream);    
                    PU.WriteInt(typeId, stream);

                    // TODO: GG-8535: Implement.
                }

                // 9. Write collection.
                // TODO: GG-8535: Implement.

                // 10. Write map.
                // TODO: GG-8535: Implement.

                // 8. Write object array.
                // TODO: GG-8535: Implement.

                // 11. Just object.
                GridClientPortableTypeDescriptor desc = descs[type.Name];

                if (desc == null)
                    throw new GridClientPortableException("Unsupported object type [type=" + type + ", object=" + obj + ']');

                stream.WriteByte(HDR_FULL);
                PU.WriteBoolean(desc.UserType, stream);
                PU.WriteInt(desc.TypeId, stream);
                PU.WriteInt(obj.GetHashCode(), stream);
                PU.WriteByteArray(BYTE_8, stream);

                Frame oldFrame = CurrentFrame;

                CurrentFrame = new Frame(desc.TypeId, desc.Mapper);

                desc.Serializer.WritePortable(obj, writer);
                
                WriteLength(stream, pos, stream.Position, CurrentFrame.RawPosition);

                CurrentFrame = oldFrame;
            }

            /**
             * <summary>Current frame.</summary>
             */ 
            public Frame CurrentFrame
            {
                get;
                set;
            }

            /**
             * <summary>Stream.</summary>
             */ 
            public Stream Stream 
            {
                get
                {
                    return stream;
                }
            }
        }

        /**
         * <summary>Write lengths.</summary>
         * <param name="stream"></param>
         * <param name="pos"></param>
         * <param name="retPos"></param>
         * <param name="rawPos">Raw position.</param>
         */
        private static void WriteLength(Stream stream, long pos, long retPos, long rawPos) {
            stream.Seek(pos + 10, SeekOrigin.Begin);

            PU.WriteInt((int)(retPos - pos), stream);

            if (rawPos != 0)
                PU.WriteInt((int)(retPos - rawPos), stream);

            stream.Seek(retPos, SeekOrigin.Begin);
        }

        /**
         * <summary>Serialization frame.</summary>
         */ 
        private class Frame 
        {
            /**
             * <summary>Constructor.</summary>
             * <param name="typeId">Type ID.</param>
             * <param name="mapper">Field mapper.</param>
             */
            public Frame(int typeId, GridClientPortableIdResolver mapper)
            {
                TypeId = typeId;
                Mapper = mapper;
            }

            /**
             * <summary>Type ID.</summary>
             */ 
            public int TypeId
            {
                get;
                private set;
            }

            /**
             * <summary>ID mapper.</summary>
             */ 
            public GridClientPortableIdResolver Mapper
            {
                get;
                private set;
            }

            /**
             * <summary>Raw mode.</summary>
             */
            public bool Raw
            {
                get;
                set;
            }

            /**
             * <summary>Raw position.</summary>
             */ 
            public long RawPosition 
            {
                get;
                set;
            }
        }

        /**
         * <summary>Writer.</summary>
         */ 
        private class Writer : IGridClientPortableWriter, IGridClientPortableRawWriter
        {
            /** Context. */
            private readonly WriteContext ctx;

            /**
             * <summary>Constructor.</summary>
             * <param name="ctx">Context.</param>
             */ 
            public Writer(WriteContext ctx)
            {
                this.ctx = ctx;
            }

            /** <inheritdoc /> */
            public IGridClientPortableRawWriter RawWriter()
            {
                MarkRaw();

                return this;
            }

            private Frame Frame
            {
                get;
                set;
            }

            /** <inheritdoc /> */
            public void WriteBoolean(string fieldName, bool val)
            {
                ObtainFrame();

                WriteField(fieldName);

                PU.WriteInt(1, ctx.Stream);

                WriteBoolean(val);

                RestoreFrame();
            }

            /** <inheritdoc /> */
            public void WriteBoolean(bool val)
            {
                PU.WriteBoolean(val, ctx.Stream);
            }

            /** <inheritdoc /> */
            public void WriteBooleanArray(string fieldName, bool[] val)
            {
                ObtainFrame();

                WriteField(fieldName);

                PU.WriteInt(1 + (val == null ? 0 : val.Length), ctx.Stream);

                WriteBooleanArray(val);
            }

            /** <inheritdoc /> */
            public void WriteBooleanArray(bool[] val)
            {
                PU.WriteBooleanArray(val, ctx.Stream);
            }

            /** <inheritdoc /> */
            public void WriteByte(string fieldName, byte val)
            {
                ObtainFrame();

                WriteField(fieldName);

                PU.WriteInt(1, ctx.Stream);

                WriteByte(val);
            }

            /** <inheritdoc /> */
            public void WriteByte(byte val)
            {
                ctx.Stream.WriteByte(val);
            }

            /** <inheritdoc /> */
            public void WriteByteArray(string fieldName, byte[] val)
            {
                ObtainFrame();

                WriteField(fieldName);

                PU.WriteInt(1 + (val == null ? 0 : val.Length), ctx.Stream);
                
                WriteByteArray(val);
            }

            /** <inheritdoc /> */
            public void WriteByteArray(byte[] val)
            {
                PU.WriteByteArray(val, ctx.Stream);
            }

            /** <inheritdoc /> */
            public void WriteShort(string fieldName, short val)
            {
                ObtainFrame();

                WriteField(fieldName);

                PU.WriteInt(2, ctx.Stream);
                
                WriteShort(val);
            }

            /** <inheritdoc /> */
            public void WriteShort(short val)
            {
                PU.WriteShort(val, ctx.Stream);
            }

            /** <inheritdoc /> */
            public void WriteShortArray(string fieldName, short[] val)
            {
                ObtainFrame();

                WriteField(fieldName);

                PU.WriteInt(1 + (val == null ? 0 : 2 * val.Length), ctx.Stream);
                
                WriteShortArray(val);
            }

            /** <inheritdoc /> */
            public void WriteShortArray(short[] val)
            {
                PU.WriteShortArray(val, ctx.Stream);
            }

            /** <inheritdoc /> */
            public void WriteInt(string fieldName, int val)
            {
                ObtainFrame();

                PU.WriteInt(4, ctx.Stream);
                
                WriteInt(val);
            }

            /** <inheritdoc /> */
            public void WriteInt(int val)
            {
                PU.WriteInt(val, ctx.Stream);
            }

            /** <inheritdoc /> */
            public void WriteIntArray(string fieldName, int[] val)
            {
                ObtainFrame();

                WriteField(fieldName);

                PU.WriteInt(1 + (val == null ? 0 : 4 * val.Length), ctx.Stream);
                
                WriteIntArray(val);
            }

            /** <inheritdoc /> */
            public void WriteIntArray(int[] val)
            {
                PU.WriteIntArray(val, ctx.Stream);
            }

            /** <inheritdoc /> */
            public void WriteLong(string fieldName, long val)
            {
                ObtainFrame();

                WriteField(fieldName);

                PU.WriteInt(8, ctx.Stream);
                
                WriteLong(val);
            }

            /** <inheritdoc /> */
            public void WriteLong(long val)
            {
                PU.WriteLong(val, ctx.Stream);
            }

            /** <inheritdoc /> */
            public void WriteLongArray(string fieldName, long[] val)
            {
                ObtainFrame();

                WriteField(fieldName);

                PU.WriteInt(1 + (val == null ? 0 : 8 * val.Length), ctx.Stream);
                
                WriteLongArray(val);
            }

            /** <inheritdoc /> */
            public void WriteLongArray(long[] val)
            {
                PU.WriteLongArray(val, ctx.Stream);
            }

            /** <inheritdoc /> */
            public void WriteChar(string fieldName, char val)
            {
                WriteShort(fieldName, (short)val);
            }

            /** <inheritdoc /> */
            public void WriteChar(char val)
            {
                WriteShort((short)val);
            }

            /** <inheritdoc /> */
            public void WriteCharArray(string fieldName, char[] val)
            {
                ObtainFrame();

                WriteField(fieldName);

                PU.WriteInt(1 + (val == null ? 0 : 2 * val.Length), ctx.Stream);
                
                WriteCharArray(val);

                RestoreFrame();    
            }

            /** <inheritdoc /> */
            public void WriteCharArray(char[] val)
            {
                PU.WriteCharArray(val, ctx.Stream);
            }

            /** <inheritdoc /> */
            public void WriteFloat(string fieldName, float val)
            {
                ObtainFrame();

                WriteField(fieldName);

                PU.WriteInt(4, ctx.Stream);
                
                WriteFloat(val);

                RestoreFrame();
            }

            /** <inheritdoc /> */
            public void WriteFloat(float val)
            {
                PU.WriteFloat(val, ctx.Stream);
            }

            /** <inheritdoc /> */
            public void WriteFloatArray(string fieldName, float[] val)
            {
                ObtainFrame();

                WriteField(fieldName);

                PU.WriteInt(1 + (val == null ? 0 : 4 * val.Length), ctx.Stream);
                
                WriteFloatArray(val);

                RestoreFrame();
            }

            /** <inheritdoc /> */
            public void WriteFloatArray(float[] val)
            {
                PU.WriteFloatArray(val, ctx.Stream);
            }

            /** <inheritdoc /> */
            public void WriteDouble(string fieldName, double val)
            {
                ObtainFrame();

                WriteField(fieldName);

                PU.WriteInt(4, ctx.Stream);

                WriteDouble(val);

                RestoreFrame();
            }

            /** <inheritdoc /> */
            public void WriteDouble(double val)
            {
                PU.WriteDouble(val, ctx.Stream);
            }

            /** <inheritdoc /> */
            public void WriteDoubleArray(string fieldName, double[] val)
            {
                ObtainFrame();

                WriteField(fieldName);

                PU.WriteInt(1 + (val == null ? 0 : 4 * val.Length), ctx.Stream);
                
                WriteDoubleArray(val);

                RestoreFrame();
            }

            /** <inheritdoc /> */
            public void WriteDoubleArray(double[] val)
            {
                PU.WriteDoubleArray(val, ctx.Stream);
            }

            /** <inheritdoc /> */
            public void WriteString(string fieldName, string val)
            {
                ObtainFrame();

                WriteField(fieldName);

                long pos = ctx.Stream.Position;

                ctx.Stream.Seek(4, SeekOrigin.Current);

                WriteString(val);

                WriteLength(pos);

                RestoreFrame();
            }

            /** <inheritdoc /> */
            public void WriteString(string val)
            {
                PU.WriteString(val, ctx.Stream);
            }

            /** <inheritdoc /> */
            public void WriteStringArray(string fieldName, string[] val)
            {
                ObtainFrame();

                WriteField(fieldName);

                long pos = ctx.Stream.Position;

                ctx.Stream.Seek(4, SeekOrigin.Current);

                WriteStringArray(val);

                WriteLength(pos);

                RestoreFrame();
            }

            /** <inheritdoc /> */
            public void WriteStringArray(string[] val)
            {
                PU.WriteStringArray(val, ctx.Stream);
            }

            public void WriteGuid(string fieldName, Guid val)
            {
                throw new NotImplementedException();
            }

            public void WriteGuid(Guid val)
            {
                throw new NotImplementedException();
            }

            public void WriteGuidArray(string fieldName, Guid[] val)
            {
                throw new NotImplementedException();
            }

            public void WriteGuidArray(Guid[] val)
            {
                throw new NotImplementedException();
            }

            public void WriteEnum(string fieldName, Enum val)
            {
                throw new NotImplementedException();
            }

            public void WriteEnum(Enum val)
            {
                throw new NotImplementedException();
            }

            public void WriteEnumArray(string fieldName, Enum[] val)
            {
                throw new NotImplementedException();
            }

            public void WriteEnumArray(Enum[] val)
            {
                throw new NotImplementedException();
            }

            /** <inheritdoc /> */
            public void WriteObject<T>(string fieldName, T val)
            {
                ObtainFrame();

                WriteField(fieldName);

                long pos = ctx.Stream.Position;

                ctx.Stream.Seek(4, SeekOrigin.Current);

                WriteObject(val);

                WriteLength(pos);

                RestoreFrame();
            }

            /** <inheritdoc /> */
            public void WriteObject<T>(T val)
            {
                ctx.Write(val);
            }

            public void WriteObjectArray<T>(string fieldName, T[] val)
            {
                throw new NotImplementedException();
            }

            public void WriteObjectArray<T>(T[] val)
            {
                throw new NotImplementedException();
            }

            public void WriteCollection(string fieldName, ICollection val)
            {
                throw new NotImplementedException();
            }

            public void WriteCollection(ICollection val)
            {
                throw new NotImplementedException();
            }

            public void WriteCollection<T>(string fieldName, ICollection<T> val)
            {
                throw new NotImplementedException();
            }

            public void WriteCollection<T>(ICollection<T> val)
            {
                throw new NotImplementedException();
            }

            public void WriteMap(string fieldName, IDictionary val)
            {
                throw new NotImplementedException();
            }

            public void WriteMap(IDictionary val)
            {
                throw new NotImplementedException();
            }

            public void WriteMap<K, V>(string fieldName, IDictionary<K, V> val)
            {
                throw new NotImplementedException();
            }

            public void WriteMap<K, V>(IDictionary<K, V> val)
            {
                throw new NotImplementedException();
            }
            
            /**
             * <summary>Mark current output as raw.</summary>
             */ 
            public void MarkRaw()
            {
                if (!Frame.Raw)
                {
                    Frame.RawPosition = ctx.Stream.Position;

                    Frame.Raw = true;
                }
            }

            /**
             * <summary>Write field.</summary>
             * <param name="fieldName">Field name.</param>
             */ 
            private void WriteField(string fieldName)
            {
                if (ctx.CurrentFrame.Raw)
                    throw new GridClientPortableException("Cannot write named fields after raw data is written.");

                // TODO: GG-8535: Check string mode here.
                int? fieldIdRef = Frame.Mapper.FieldId(Frame.TypeId, fieldName);

                int fieldId = fieldIdRef.HasValue ? fieldIdRef.Value : PU.StringHashCode(fieldName.ToLower());

                PU.WriteInt(fieldId, ctx.Stream);
            }

            /**
             * <summary></summary>
             * <param name="pos">Position where length should reside.</param>
             */
            private void WriteLength(long pos)
            {
                Stream stream = ctx.Stream;

                long retPos = stream.Position;

                stream.Seek(pos, SeekOrigin.Begin);

                PU.WriteInt((int)(retPos - pos), stream);

                stream.Seek(retPos, SeekOrigin.Begin);
            }

            /**
             * <summary>Obtain current frame to context.</summary>
             * <returns>Frame.</returns>
             */
            private Frame ObtainFrame()
            {
                Frame = ctx.CurrentFrame;

                return Frame;
            }

            /**
             * <summary>Restore context frame to current.</summary>
             */ 
            private void RestoreFrame()
            {
                ctx.CurrentFrame = Frame;
            }
        }        
    }    
}
