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

        /** Descriptors. */
        public IDictionary<string, GridClientPortableTypeDescriptor> descs = new Dictionary<string, GridClientPortableTypeDescriptor>();

        /**
         * <summary>Static initializer.</summary>
         */
        static GridClientPortableMarshaller()
        {
            BYTE_8 = new byte[8]; 
        }

        /**
         * <summary>Constructor.</summary>
         * <param name="typeCfgs">User type configurtaions.</param>
         */ 
        public GridClientPortableMarshaller(ICollection<GridClientPortableTypeConfiguration> typeCfgs) 
        {
            GridClientPortableReflectingSerializer refSerializer = new GridClientPortableReflectingSerializer();
            IGridClientPortableSerializer extSerializer = new GridClientPortableExternalSerializer();

            // 1. Define system types.
            addSystemType(typeof(GridClientAuthenticationRequest), refSerializer);
            addSystemType(typeof(GridClientTopologyRequest), refSerializer);
            addSystemType(typeof(GridClientTaskRequest), refSerializer);
            addSystemType(typeof(GridClientCacheRequest), refSerializer);
            addSystemType(typeof(GridClientLogRequest), refSerializer);
            addSystemType(typeof(GridClientResponse), refSerializer);
            addSystemType(typeof(GridClientNodeBean), refSerializer);
            addSystemType(typeof(GridClientNodeMetricsBean), refSerializer);
            
            // 2. Define user types.
            if (typeCfgs != null)
            {
                foreach (GridClientPortableTypeConfiguration typeCfg in typeCfgs)
                    addUserType(typeCfg, refSerializer, extSerializer);
            }
        }

        /**
         * <summary>Add system type.</summary>
         * <param name="type">Type.</param>
         * <param name="refSerializer">Reflection serializer.</param>
         */
        private void addSystemType(Type type, GridClientPortableReflectingSerializer refSerializer)
        {
            GridClientPortableReflectingIdMapper mapper = new GridClientPortableReflectingIdMapper(type);

            int? typeIdHolder = mapper.TypeId(type.Name);

            int typeId = typeIdHolder.HasValue ? typeIdHolder.Value : PU.StringHashCode(type.Name.ToLower());

            refSerializer.Register(type, typeId, mapper);

            addType(type, typeId, false, mapper, refSerializer);
        }

        /**
         * <summary>Add user type.</summary>
         * <param name="typeCfg">Type configuration.</param>
         * <param name="refSerializer">Reflection serializer.</param>
         * <param name="extSerializer">External serializer.</param>
         */
        private void addUserType(GridClientPortableTypeConfiguration typeCfg, GridClientPortableReflectingSerializer refSerializer, IGridClientPortableSerializer extSerializer)
        {            
            if (typeCfg.ClassName == null)
                throw new GridClientPortableException("Type name cannot be null: " + typeCfg);
            
            Type type;

            try
            {
                type = Type.GetType(typeCfg.ClassName);
            }
            catch (Exception e)
            {
                throw new GridClientPortableException("Type cannot be instantiated: " + typeCfg, e);
            }

            GridClientPortableIdMapper mapper = typeCfg.IdMapper != null ?
                typeCfg.IdMapper : new GridClientPortableReflectingIdMapper(type);

            int? typeIdRef = mapper.TypeId(type.Name);

            int typeId = typeIdRef.HasValue ? typeIdRef.Value : PU.StringHashCode(type.Name.ToLower());

            IGridClientPortableSerializer serializer;

            if (typeCfg.Serializer != null)
                serializer = typeCfg.Serializer;
            else
            {
                if (type.GetInterface(typeof(IGridClientPortable).Name) != null)
                    serializer = extSerializer;
                else
                {
                    refSerializer.Register(type, typeId, mapper);

                    serializer = refSerializer;
                }
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
        private void addType(Type type, int typeId, bool userType, GridClientPortableIdMapper mapper, IGridClientPortableSerializer serializer)
        {
            foreach (KeyValuePair<string, GridClientPortableTypeDescriptor> desc in descs)
            {
                if (desc.Value.TypeId == typeId)
                    throw new GridClientPortableException("Conflicting type IDs [type1=" + desc.Key +
                        ", type2=" + type.Name + ", typeId=" + typeId + ']');
            }

            descs[type.Name] = new GridClientPortableTypeDescriptor(typeId, false, mapper, serializer);
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
            new Context(ctx, descs, stream).Write(val);
        }

        /**
         * <summary>Unmarshal object.</summary>
         * <param name="data">Raw data.</param>
         * <returns>Unmarshalled object.</returns>
         */ 
        public T Unmarshal<T>(byte[] data)
        {
            return Unmarshal<T>(new MemoryStream(data));
        }

        /**
         * <summary>Unmarshal object.</summary>
         * <param name="data">Stream.</param>
         * <returns>Unmarshalled object.</returns>
         */ 
        public T Unmarshal<T>(Stream input)
        {
            // TODO: GG-8535: Implement.
            throw new NotImplementedException();
        }
        
        /**
         * <summary>Context.</summary>
         */ 
        private class Context 
        {
            /** Per-connection serialization context. */
            private readonly GridClientPortableSerializationContext ctx;

            /** Type descriptors. */
            private readonly IDictionary<string, GridClientPortableTypeDescriptor> descs;

            /** Output. */
            private readonly Stream stream;

            /** Tracking wrier. */
            private readonly Writer writer;
                        
            /** Handles. */
            private readonly IDictionary<GridClientPortableObjectHandle, int> hnds = new Dictionary<GridClientPortableObjectHandle, int>();

            /** Frames. */
            private readonly Stack<Frame> frames = new Stack<Frame>();
            
            /** Current object handle number. */
            private int curHndNum;
            
            /**
             * <summary>Constructor.</summary>
             * <param name="ctx">Client connection context.</param>
             * <param name="stream">Output stream.</param>
             */
            public Context(GridClientPortableSerializationContext ctx, IDictionary<string, GridClientPortableTypeDescriptor> descs, Stream stream)
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

                // Push frame to stack.
                CurrentFrame = new Frame(desc.TypeId, desc.Mapper);

                frames.Push(CurrentFrame);

                desc.Serializer.WritePortable(obj, writer);
                
                // Write length poping frame from stack.
                WriteLength(stream, pos, stream.Position, CurrentFrame.RawPosition);

                CurrentFrame = frames.Pop();
            }

            /**
             * <summary>Current frame.</summary>
             */ 
            public Frame CurrentFrame
            {
                get;
                private set;
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
         * <param name="raw">Raw data length.</param>
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
            public Frame(int typeId, GridClientPortableIdMapper mapper)
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
            public GridClientPortableIdMapper Mapper
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
        private class Writer : IGridClientPortableWriter 
        {
            /** Context. */
            private readonly Context ctx;

            /**
             * <summary>Constructor.</summary>
             * <param name="ctx">Context.</param>
             */ 
            public Writer(Context ctx)
            {
                this.ctx = ctx;
            }

            /** <inheritdoc /> */
            public void WriteBoolean(string fieldName, bool val)
            {
                WriteField(fieldName);

                PU.WriteInt(1, ctx.Stream);
                PU.WriteBoolean(val, ctx.Stream);
            }

            /** <inheritdoc /> */
            public void WriteBoolean(bool val)
            {
                MarkRaw();

                PU.WriteBoolean(val, ctx.Stream);
            }

            /** <inheritdoc /> */
            public void WriteBooleanArray(string fieldName, bool[] val)
            {
                WriteField(fieldName);

                PU.WriteInt(1 + (val == null ? 0 : val.Length), ctx.Stream);
                PU.WriteBooleanArray(val, ctx.Stream);
            }

            /** <inheritdoc /> */
            public void WriteBooleanArray(bool[] val)
            {
                MarkRaw();

                PU.WriteBooleanArray(val, ctx.Stream);
            }

            /** <inheritdoc /> */
            public void WriteByte(string fieldName, byte val)
            {
                WriteField(fieldName);

                PU.WriteInt(1, ctx.Stream);

                ctx.Stream.WriteByte(val);
            }

            /** <inheritdoc /> */
            public void WriteByte(byte val)
            {
                MarkRaw();

                ctx.Stream.WriteByte(val);
            }

            /** <inheritdoc /> */
            public void WriteByteArray(string fieldName, byte[] val)
            {
                WriteField(fieldName);

                PU.WriteInt(1 + (val == null ? 0 : val.Length), ctx.Stream);
                PU.WriteByteArray(val, ctx.Stream);
            }

            /** <inheritdoc /> */
            public void WriteByteArray(byte[] val)
            {
                MarkRaw();

                PU.WriteByteArray(val, ctx.Stream);
            }

            /** <inheritdoc /> */
            public void WriteShort(string fieldName, short val)
            {
                WriteField(fieldName);

                PU.WriteInt(2, ctx.Stream);
                PU.WriteShort(val, ctx.Stream);
            }

            /** <inheritdoc /> */
            public void WriteShort(short val)
            {
                MarkRaw();

                PU.WriteShort(val, ctx.Stream);
            }

            /** <inheritdoc /> */
            public void WriteShortArray(string fieldName, short[] val)
            {
                WriteField(fieldName);

                PU.WriteInt(1 + (val == null ? 0 : 2 * val.Length), ctx.Stream);
                PU.WriteShortArray(val, ctx.Stream);
            }

            /** <inheritdoc /> */
            public void WriteShortArray(short[] val)
            {
                MarkRaw();

                PU.WriteShortArray(val, ctx.Stream);
            }

            /** <inheritdoc /> */
            public void WriteInt(string fieldName, int val)
            {
                WriteField(fieldName);

                PU.WriteInt(4, ctx.Stream);
                PU.WriteInt(val, ctx.Stream);
            }

            /** <inheritdoc /> */
            public void WriteInt(int val)
            {
                MarkRaw();

                PU.WriteInt(val, ctx.Stream);
            }

            /** <inheritdoc /> */
            public void WriteIntArray(string fieldName, int[] val)
            {
                WriteField(fieldName);

                PU.WriteInt(1 + (val == null ? 0 : 4 * val.Length), ctx.Stream);
                PU.WriteIntArray(val, ctx.Stream);
            }

            /** <inheritdoc /> */
            public void WriteIntArray(int[] val)
            {
                MarkRaw();

                PU.WriteIntArray(val, ctx.Stream);
            }

            /** <inheritdoc /> */
            public void WriteLong(string fieldName, long val)
            {
                WriteField(fieldName);

                PU.WriteInt(8, ctx.Stream);
                PU.WriteLong(val, ctx.Stream);
            }

            /** <inheritdoc /> */
            public void WriteLong(long val)
            {
                MarkRaw();

                PU.WriteLong(val, ctx.Stream);
            }

            /** <inheritdoc /> */
            public void WriteLongArray(string fieldName, long[] val)
            {
                WriteField(fieldName);

                PU.WriteInt(1 + (val == null ? 0 : 8 * val.Length), ctx.Stream);
                PU.WriteLongArray(val, ctx.Stream);
            }

            /** <inheritdoc /> */
            public void WriteLongArray(long[] val)
            {
                MarkRaw();

                PU.WriteLongArray(val, ctx.Stream);
            }

            /** <inheritdoc /> */
            public void WriteChar(string fieldName, char val)
            {
                unchecked 
                {
                    WriteShort(fieldName, (short)val);
                }
            }

            /** <inheritdoc /> */
            public void WriteChar(char val)
            {
                unchecked
                {
                    WriteShort((short)val);
                }
            }

            /** <inheritdoc /> */
            public void WriteCharArray(string fieldName, char[] val)
            {
                WriteField(fieldName);

                PU.WriteInt(1 + (val == null ? 0 : 2 * val.Length), ctx.Stream);
                PU.WriteCharArray(val, ctx.Stream);
            }

            /** <inheritdoc /> */
            public void WriteCharArray(char[] val)
            {
                MarkRaw();

                PU.WriteCharArray(val, ctx.Stream);
            }

            /** <inheritdoc /> */
            public void WriteFloat(string fieldName, float val)
            {
                WriteField(fieldName);

                PU.WriteInt(4, ctx.Stream);
                PU.WriteFloat(val, ctx.Stream);
            }

            /** <inheritdoc /> */
            public void WriteFloat(float val)
            {
                MarkRaw();

                PU.WriteFloat(val, ctx.Stream);
            }

            /** <inheritdoc /> */
            public void WriteFloatArray(string fieldName, float[] val)
            {
                WriteField(fieldName);

                PU.WriteInt(1 + (val == null ? 0 : 4 * val.Length), ctx.Stream);
                PU.WriteFloatArray(val, ctx.Stream);
            }

            /** <inheritdoc /> */
            public void WriteFloatArray(float[] val)
            {
                MarkRaw();

                PU.WriteFloatArray(val, ctx.Stream);
            }

            /** <inheritdoc /> */
            public void WriteDouble(string fieldName, double val)
            {
                WriteField(fieldName);

                PU.WriteInt(4, ctx.Stream);
                PU.WriteDouble(val, ctx.Stream);
            }

            /** <inheritdoc /> */
            public void WriteDouble(double val)
            {
                MarkRaw();

                PU.WriteDouble(val, ctx.Stream);
            }

            /** <inheritdoc /> */
            public void WriteDoubleArray(string fieldName, double[] val)
            {
                WriteField(fieldName);

                PU.WriteInt(1 + (val == null ? 0 : 4 * val.Length), ctx.Stream);
                PU.WriteDoubleArray(val, ctx.Stream);
            }

            /** <inheritdoc /> */
            public void WriteDoubleArray(double[] val)
            {
                MarkRaw();

                PU.WriteDoubleArray(val, ctx.Stream);
            }

            /** <inheritdoc /> */
            public void WriteString(string fieldName, string val)
            {
                WriteField(fieldName);

                long pos = ctx.Stream.Position;

                ctx.Stream.Seek(4, SeekOrigin.Current);

                PU.WriteString(val, ctx.Stream);

                WriteLength(pos);
            }

            /** <inheritdoc /> */
            public void WriteString(string val)
            {
                MarkRaw();

                PU.WriteString(val, ctx.Stream);
            }

            /** <inheritdoc /> */
            public void WriteStringArray(string fieldName, string[] val)
            {
                WriteField(fieldName);

                long pos = ctx.Stream.Position;

                ctx.Stream.Seek(4, SeekOrigin.Current);

                PU.WriteStringArray(val, ctx.Stream);

                WriteLength(pos);
            }

            /** <inheritdoc /> */
            public void WriteStringArray(string[] val)
            {
                MarkRaw();

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
                WriteField(fieldName);

                long pos = ctx.Stream.Position;

                ctx.Stream.Seek(4, SeekOrigin.Current);

                ctx.Write(val);

                WriteLength(pos);
            }

            /** <inheritdoc /> */
            public void WriteObject<T>(T val)
            {
                MarkRaw();

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
                Frame frame = ctx.CurrentFrame;

                if (!frame.Raw)
                {
                    frame.RawPosition = ctx.Stream.Position;

                    frame.Raw = true;
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
                Frame frame = ctx.CurrentFrame;

                int? fieldIdRef = frame.Mapper.FieldId(frame.TypeId, fieldName);

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
        }        
    }    
}
