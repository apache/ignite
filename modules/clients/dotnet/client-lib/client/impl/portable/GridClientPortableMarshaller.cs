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

        /** Descriptors. */
        public IDictionary<string, GridClientPortableTypeDescriptor> descriptors = new Dictionary<string, GridClientPortableTypeDescriptor>();

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
            foreach (KeyValuePair<string, GridClientPortableTypeDescriptor> descriptor in descriptors)
            {
                if (descriptor.Value.TypeId == typeId)
                    throw new GridClientPortableException("Conflicting type IDs [type1=" + descriptors.Keys +
                        ", type2=" + type.Name + ", typeId=" + typeId + ']');
            }

            descriptors[type.Name] = new GridClientPortableTypeDescriptor(typeId, false, mapper, serializer);
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
            new Context(ctx, stream).Write(val);
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

            /** Output. */
            private readonly Stream stream;

            /** Tracking wrier. */
            private readonly Writer writer;
                        
            /** Handles. */
            private readonly IDictionary<GridClientPortableObjectHandle, int> hnds = new Dictionary<GridClientPortableObjectHandle, int>();
            
            /** Current object handle number. */
            private int curHndNum;
            
            /**
             * <summary>Constructor.</summary>
             * <param name="ctx">Client connection context.</param>
             * <param name="stream">Output stream.</param>
             */
            public Context(GridClientPortableSerializationContext ctx, Stream stream)
            {
                this.ctx = ctx;
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

                int? hndNum = hnds[hnd];

                if (hndNum.HasValue) 
                {
                    stream.WriteByte(HDR_HND);

                    PU.WriteInt(hndNum.Value, stream);

                    return;
                }
                else
                    hnds.Add(hnd, curHndNum++);

                // 4. Write string.
                dynamic obj0 = obj;

                if (type == typeof(string)) 
                {
                    stream.WriteByte(HDR_FULL);

                    PU.WriteBoolean(false, stream);                    
                    PU.WriteInt(PU.TYPE_STRING, stream);
                    PU.WriteInt(PU.StringHashCode(obj0), stream);
                    PU.WriteString(obj0, stream);

                    return;
                }

                // 5. Write GUID.
                if (type == typeof(Guid))
                {
                    stream.WriteByte(HDR_FULL);
                    PU.WriteBoolean(false, stream);    
                    PU.WriteInt(PU.TYPE_GUID, stream);
                    PU.WriteInt(PU.GuidHashCode(obj0), stream);
                    PU.WriteGuid(obj0, stream);

                    return;
                }

                // 6. Write primitive array.
                typeId = PU.PrimitiveArrayTypeId(type);

                if (typeId != 0) 
                {
                    stream.WriteByte(HDR_FULL);
                    PU.WriteBoolean(false, stream);    
                    PU.WriteInt(typeId, stream);

                    // TODO: GG-8535: Implement.
                }

                // 8. Write object array.
                // TODO: GG-8535: Implement.

                // 9. Write collection.
                // TODO: GG-8535: Implement.

                // 10. Write map.
                // TODO: GG-8535: Implement.

                // 11. Just object.
                long pos = stream.Position; 
                
   

                
            }
        }

        /**
         * <summary>Writer.</summary>
         */ 
        private class Writer //: IGridClientPortableWriter 
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
        }
        
    }

    /** <summary>Writer which simply caches passed values.</summary> */
    public class DelayedWriter //: IGridClientPortableWriter 
    {
        ///** Named actions. */
        //private readonly List<Action<IGridClientPortableWriter>> namedActions = new List<Action<IGridClientPortableWriter>>();

        ///** Actions. */
        //private readonly List<Action<IGridClientPortableWriter>> actions = new List<Action<IGridClientPortableWriter>>();

        ///**
        // * <summary>Execute all tracked write actions.</summary>
        // * <param name="writer">Underlying real writer.</param>
        // */
        //private void execute(IGridClientPortableWriter writer)
        //{
        //    foreach (Action<IGridClientPortableWriter> namedAction in namedActions)
        //    {
        //        namedAction.Invoke(writer);
        //    }

        //    foreach (Action<IGridClientPortableWriter> action in actions)
        //    {
        //        action.Invoke(writer);
        //    }
        //}

        ///** <inheritdoc /> */
        //public void WriteByte(string fieldName, sbyte val)
        //{
        //    namedActions.Add((writer) => writer.WriteByte(fieldName, val));
        //}

        ///** <inheritdoc /> */
        //public void WriteByte(sbyte val)
        //{
        //    actions.Add((writer) => writer.WriteByte(val));
        //}

        ///** <inheritdoc /> */
        //public void WriteByteArray(string fieldName, sbyte[] val)
        //{
        //    namedActions.Add((writer) => writer.WriteByteArray(fieldName, val));
        //}

        ///** <inheritdoc /> */
        //public void WriteByteArray(sbyte[] val)
        //{
        //    actions.Add((writer) => writer.WriteByteArray(val));
        //}

        ///** <inheritdoc /> */
        //public void WriteChar(string fieldName, char val)
        //{
        //    namedActions.Add((writer) => writer.WriteChar(fieldName, val));
        //}

        ///** <inheritdoc /> */
        //public void WriteChar(char val)
        //{
        //    actions.Add((writer) => writer.WriteChar(val));
        //}

        ///** <inheritdoc /> */
        //public void WriteCharArray(string fieldName, char[] val)
        //{
        //    namedActions.Add((writer) => writer.WriteCharArray(fieldName, val));
        //}

        ///** <inheritdoc /> */
        //public void WriteCharArray(char[] val)
        //{
        //    actions.Add((writer) => writer.WriteCharArray(val));
        //}

        ///** <inheritdoc /> */
        //public void WriteShort(string fieldName, short val)
        //{
        //    namedActions.Add((writer) => writer.WriteShort(fieldName, val));
        //}

        ///** <inheritdoc /> */
        //public void WriteShort(short val)
        //{
        //    actions.Add((writer) => writer.WriteShort(val));
        //}

        ///** <inheritdoc /> */
        //public void WriteShortArray(string fieldName, short[] val)
        //{
        //    namedActions.Add((writer) => writer.WriteShortArray(fieldName, val));
        //}

        ///** <inheritdoc /> */
        //public void WriteShortArray(short[] val)
        //{
        //    actions.Add((writer) => writer.WriteShortArray(val));
        //}

        ///** <inheritdoc /> */
        //public void WriteInt(string fieldName, int val)
        //{
        //    namedActions.Add((writer) => writer.WriteInt(fieldName, val));
        //}

        ///** <inheritdoc /> */
        //public void WriteInt(int val)
        //{
        //    actions.Add((writer) => writer.WriteInt(val));
        //}

        ///** <inheritdoc /> */
        //public void WriteIntArray(string fieldName, int[] val)
        //{
        //    namedActions.Add((writer) => writer.WriteIntArray(fieldName, val));
        //}

        ///** <inheritdoc /> */
        //public void WriteIntArray(int[] val)
        //{
        //    actions.Add((writer) => writer.WriteIntArray(val));
        //}

        ///** <inheritdoc /> */
        //public void WriteLong(string fieldName, long val)
        //{
        //    namedActions.Add((writer) => writer.WriteLong(fieldName, val));
        //}

        ///** <inheritdoc /> */
        //public void WriteLong(long val)
        //{
        //    actions.Add((writer) => writer.WriteLong(val));
        //}

        ///** <inheritdoc /> */
        //public void WriteLongArray(string fieldName, long[] val)
        //{
        //    namedActions.Add((writer) => writer.WriteLongArray(fieldName, val));
        //}

        ///** <inheritdoc /> */
        //public void WriteLongArray(long[] val)
        //{
        //    actions.Add((writer) => writer.WriteLongArray(val));
        //}

        ///** <inheritdoc /> */
        //public void WriteBoolean(string fieldName, bool val)
        //{
        //    namedActions.Add((writer) => writer.WriteBoolean(fieldName, val));
        //}

        ///** <inheritdoc /> */
        //public void WriteBoolean(bool val)
        //{
        //    actions.Add((writer) => writer.WriteBoolean(val));
        //}

        ///** <inheritdoc /> */
        //public void WriteBooleanArray(string fieldName, bool[] val)
        //{
        //    namedActions.Add((writer) => writer.WriteBooleanArray(fieldName, val));
        //}

        ///** <inheritdoc /> */
        //public void WriteBooleanArray(bool[] val)
        //{
        //    actions.Add((writer) => writer.WriteBooleanArray(val));
        //}

        ///** <inheritdoc /> */
        //public void WriteFloat(string fieldName, float val)
        //{
        //    namedActions.Add((writer) => writer.WriteFloat(fieldName, val));
        //}

        ///** <inheritdoc /> */
        //public void WriteFloat(float val)
        //{
        //    actions.Add((writer) => writer.WriteFloat(val));
        //}

        ///** <inheritdoc /> */
        //public void WriteFloatArray(string fieldName, float[] val)
        //{
        //    namedActions.Add((writer) => writer.WriteFloatArray(fieldName, val));
        //}

        ///** <inheritdoc /> */
        //public void WriteFloatArray(float[] val)
        //{
        //    actions.Add((writer) => writer.WriteFloatArray(val));
        //}

        ///** <inheritdoc /> */
        //public void WriteDouble(string fieldName, double val)
        //{
        //    namedActions.Add((writer) => writer.WriteDouble(fieldName, val));
        //}

        ///** <inheritdoc /> */
        //public void WriteDouble(double val)
        //{
        //    actions.Add((writer) => writer.WriteDouble(val));
        //}

        ///** <inheritdoc /> */
        //public void WriteDoubleArray(string fieldName, double[] val)
        //{
        //    namedActions.Add((writer) => writer.WriteDoubleArray(fieldName, val));
        //}

        ///** <inheritdoc /> */
        //public void WriteDoubleArray(double[] val)
        //{
        //    actions.Add((writer) => writer.WriteDoubleArray(val));
        //}

        ///** <inheritdoc /> */
        //public void WriteString(string fieldName, string val)
        //{
        //    namedActions.Add((writer) => writer.WriteString(fieldName, val));
        //}

        ///** <inheritdoc /> */
        //public void WriteString(string val)
        //{
        //    actions.Add((writer) => writer.WriteString(val));
        //}

        ///** <inheritdoc /> */
        //public void WriteStringArray(string fieldName, string[] val)
        //{
        //    namedActions.Add((writer) => writer.WriteStringArray(fieldName, val));
        //}

        ///** <inheritdoc /> */
        //public void WriteStringArray(string[] val)
        //{
        //    actions.Add((writer) => writer.WriteStringArray(val));
        //}

        ///** <inheritdoc /> */
        //public void WriteGuid(string fieldName, Guid val)
        //{
        //    namedActions.Add((writer) => writer.WriteGuid(fieldName, val));
        //}

        ///** <inheritdoc /> */
        //public void WriteGuid(Guid val)
        //{
        //    actions.Add((writer) => writer.WriteGuid(val));
        //}

        ///** <inheritdoc /> */
        //public void WriteGuidArray(string fieldName, Guid[] val)
        //{
        //    namedActions.Add((writer) => writer.WriteGuidArray(fieldName, val));
        //}

        ///** <inheritdoc /> */
        //public void WriteGuidArray(Guid[] val)
        //{
        //    actions.Add((writer) => writer.WriteGuidArray(val));
        //}

        ///** <inheritdoc /> */
        //public void WriteObject<T>(string fieldName, T val)
        //{
        //    namedActions.Add((writer) => writer.WriteObject(fieldName, val));
        //}

        ///** <inheritdoc /> */
        //public void WriteObject<T>(T val)
        //{
        //    actions.Add((writer) => writer.WriteObject(val));
        //}

        ///** <inheritdoc /> */
        //public void WriteObjectArray<T>(string fieldName, T[] val)
        //{
        //    namedActions.Add((writer) => writer.WriteObjectArray(fieldName, val));
        //}

        ///** <inheritdoc /> */
        //public void WriteObjectArray<T>(T[] val)
        //{
        //    actions.Add((writer) => writer.WriteObjectArray(val));
        //}

        ///** <inheritdoc /> */
        //public void WriteCollection(string fieldName, ICollection val)
        //{
        //    namedActions.Add((writer) => writer.WriteCollection(fieldName, val));
        //}

        ///** <inheritdoc /> */
        //public void WriteCollection(ICollection val)
        //{
        //    actions.Add((writer) => writer.WriteCollection(val));
        //}

        ///** <inheritdoc /> */
        //public void WriteCollection<T>(string fieldName, ICollection<T> val)
        //{
        //    namedActions.Add((writer) => writer.WriteCollection(fieldName, val));
        //}

        ///** <inheritdoc /> */
        //public void WriteCollection<T>(ICollection<T> val)
        //{
        //    actions.Add((writer) => writer.WriteCollection(val));
        //}

        ///** <inheritdoc /> */
        //public void WriteMap(string fieldName, IDictionary val)
        //{
        //    namedActions.Add((writer) => writer.WriteMap(fieldName, val));
        //}

        ///** <inheritdoc /> */
        //public void WriteMap(IDictionary val)
        //{
        //    actions.Add((writer) => writer.WriteMap(val));
        //}

        ///** <inheritdoc /> */
        //public void WriteMap<K, V>(string fieldName, IDictionary<K, V> val)
        //{
        //    namedActions.Add((writer) => writer.WriteMap(fieldName, val));
        //}

        ///** <inheritdoc /> */
        //public void WriteMap<K, V>(IDictionary<K, V> val)
        //{
        //    actions.Add((writer) => writer.WriteMap(val));
        //}
    }
}
