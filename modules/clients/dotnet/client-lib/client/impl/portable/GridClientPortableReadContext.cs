// @csharp.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Client.Impl.Portable
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Runtime.Serialization;
    using GridGain.Client.Portable;

    using PU = GridGain.Client.Impl.Portable.GridClientPortableUilts;
    using PSH = GridGain.Client.Impl.Portable.GridClientPortableSystemHandlers;

    /**
     * <summary>Portable read context.</summary>
     */ 
    internal class GridClientPortableReadContext
    {
        /** Marshaller. */
        private readonly GridClientPortableMarshaller marsh;

        /** Type descriptors. */
        private readonly IDictionary<long, GridClientPortableTypeDescriptor> descs;

        /** Reader. */
        private readonly GridClientPortableReaderImpl reader;

        /** Handles. */
        private readonly IDictionary<int, object> hnds = new Dictionary<int, object>();

        /**
         * <summary>Constructor.</summary>
         * <param name="marsh">Marshaller.</param>
         * <param name="descs">Descriptors.</param>
         * <param name="stream">Input stream.</param>
         */
        public GridClientPortableReadContext(GridClientPortableMarshaller marsh, 
            IDictionary<long, GridClientPortableTypeDescriptor> descs, MemoryStream stream) 
        {
            this.marsh = marsh;
            this.descs = descs;

            Stream = stream;

            reader = new GridClientPortableReaderImpl(this);
        }

        /**
         * <summary>Current frame.</summary>
         */
        public GridClientPortableFrame CurrentFrame
        {
            get;
            private set;
        }

        /**
         * <summary>Stream.</summary>
         */
        public MemoryStream Stream
        {
            get;
            private set;
        }

        /**
         * <summary>Deserialize portable object.</summary>
         * <param name="stream">Stream.</param>
         * <returns>Desertialized object.</returns>
         */
        public T Deserialize<T>(MemoryStream stream)
        {
            return Deserialize<T>((GridClientPortableObjectImpl)marsh.Unmarshal(stream, false));
        }

        /**
         * <summary>Deserialize portable object.</summary>
         * <param name="portObj">Portable object.</param>
         * <returns>Desertialized object.</returns>
         */ 
        public T Deserialize<T>(GridClientPortableObjectImpl portObj)
        {
            // Lookup handle table first because the following scenario is possible:
            // 1. Inner object meets handle of outer object.
            // 2. Deserialization of outer object is triggered.
            // 3. Outer object is trying to deserialize the same inner object again.
            object hndObj;

            if (hnds.TryGetValue(portObj.Offset, out hndObj))
                return (T)hndObj;

            MemoryStream stream = portObj.Stream();

            byte hdr = (byte)stream.ReadByte();

            if (hdr == PU.HDR_NULL)
                return default(T);
            if (hdr == PU.HDR_FULL)
            {
                bool userType = PU.ReadBoolean(stream);
                int typeId = PU.ReadInt(stream);
                int hashCode = PU.ReadInt(stream);
                int len = PU.ReadInt(stream);
                int rawPos = PU.ReadInt(stream);

                GridClientPortableFrame oldFrame = CurrentFrame;

                try
                {
                    if (userType)
                    {
                        // 1. Lookup type and instantiate it.
                        GridClientPortableTypeDescriptor desc;

                        if (!descs.TryGetValue(PU.TypeKey(userType, typeId), out desc))
                            throw new GridClientPortableException("Unknown type ID: " + typeId);

                        CurrentFrame = new GridClientPortableFrame(typeId, desc.Mapper, portObj);

                        object obj;

                        try
                        {
                            obj = FormatterServices.GetUninitializedObject(desc.Type);
                            
                            hnds[portObj.Offset] = obj;
                        }
                        catch (Exception e)
                        {
                            throw new GridClientPortableException("Failed to create type instance: " +
                                desc.Type.AssemblyQualifiedName, e);
                        }
                        
                        desc.Serializer.ReadPortable(obj, reader);

                        return (T)obj;
                    }
                    else
                    {
                        object rrr;

                        GridClientPortableSystemReadDelegate handler = PSH.ReadHandler(typeId);

                        if (handler != null)
                        {
                            handler.Invoke(stream, typeof(T), out rrr);

                            return (T)rrr;
                        }

                        // 2. String?
                        if (typeId == PU.TYPE_STRING)
                            return (T)(object)PU.ReadString(stream);

                        // 3. Guid?
                        if (typeId == PU.TYPE_GUID) 
                            return (T)(object)PU.ReadGuid(stream);
                        
                        return (T)(new object());
                    }
                }
                finally
                {
                    CurrentFrame = oldFrame;
                }
            }
            else if (hdr == PU.HDR_HND)
            {
                int hndPos = PU.ReadInt(stream);
                
                if (hnds.TryGetValue(hndPos, out hndObj))
                    return (T)hndObj;
                else
                {
                    // No such handle, i.e. we trying to deserialize inner object before deserializing outer.
                    MemoryStream hndStream = new MemoryStream(stream.ToArray());

                    hndStream.Seek(hndPos, SeekOrigin.Begin);

                    return Deserialize<T>(hndStream);
                }
            }
            else
                throw new GridClientPortableException("Invalid header: " + hdr);
        }
    }
}
