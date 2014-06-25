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
        public GridClientPortableReadFrame CurrentFrame
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
            long pos = stream.Position;

            byte hdr = (byte)stream.ReadByte();

            if (hdr == PU.HDR_NULL)
                return default(T);
            else if (hdr == PU.HDR_HND)
            {
                object hndObj;

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
            {
                GridClientPortableObjectImpl portObj = (GridClientPortableObjectImpl)marsh.Unmarshal0(stream, false, pos, hdr);

                return Deserialize<T>(portObj);
            }            
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
                // 1. Read header.
                bool userType = PU.ReadBoolean(stream);
                int typeId = PU.ReadInt(stream);
                int hashCode = PU.ReadInt(stream);
                int len = PU.ReadInt(stream);
                int rawPos = PU.ReadInt(stream);

                // 2. Preserve frame.
                GridClientPortableReadFrame oldFrame = CurrentFrame;

                try
                {
                    if (!userType)
                    {
                        // 3. Try reading predefined type.
                        object sysObj;

                        GridClientPortableSystemReadDelegate handler = PSH.ReadHandler(typeId);

                        if (handler != null)
                        {
                            handler.Invoke(this, stream, typeof(T), out sysObj);

                            return (T)sysObj;
                        }
                    }

                    // 4. Try getting gdescriptor.
                    GridClientPortableTypeDescriptor desc;

                    if (!descs.TryGetValue(PU.TypeKey(userType, typeId), out desc))
                        throw new GridClientPortableException("Unknown type ID: " + typeId);

                    // 5. Set new frame.
                    CurrentFrame = new GridClientPortableReadFrame(typeId, desc.Mapper, portObj);

                    // 6. Instantiate object. 
                    object obj;

                    try
                    {
                        obj = FormatterServices.GetUninitializedObject(desc.Type);

                        // 7. Save handle.
                        hnds[portObj.Offset] = obj;
                    }
                    catch (Exception e)
                    {
                        throw new GridClientPortableException("Failed to create type instance: " +
                            desc.Type.AssemblyQualifiedName, e);
                    }

                    // 8. Populate object fields.
                    desc.Serializer.ReadPortable(obj, reader);
                    
                    return (T)obj;
                }
                finally
                {
                    // 9. Restore old frame.
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
