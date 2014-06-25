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
         * <param name="portObj">Portable object.</param>
         * <returns>Desertialized object.</returns>
         */ 
        public T Deserialize<T>(GridClientPortableObjectImpl portObj)
        {
            // 1. Lookup handle table first because the following scenario is possible:
            // 1.1. Inner object meets handle of outer object.
            // 1.2. Deserialization of outer object is triggered.
            // 1.3. Outer object is trying to deserialize the same inner object again.
            object hndObj;

            if (hnds.TryGetValue(portObj.Offset, out hndObj))
                return (T)hndObj;

            try
            {
                // 2. Position stream before portable object.
                Stream.Position = portObj.Offset;

                byte hdr = (byte)Stream.ReadByte();

                if (hdr == PU.HDR_NULL)
                    // 3. Handle null object.
                    return default(T);
                if (hdr == PU.HDR_FULL)
                {
                    // 4. Read header.
                    bool userType = PU.ReadBoolean(Stream);
                    int typeId = PU.ReadInt(Stream);
                    int hashCode = PU.ReadInt(Stream);
                    int len = PU.ReadInt(Stream);
                    int rawPos = PU.ReadInt(Stream);

                    // 5. Preserve frame.
                    GridClientPortableReadFrame oldFrame = CurrentFrame;

                    try
                    {
                        if (!userType)
                        {
                            // 6. Try reading predefined type.
                            object sysObj;

                            GridClientPortableSystemReadDelegate handler = PSH.ReadHandler(typeId);

                            if (handler != null)
                            {
                                handler.Invoke(this, typeof(T), out sysObj);

                                return (T)sysObj;
                            }
                        }

                        // 7. Try getting gdescriptor.
                        GridClientPortableTypeDescriptor desc;

                        if (!descs.TryGetValue(PU.TypeKey(userType, typeId), out desc))
                            throw new GridClientPortableException("Unknown type ID: " + typeId);

                        // 8. Set new frame.
                        CurrentFrame = new GridClientPortableReadFrame(typeId, desc.Mapper, portObj);

                        // 9. Instantiate object. 
                        object obj;

                        try
                        {
                            obj = FormatterServices.GetUninitializedObject(desc.Type);

                            // 10. Save handle.
                            hnds[portObj.Offset] = obj;
                        }
                        catch (Exception e)
                        {
                            throw new GridClientPortableException("Failed to create type instance: " +
                                desc.Type.AssemblyQualifiedName, e);
                        }

                        // 11. Populate object fields.
                        desc.Serializer.ReadPortable(obj, reader);

                        return (T)obj;
                    }
                    finally
                    {
                        // 12. Restore old frame.
                        CurrentFrame = oldFrame;
                    }
                }
                else if (hdr == PU.HDR_HND)
                {
                    // 13. Dealing with handles.
                    int hndPos = PU.ReadInt(Stream);

                    if (hnds.TryGetValue(hndPos, out hndObj))
                        // 14. Already met this object, return immediately.
                        return (T)hndObj;
                    else
                    {
                        // 15. No such handler, i.e. we trying to deserialize inner object before deserializing outer.
                        Stream.Seek(hndPos, SeekOrigin.Begin);

                        return Deserialize<T>(Stream);
                    }
                }
                else
                    throw new GridClientPortableException("Invalid header: " + hdr);
            }
            finally
            {
                // 16. Position stream right after the object.
                Stream.Position = portObj.Offset + portObj.Length;
            }
        }

        /**
         * <summary>Deserialize portable object.</summary>
         * <param name="stream">Stream.</param>
         * <returns>Desertialized object.</returns>
         */
        public T Deserialize<T>(MemoryStream stream)
        {
            // 1. Read header.
            byte hdr = (byte)stream.ReadByte();

            if (hdr == PU.HDR_NULL)
                // 2. Dealing with null.
                return default(T);
            else if (hdr == PU.HDR_FULL)
            {
                // 3. Dealing with full object.
                GridClientPortableObjectImpl portObj = (GridClientPortableObjectImpl)marsh.Unmarshal0(stream, false, stream.Position - 1, hdr);

                T res = Deserialize<T>(portObj);

                // 4. Set correct position after deserialization.
                stream.Seek(portObj.Offset + portObj.Length, SeekOrigin.Begin);

                return res;
            }
            else if (hdr == PU.HDR_HND)
            {
                // 5. Dealing with handle.
                object hndObj;

                int hndPos = PU.ReadInt(stream);

                long retPos = Stream.Position;

                try
                {
                    if (hnds.TryGetValue(hndPos, out hndObj))
                        // 6. Already met this object, return immediately.
                        return (T)hndObj;
                    else
                    {
                        // 7. No such handle, i.e. we trying to deserialize inner object before deserializing outer.
                        Stream.Seek(hndPos, SeekOrigin.Begin);

                        return Deserialize<T>(Stream);
                    }
                }
                finally
                {
                    // 8. Set correct stream position - right after handle.
                    stream.Seek(retPos, SeekOrigin.Begin);
                }
            }
            else
                throw new GridClientPortableException("Invalid header: " + hdr);            
        }
    }
}
