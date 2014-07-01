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
    using System.Diagnostics;
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

        /** Current type ID. */
        private int curTypeId;

        /** Current mapper. */
        private GridClientPortableIdResolver curMapper;

        /** Current portable object. */
        private GridClientPortableObjectImpl curPort;

        /** Current raw flag. */
        private bool curRaw;

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
         * <summary>Current type ID.</summary>
         */
        public int CurrentTypeId
        {
            get { return curTypeId; }
        }

        /**
         * <summary>Current ID mapper.</summary>
         */
        public GridClientPortableIdResolver CurrentMapper
        {
            get { return curMapper; }
        }

        /**
         * <summary>Current portable object.</summary>
         */
        public GridClientPortableObjectImpl CurrentPortable
        {
            get { return curPort; }
        }

        /**
         * <summary>Current raw flag.</summary>
         */
        public bool CurrentRaw
        {
            get { return curRaw; }
            set { curRaw = value; }
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
         * <param name="port">Portable object.</param>
         * <returns>Desertialized object.</returns>
         */ 
        public T Deserialize<T>(GridClientPortableObjectImpl port)
        {
            // 1. Lookup handle table first because the following scenario is possible:
            // 1.1. Inner object meets handle of outer object.
            // 1.2. Deserialization of outer object is triggered.
            // 1.3. Outer object is trying to deserialize the same inner object again.
            object hndObj;

            if (hnds.TryGetValue(port.Offset, out hndObj))
                return (T)hndObj;

            try
            {
                // 2. Position stream before portable object.
                Stream.Position = port.Offset;

                byte hdr = (byte)Stream.ReadByte();

                if (hdr == PU.HDR_NULL)
                    // 3. Handle null object.
                    return default(T);
                else if (hdr == PU.HDR_FULL)
                {
                    // 4. Read header.
                    bool userType = PU.ReadBoolean(Stream);
                    int typeId = PU.ReadInt(Stream);
                    int hashCode = PU.ReadInt(Stream);
                    int len = PU.ReadInt(Stream);
                    int rawPos = PU.ReadInt(Stream);

                    // 5. Preserve frame.
                    int oldTypeId = curTypeId;
                    GridClientPortableIdResolver oldMapper = curMapper;
                    GridClientPortableObjectImpl oldPort = curPort;
                    bool oldRaw = curRaw;

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

                        // 7. Try getting descriptor.
                        GridClientPortableTypeDescriptor desc;

                        if (!descs.TryGetValue(PU.TypeKey(userType, typeId), out desc))
                            throw new GridClientPortableException("Unknown type ID: " + typeId);

                        // 8. Set new frame.
                        curTypeId = typeId;
                        curMapper = desc.Mapper;
                        curPort = port;
                        curRaw = false;

                        // 9. Instantiate object. 
                        object obj;

                        try
                        {
                            obj = FormatterServices.GetUninitializedObject(desc.Type);

                            // 10. Save handle.
                            hnds[port.Offset] = obj;
                        }
                        catch (Exception e)
                        {
                            throw new GridClientPortableException("Failed to create type instance: " +
                                desc.Type.AssemblyQualifiedName, e);
                        }

                        // 11. Populate object fields.
                        desc.Serializer.ReadPortable(obj, reader);
                        
                        // 12. Special case for portable object.
                        if (obj is GridClientPortableObjectImpl)
                            ((GridClientPortableObjectImpl)obj).Populate(marsh);

                        return (T)obj;
                    }
                    finally
                    {
                        // 13. Restore old frame.
                        curTypeId = oldTypeId;
                        curMapper = oldMapper;
                        curPort = oldPort;
                        curRaw = oldRaw;
                    }
                }
                else if (hdr == PU.HDR_HND)
                {
                    // 14. Dealing with handles.
                    int curPos = (int)Stream.Position - 1;

                    int hndDelta = PU.ReadInt(Stream);

                    if (hnds.TryGetValue(curPos - hndDelta, out hndObj))
                        // 15. Already met this object, return immediately.
                        return (T)hndObj;
                    else
                    {
                        // 16. No such handler, i.e. we trying to deserialize inner object before deserializing outer.
                        Stream.Seek(curPos - hndDelta, SeekOrigin.Current);

                        return Deserialize<T>(Stream);
                    }
                }
                else if (PU.IsPredefinedType(hdr))
                {
                    GridClientPortableSystemFieldDelegate handler = PSH.FieldHandler(hdr);

                    Debug.Assert(handler != null, "Cannot find predefined read handler: " + hdr);

                    object val = handler.Invoke(Stream, marsh);

                    return (T)val;
                }
                else
                    throw new GridClientPortableException("Invalid header: " + hdr);
            }
            finally
            {
                // 17. Position stream right after the object.
                Stream.Position = port.Offset + port.Length;
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
            else if (hdr == PU.HDR_FULL || PU.IsPredefinedType(hdr))
            {
                // 3. Dealing with full object.
                GridClientPortableObjectImpl portObj = 
                    (GridClientPortableObjectImpl)marsh.Unmarshal0(stream, false, stream.Position - 1, hdr);

                T res = Deserialize<T>(portObj);

                // 4. Set correct position after deserialization.
                stream.Seek(portObj.Offset + portObj.Length, SeekOrigin.Begin);

                return res;
            }
            else if (hdr == PU.HDR_HND)
            {
                // 5. Dealing with handle.
                object hndObj;

                int curPos = (int)Stream.Position - 1;

                int hndDelta = PU.ReadInt(stream);

                long retPos = Stream.Position;

                try
                {
                    if (hnds.TryGetValue(curPos - hndDelta, out hndObj))
                        // 6. Already met this object, return immediately.
                        return (T)hndObj;
                    else
                    {
                        // 7. No such handle, i.e. we trying to deserialize inner object before deserializing outer.
                        Stream.Seek(curPos - hndDelta, SeekOrigin.Begin);

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
