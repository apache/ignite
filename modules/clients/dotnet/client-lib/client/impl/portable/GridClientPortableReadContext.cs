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

    /**
     * <summary>Portable read context.</summary>
     */ 
    internal class GridClientPortableReadContext
    {
        /** Type descriptors. */
        private readonly IDictionary<long, GridClientPortableTypeDescriptor> descs;

        /** Reader. */
        private readonly GridClientPortableReaderImpl reader;

        //private IDictionary<int, GridClientPortableObjectHandle> hnds;

        /**
         * <summary>Constructor.</summary>
         * <param name="descs">Descriptors.</param>
         * <param name="stream">Input stream.</param>
         */
        public GridClientPortableReadContext(IDictionary<long, GridClientPortableTypeDescriptor> descs,
            MemoryStream stream) 
        {
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
         * <param name="portObj">Portable object.</param>
         * <returns>Desertialized object.</returns>
         */ 
        public T Deserialize<T>(GridClientPortableObjectImpl portObj)
        {
            MemoryStream stream = portObj.Stream();

            byte hdr = (byte)stream.ReadByte();

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
                        // 1. Primitive type?
                        bool processed;

                        object res = PU.ReadPrimitive(typeId, (typeof(T)), stream, out processed);

                        if (processed)
                            return (T)res;

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
                throw new NotImplementedException();
            }
            else
                throw new GridClientPortableException("Invalid header: " + hdr);
        }
    }
}
