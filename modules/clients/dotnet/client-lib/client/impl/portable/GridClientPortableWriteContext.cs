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
    using GridGain.Client.Portable;

    using PU = GridGain.Client.Impl.Portable.GridClientPortableUilts;
    using PSH = GridGain.Client.Impl.Portable.GridClientPortableSystemHandlers;

    /**
     * <summary>Portable write context.</summary>
     */ 
    internal class GridClientPortableWriteContext
    {
        /** Type descriptors. */
        private readonly IDictionary<Type, GridClientPortableTypeDescriptor> descs;

        /** Writer. */
        private readonly GridClientPortableWriterImpl writer;

        /** Start position in the stream. */
        private readonly int startPos;

        /** Handles. */
        private IDictionary<GridClientPortableObjectHandle, int> hnds;
                                
        /**
            * <summary>Constructor.</summary>
            * <param name="descs">Type descriptors.</param>
            * <param name="stream">Output stream.</param>
            */
        public GridClientPortableWriteContext(IDictionary<Type, GridClientPortableTypeDescriptor> descs,
            Stream stream)
        {
            this.descs = descs;

            Stream = stream;

            startPos = (int)stream.Position;

            writer = new GridClientPortableWriterImpl(this);
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
        public Stream Stream
        {
            get;
            private set;
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
                Stream.WriteByte(PU.HDR_NULL);

                return;
            }
                
            // 2. Write primitive.
            int pos = (int)Stream.Position;

            Type type = obj.GetType();

            GridClientPortableSystemWriteDelegate sysHandler = PSH.WriteHandler(type);

            if (sysHandler != null)
            {
                sysHandler.Invoke(Stream, pos, obj);

                return;
            }
            
            // 3. Try interpreting object as handle.
            if (hnds == null)
                hnds = new Dictionary<GridClientPortableObjectHandle, int>();

            GridClientPortableObjectHandle hnd = new GridClientPortableObjectHandle(obj);

            int hndPos;

            if (hnds.TryGetValue(hnd, out hndPos))
            {
                Stream.WriteByte(PU.HDR_HND);

                PU.WriteInt(hndPos, Stream);

                return;
            }
            else
                // Handle position must be relative to the overall message start.
                hnds.Add(hnd, (int)pos - startPos);

            // 6. Write enum.

            // 9. Write collection.
            // TODO: GG-8535: Implement.

            // 10. Write map.
            // TODO: GG-8535: Implement.

            // 8. Write object array.
            // TODO: GG-8535: Implement.

            // 11. Just object.
            GridClientPortableTypeDescriptor desc;
                
            if (!descs.TryGetValue(type, out desc))
                throw new GridClientPortableException("Unsupported object type [type=" + type +
                    ", object=" + obj + ']');

            Stream.WriteByte(PU.HDR_FULL);
            PU.WriteBoolean(desc.UserType, Stream);
            PU.WriteInt(desc.TypeId, Stream);
            PU.WriteInt(obj.GetHashCode(), Stream);

            Stream.Seek(8, SeekOrigin.Current);

            GridClientPortableFrame oldFrame = CurrentFrame;

            CurrentFrame = new GridClientPortableFrame(desc.TypeId, desc.Mapper, null);

            desc.Serializer.WritePortable(obj, writer);

            WriteLength(Stream, pos, Stream.Position, CurrentFrame.RawPosition);

            CurrentFrame = oldFrame;
        }

        /**
         * <summary>Write lengths.</summary>
         * <param name="stream">Stream</param>
         * <param name="pos">Initial position.</param>
         * <param name="retPos">Return position</param>
         * <param name="rawPos">Raw data position.</param>
         */
        private static void WriteLength(Stream stream, long pos, long retPos, long rawPos)
        {
            stream.Seek(pos + 10, SeekOrigin.Begin);

            PU.WriteInt((int)(retPos - pos), stream);

            if (rawPos != 0)
                PU.WriteInt((int)(rawPos - pos), stream);

            stream.Seek(retPos, SeekOrigin.Begin);
        }
    }
}
