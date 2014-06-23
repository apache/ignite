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
    using System.Collections.Generic;
    using System.IO;

    using PU = GridGain.Client.Impl.Portable.GridClientPortableUilts;

    /**
     * <summary>Write delegate.</summary> 
     */
    internal delegate void GridClientPortableSystemWriteDelegate(Stream stream, int pos, object obj);

    /**
     * <summary>Read delegate.</summary> 
     */
    internal delegate void GridClientPortableSystemReadDelegate(Stream stream, Type type, out object obj);

    /**
     * <summary>Collection of predefined handlers for various system types.</summary>
     */ 
    internal static class GridClientPortableSystemHandlers
    {
        /** Header length. */
        private const int HDR_LEN = 18;

        /** Write handlers. */
        private static readonly IDictionary<Type, GridClientPortableSystemWriteDelegate> WRITE_HANDLERS =
            new Dictionary<Type, GridClientPortableSystemWriteDelegate>();

        /** Write handlers. */
        private static readonly IDictionary<int, GridClientPortableSystemReadDelegate> READ_HANDLERS =
            new Dictionary<int, GridClientPortableSystemReadDelegate>();

        /**
         * <summary>Static initializer.</summary>
         */ 
        static unsafe GridClientPortableSystemHandlers()
        {
            // 1. Primitives.
            WRITE_HANDLERS[typeof(bool)] = (stream, pos, obj) => 
                {
                    WriteCommonHeader(stream, PU.TYPE_BOOL, obj.GetHashCode(), HDR_LEN + 1);
                    PU.WriteBoolean((bool)obj, stream);
                };

            READ_HANDLERS[PU.TYPE_BOOL] = delegate(Stream stream, Type type, out object obj) 
            {
                obj = PU.ReadBoolean(stream);
            };

            WRITE_HANDLERS[typeof(sbyte)] = (stream, pos, obj) =>
            {
                WriteCommonHeader(stream, PU.TYPE_BYTE, obj.GetHashCode(), HDR_LEN + 1);

                sbyte val = (sbyte)obj;

                PU.WriteByte(*(byte*)&val, stream);
            };

            WRITE_HANDLERS[typeof(byte)] = (stream, pos, obj) =>
            {
                WriteCommonHeader(stream, PU.TYPE_BYTE, obj.GetHashCode(), HDR_LEN + 1);

                PU.WriteByte((byte)obj, stream);
            };

            READ_HANDLERS[PU.TYPE_BYTE] = delegate(Stream stream, Type type, out object obj)
            {
                byte val = PU.ReadByte(stream);

                if (type == typeof(sbyte) || type == typeof(sbyte?))
                    obj = *(sbyte*)&val;
                else
                    obj = val;
            };

            WRITE_HANDLERS[typeof(short)] = (stream, pos, obj) =>
            {
                WriteCommonHeader(stream, PU.TYPE_SHORT, obj.GetHashCode(), HDR_LEN + 2);

                PU.WriteShort((short)obj, stream);
            };

            WRITE_HANDLERS[typeof(ushort)] = (stream, pos, obj) =>
            {
                WriteCommonHeader(stream, PU.TYPE_SHORT, obj.GetHashCode(), HDR_LEN + 2);

                ushort val = (ushort)obj;

                PU.WriteShort(*(short*)&val, stream);
            };

            READ_HANDLERS[PU.TYPE_SHORT] = delegate(Stream stream, Type type, out object obj)
            {
                short val = PU.ReadShort(stream);

                if (type == typeof(ushort) || type == typeof(ushort?))
                    obj = *(ushort*)&val;
                else
                    obj = val;
            };

            WRITE_HANDLERS[typeof(char)] = (stream, pos, obj) =>
            {
                WriteCommonHeader(stream, PU.TYPE_CHAR, obj.GetHashCode(), HDR_LEN + 2);

                char val = (char)obj;

                PU.WriteShort(*(short*)&val, stream);
            };

            READ_HANDLERS[PU.TYPE_CHAR] = delegate(Stream stream, Type type, out object obj)
            {
                short val = PU.ReadShort(stream);

                obj = *(char*)&val;
            };

            WRITE_HANDLERS[typeof(int)] = (stream, pos, obj) =>
            {
                WriteCommonHeader(stream, PU.TYPE_INT, obj.GetHashCode(), HDR_LEN + 4);

                PU.WriteInt((int)obj, stream);
            };

            WRITE_HANDLERS[typeof(uint)] = (stream, pos, obj) =>
            {
                WriteCommonHeader(stream, PU.TYPE_INT, obj.GetHashCode(), HDR_LEN + 4);

                uint val = (uint)obj;

                PU.WriteInt(*(int*)&val, stream);
            };

            READ_HANDLERS[PU.TYPE_INT] = delegate(Stream stream, Type type, out object obj)
            {
                int val = PU.ReadInt(stream);

                if (type == typeof(uint) || type == typeof(uint?))
                    obj = *(uint*)&val;
                else
                    obj = val;
            };

            WRITE_HANDLERS[typeof(long)] = (stream, pos, obj) =>
            {
                WriteCommonHeader(stream, PU.TYPE_LONG, obj.GetHashCode(), HDR_LEN + 8);

                PU.WriteLong((long)obj, stream);
            };

            WRITE_HANDLERS[typeof(ulong)] = (stream, pos, obj) =>
            {
                WriteCommonHeader(stream, PU.TYPE_LONG, obj.GetHashCode(), HDR_LEN + 8);

                ulong val = (ulong)obj;

                PU.WriteLong(*(long*)&val, stream);
            };

            READ_HANDLERS[PU.TYPE_LONG] = delegate(Stream stream, Type type, out object obj)
            {
                long val = PU.ReadLong(stream);

                if (type == typeof(ulong) || type == typeof(ulong?))
                    obj = *(ulong*)&val;
                else
                    obj = val;
            };

            WRITE_HANDLERS[typeof(float)] = (stream, pos, obj) =>
            {
                WriteCommonHeader(stream, PU.TYPE_FLOAT, obj.GetHashCode(), HDR_LEN + 4);

                PU.WriteFloat((float)obj, stream);
            };

            READ_HANDLERS[PU.TYPE_FLOAT] = delegate(Stream stream, Type type, out object obj)
            {
                obj = PU.ReadFloat(stream);
            };

            WRITE_HANDLERS[typeof(double)] = (stream, pos, obj) =>
            {
                WriteCommonHeader(stream, PU.TYPE_DOUBLE, obj.GetHashCode(), HDR_LEN + 8);

                PU.WriteDouble((double)obj, stream);
            };

            READ_HANDLERS[PU.TYPE_DOUBLE] = delegate(Stream stream, Type type, out object obj)
            {
                obj = PU.ReadDouble(stream);
            };

            // 2. String.
            WRITE_HANDLERS[typeof(string)] = (stream, pos, obj) =>
            {
                string obj0 = (string)obj;

                WriteCommonHeader(stream, PU.TYPE_STRING, PU.StringHashCode(obj0));

                PU.WriteString(obj0, stream);

                WriteLength(stream, pos, stream.Position, 0);
            };

            // 3. Guid.
            WRITE_HANDLERS[typeof(Guid)] = (stream, pos, obj) =>
            {
                Guid obj0 = (Guid)obj;

                WriteCommonHeader(stream, PU.TYPE_GUID, obj0.GetHashCode(), HDR_LEN + 1 + 16);

                PU.WriteGuid(obj0, stream);
            };

            // 4. Primitive arrays.

            // 5. String array.

            // 6. Guid array.

            // 7. Object array.
        }

        /**
         * <summary>Get write handler for type.</summary>
         * <param name="type">Type.</param>
         * <returns>Handler or null if cannot be hanled in special way.</returns>
         */
        public static GridClientPortableSystemWriteDelegate WriteHandler(Type type)
        {
            GridClientPortableSystemWriteDelegate handler;

            WRITE_HANDLERS.TryGetValue(type, out handler);

            return handler;
        }

        /**
         * <summary>Get read handler for type ID.</summary>
         * <param name="typeId">Type ID.</param>
         * <returns>Handler or null if cannot be hanled in special way.</returns>
         */
        public static GridClientPortableSystemReadDelegate ReadHandler(int typeId)
        {
            GridClientPortableSystemReadDelegate handler;

            READ_HANDLERS.TryGetValue(typeId, out handler);

            return handler;
        }

        /**
         * <summary>Write common header without length.</summary>
         * <param name="stream">Stream.</param>
         * <param name="typeId">Type ID.</param>
         * <param name="hashCode">Hash code.</param>
         */
        private static void WriteCommonHeader(Stream stream, int typeId, int hashCode)
        {
            stream.WriteByte(PU.HDR_FULL);
            PU.WriteBoolean(false, stream);
            PU.WriteInt(typeId, stream);
            PU.WriteInt(hashCode, stream);
            stream.Seek(8, SeekOrigin.Current);
        }

        /**
         * <summary>Write common header with length.</summary>
         * <param name="stream">Stream.</param>
         * <param name="typeId">Type ID.</param>
         * <param name="hashCode">Hash code.</param>
         * <param name="len">Length.</param>
         */
        private static void WriteCommonHeader(Stream stream, int typeId, int hashCode, int len)
        {
            stream.WriteByte(PU.HDR_FULL);
            PU.WriteBoolean(false, stream);
            PU.WriteInt(typeId, stream);
            PU.WriteInt(hashCode, stream);
            PU.WriteInt(len, stream);
            stream.Seek(4, SeekOrigin.Current);
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
