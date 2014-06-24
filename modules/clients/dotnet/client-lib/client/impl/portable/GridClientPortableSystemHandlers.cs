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
    using System.Collections.Concurrent;
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
            WRITE_HANDLERS[typeof(bool[])] = (stream, pos, obj) =>
            {
                bool[] arr = (bool[])obj;

                WriteCommonHeader(stream, PU.TYPE_ARRAY_BOOL, obj.GetHashCode(), HDR_LEN + 1 + (arr != null ? 4 + arr.Length : 0));

                PU.WriteBooleanArray(arr, stream);
            };

            READ_HANDLERS[PU.TYPE_ARRAY_BOOL] = delegate(Stream stream, Type type, out object obj)
            {
                obj = PU.ReadBooleanArray(stream);
            };

            WRITE_HANDLERS[typeof(byte[])] = (stream, pos, obj) =>
            {
                byte[] arr = (byte[])obj;

                WriteCommonHeader(stream, PU.TYPE_ARRAY_BYTE, obj.GetHashCode(), HDR_LEN + 1 + (arr != null ? 4 + arr.Length : 0));

                PU.WriteByteArray(arr, stream);
            };

            WRITE_HANDLERS[typeof(sbyte[])] = (stream, pos, obj) =>
            {
                byte[] arr = (byte[])(Array)obj;

                WriteCommonHeader(stream, PU.TYPE_ARRAY_BYTE, obj.GetHashCode(), HDR_LEN + 1 + (arr != null ? 4 + arr.Length : 0));

                PU.WriteByteArray(arr, stream);
            };

            READ_HANDLERS[PU.TYPE_ARRAY_BYTE] = delegate(Stream stream, Type type, out object obj)
            {
                obj = PU.ReadByteArray(stream, type == typeof(sbyte[]));
            };

            WRITE_HANDLERS[typeof(short[])] = (stream, pos, obj) =>
            {
                short[] arr = (short[])obj;

                WriteCommonHeader(stream, PU.TYPE_ARRAY_SHORT, obj.GetHashCode(), HDR_LEN + 1 + (arr != null ? 4 + arr.Length * 2 : 0));

                PU.WriteShortArray(arr, stream);
            };

            WRITE_HANDLERS[typeof(ushort[])] = (stream, pos, obj) =>
            {
                short[] arr = (short[])(Array)obj;

                WriteCommonHeader(stream, PU.TYPE_ARRAY_SHORT, obj.GetHashCode(), HDR_LEN + 1 + (arr != null ? 4 + arr.Length * 2 : 0));

                PU.WriteShortArray(arr, stream);
            };

            READ_HANDLERS[PU.TYPE_ARRAY_SHORT] = delegate(Stream stream, Type type, out object obj)
            {
                obj = PU.ReadShortArray(stream, type == typeof(short[]));
            };

            WRITE_HANDLERS[typeof(char[])] = (stream, pos, obj) =>
            {
                char[] arr = (char[])obj;

                WriteCommonHeader(stream, PU.TYPE_ARRAY_CHAR, obj.GetHashCode(), HDR_LEN + 1 + (arr != null ? 4 + arr.Length * 2 : 0));

                PU.WriteCharArray(arr, stream);
            };

            READ_HANDLERS[PU.TYPE_ARRAY_CHAR] = delegate(Stream stream, Type type, out object obj)
            {
                obj = PU.ReadCharArray(stream);
            };

            WRITE_HANDLERS[typeof(int[])] = (stream, pos, obj) =>
            {
                int[] arr = (int[])obj;

                WriteCommonHeader(stream, PU.TYPE_ARRAY_INT, obj.GetHashCode(), HDR_LEN + 1 + (arr != null ? 4 + arr.Length * 4 : 0));

                PU.WriteIntArray(arr, stream);
            };

            WRITE_HANDLERS[typeof(uint[])] = (stream, pos, obj) =>
            {
                int[] arr = (int[])(Array)obj;

                WriteCommonHeader(stream, PU.TYPE_ARRAY_INT, obj.GetHashCode(), HDR_LEN + 1 + (arr != null ? 4 + arr.Length * 4 : 0));

                PU.WriteIntArray(arr, stream);
            };

            READ_HANDLERS[PU.TYPE_ARRAY_INT] = delegate(Stream stream, Type type, out object obj)
            {
                obj = PU.ReadIntArray(stream, type == typeof(int[]));
            };

            WRITE_HANDLERS[typeof(long[])] = (stream, pos, obj) =>
            {
                long[] arr = (long[])obj;

                WriteCommonHeader(stream, PU.TYPE_ARRAY_LONG, obj.GetHashCode(), HDR_LEN + 1 + (arr != null ? 4 + arr.Length * 8 : 0));

                PU.WriteLongArray(arr, stream);
            };

            WRITE_HANDLERS[typeof(ulong[])] = (stream, pos, obj) =>
            {
                long[] arr = (long[])(Array)obj;

                WriteCommonHeader(stream, PU.TYPE_ARRAY_LONG, obj.GetHashCode(), HDR_LEN + 1 + (arr != null ? 4 + arr.Length * 8 : 0));

                PU.WriteLongArray(arr, stream);
            };

            READ_HANDLERS[PU.TYPE_ARRAY_LONG] = delegate(Stream stream, Type type, out object obj)
            {
                obj = PU.ReadLongArray(stream, type == typeof(long[]));
            };

            WRITE_HANDLERS[typeof(float[])] = (stream, pos, obj) =>
            {
                float[] arr = (float[])obj;

                WriteCommonHeader(stream, PU.TYPE_ARRAY_FLOAT, obj.GetHashCode(), HDR_LEN + 1 + (arr != null ? 4 + arr.Length * 4 : 0));

                PU.WriteFloatArray(arr, stream);
            };

            READ_HANDLERS[PU.TYPE_ARRAY_FLOAT] = delegate(Stream stream, Type type, out object obj)
            {
                obj = PU.ReadFloatArray(stream);
            };

            WRITE_HANDLERS[typeof(double[])] = (stream, pos, obj) =>
            {
                double[] arr = (double[])obj;

                WriteCommonHeader(stream, PU.TYPE_ARRAY_DOUBLE, obj.GetHashCode(), HDR_LEN + 1 + (arr != null ? 4 + arr.Length * 8 : 0));

                PU.WriteDoubleArray(arr, stream);
            };

            READ_HANDLERS[PU.TYPE_ARRAY_DOUBLE] = delegate(Stream stream, Type type, out object obj)
            {
                obj = PU.ReadDoubleArray(stream);
            };

            // 5. String array.
            WRITE_HANDLERS[typeof(string[])] = (stream, pos, obj) =>
            {
                string[] arr = (string[])obj;

                WriteCommonHeader(stream, PU.TYPE_ARRAY_STRING, obj.GetHashCode());

                PU.WriteStringArray(arr, stream);

                WriteLength(stream, pos, stream.Position, 0);
            };

            READ_HANDLERS[PU.TYPE_ARRAY_STRING] = delegate(Stream stream, Type type, out object obj)
            {
                obj = PU.ReadStringArray(stream);
            };

            // 6. Guid array.
            WRITE_HANDLERS[typeof(Guid?[])] = (stream, pos, obj) =>
            {
                Guid?[] arr = (Guid?[])obj;

                WriteCommonHeader(stream, PU.TYPE_ARRAY_GUID, obj.GetHashCode());

                PU.WriteGuidArray(arr, stream);

                WriteLength(stream, pos, stream.Position, 0);
            };

            READ_HANDLERS[PU.TYPE_ARRAY_GUID] = delegate(Stream stream, Type type, out object obj)
            {
                obj = PU.ReadGuidArray(stream);
            };

            // 7. Object array.
        }

        public static void WriteCollection(GridClientPortableWriteContext ctx, Stream stream, int pos, ICollection col, byte type)
        {
            WriteCommonHeader(stream, PU.TYPE_COLLECTION, col.GetHashCode());

            PU.WriteInt(col.Count, stream);

            stream.WriteByte(type); 

            foreach (object elem in col)
                ctx.Write(elem);

            WriteLength(stream, pos, stream.Position, 0);
        }

        public static void WriteGenericCollection<T>(GridClientPortableWriteContext ctx, Stream stream, int pos, ICollection<T> col, byte type)
        {
            WriteCommonHeader(stream, PU.TYPE_COLLECTION, col.GetHashCode());

            stream.WriteByte(PU.BYTE_ONE);

            PU.WriteInt(col.Count, stream);

            stream.WriteByte(type);

            foreach (object elem in col)
                ctx.Write(elem);

            WriteLength(stream, pos, stream.Position, 0);
        }

        public static ICollection ReadCollection(GridClientPortableReadContext ctx, MemoryStream stream)
        {
            if (stream.ReadByte() == PU.BYTE_ONE)
            {
                int len = PU.ReadInt(stream);

                byte type = (byte)stream.ReadByte();

                ArrayList res = new ArrayList();

                for (int i = 0; i < len; i++)
                    res.Add(ctx.Deserialize<object>(stream));

                return res;
            }
            else
                return null;
        }

        public static ICollection<T> ReadCollection<T>(GridClientPortableReadContext ctx, MemoryStream stream)
        {
            if (stream.ReadByte() == PU.BYTE_ONE)
            {
                int len = PU.ReadInt(stream);

                byte type = (byte)stream.ReadByte();

                ICollection<T> res;

                if (type == PU.COLLECTION_ARRAY_LIST)
                    res = new List<T>(len);
                else if (type == PU.COLLECTION_LINKED_LIST)
                    res = new LinkedList<T>();
                else if (type == PU.COLLECTION_HASH_SET)
                    res = new HashSet<T>();
                else if (type == PU.COLLECTION_SORTED_SET)
                    res = new SortedSet<T>();
                else
                    res = new List<T>(len);

                for (int i = 0; i < len; i++)
                    res.Add(ctx.Deserialize<T>(stream));

                return res;
            }
            else
                return null;
        }

        /**
         * <summary>Get write handler for type.</summary>
         * <param name="type">Type.</param>
         * <returns>Handler or null if cannot be hanled in special way.</returns>
         */
        public static GridClientPortableSystemWriteDelegate WriteHandler(Type type)
        {
            GridClientPortableSystemWriteDelegate handler;

            if (WRITE_HANDLERS.TryGetValue(type, out handler))
                return handler;
            else
            {
                if (type.IsGenericType)
                {
                    // 1. Generic dictionary?
                    

                    // 2. Generic collection?
                    if (type.GetGenericTypeDefinition() == typeof(ICollection<>))
                    {

                    }
                }

                

                // 3. Generic dictionary?

                // 4. Dictionary?

                return null;
            }
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

        /**
         * <summary>Create new array list.</summary>
         * <param name="len">Length.</param>
         * <returns>Array list.</returns>
         */ 
        public static ICollection CreateArrayList(int len)
        {
            return new ArrayList(len);
        }

        /**
         * <summary>Add element to array list.</summary>
         * <param name="col">Array list.</param>
         * <param name="elem">Element</param>
         */
        public static void AddToArrayList(ICollection col, object elem)
        {
            (col as ArrayList).Add(elem);
        }

        public static ICollection<T> CreateList<T>(int len)
        {
            return new List<T>(len);
        }

        public static ICollection<T> CreateLinkedList<T>(int len)
        {
            return new LinkedList<T>();
        }

        public static ICollection<T> CreateHashSet<T>(int len)
        {
            return new HashSet<T>();
        }

        public static ICollection<T> CreateSortedSet<T>(int len)
        {
            return new SortedSet<T>(); 
        }

        public static IDictionary CreateHashtable(int len)
        {
            return new Hashtable(len);
        }

        public static IDictionary<K, V> CreateDictionary<K, V>(int len)
        {
            return new Dictionary<K, V>(len);
        }

        public static IDictionary<K, V> CreateSortedDictionary<K, V>(int len)
        {
            return new SortedDictionary<K, V>();
        }

        public static IDictionary<K, V> CreateConcurrentDictionary<K, V>(int len)
        {
            return new ConcurrentDictionary<K, V>(Environment.ProcessorCount, len);
        }
    }
}
