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
     * <param name="ctx">Write context.</param>
     * <param name="stream">Stream to wrte to.</param>
     * <param name="pos">Position where write started.</param>
     * <param name="obj">Object to write.</param>
     */
    internal delegate void GridClientPortableSystemWriteDelegate(GridClientPortableWriteContext ctx, Stream stream, int pos, object obj);

    /**
     * <summary>Read delegate.</summary> 
     * <param name="ctx">Read context.</param>
     * <param name="stream">Stream to wrte to.</param>
     * <param name="type">Type.</param>
     * <param name="obj">Object to read to.</param>
     */
    internal delegate void GridClientPortableSystemReadDelegate(GridClientPortableReadContext ctx, Stream stream, Type type, out object obj);

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
            WRITE_HANDLERS[typeof(bool)] = WriteBool;
            READ_HANDLERS[PU.TYPE_BOOL] = ReadBool;

            WRITE_HANDLERS[typeof(sbyte)] = WriteSbyte;
            WRITE_HANDLERS[typeof(byte)] = WriteByte;
            READ_HANDLERS[PU.TYPE_BYTE] = ReadByte;

            WRITE_HANDLERS[typeof(short)] = WriteShort;
            WRITE_HANDLERS[typeof(ushort)] = WriteUshort;
            READ_HANDLERS[PU.TYPE_SHORT] = ReadShort;

            WRITE_HANDLERS[typeof(char)] = WriteChar;
            READ_HANDLERS[PU.TYPE_CHAR] = ReadChar;

            WRITE_HANDLERS[typeof(int)] = WriteInt;
            WRITE_HANDLERS[typeof(uint)] = WriteUint;
            READ_HANDLERS[PU.TYPE_INT] = ReadInt;

            WRITE_HANDLERS[typeof(long)] = WriteLong;
            WRITE_HANDLERS[typeof(ulong)] = WriteUlong;
            READ_HANDLERS[PU.TYPE_LONG] = ReadLong;

            WRITE_HANDLERS[typeof(float)] = WriteFloat;
            READ_HANDLERS[PU.TYPE_FLOAT] = ReadFloat;

            WRITE_HANDLERS[typeof(double)] = WriteDouble;
            READ_HANDLERS[PU.TYPE_DOUBLE] = ReadDouble;

            // 2. Date.
            WRITE_HANDLERS[typeof(DateTime)] = WriteDate;
            READ_HANDLERS[PU.TYPE_DATE] = ReadDate;

            // 3. String.
            WRITE_HANDLERS[typeof(string)] = WriteString;
            READ_HANDLERS[PU.TYPE_STRING] = ReadString;

            // 4. Guid.
            WRITE_HANDLERS[typeof(Guid)] = WriteGuid;
            READ_HANDLERS[PU.TYPE_GUID] = ReadGuid;

            // 5. Primitive arrays.
            WRITE_HANDLERS[typeof(bool[])] = WriteBoolArray;
            READ_HANDLERS[PU.TYPE_ARRAY_BOOL] = ReadBoolArray;

            WRITE_HANDLERS[typeof(byte[])] = WriteByteArray;
            WRITE_HANDLERS[typeof(sbyte[])] = WriteSbyteArray;
            READ_HANDLERS[PU.TYPE_ARRAY_BYTE] = ReadByteArray;

            WRITE_HANDLERS[typeof(short[])] = WriteShortArray;
            WRITE_HANDLERS[typeof(ushort[])] = WriteUshortArray;
            READ_HANDLERS[PU.TYPE_ARRAY_SHORT] = ReadShortArray;

            WRITE_HANDLERS[typeof(char[])] = WriteCharArray;
            READ_HANDLERS[PU.TYPE_ARRAY_CHAR] = ReadCharArray;

            WRITE_HANDLERS[typeof(int[])] = WriteIntArray;
            WRITE_HANDLERS[typeof(uint[])] = WriteUintArray;
            READ_HANDLERS[PU.TYPE_ARRAY_INT] = ReadIntArray;

            WRITE_HANDLERS[typeof(long[])] = WriteLongArray;
            WRITE_HANDLERS[typeof(ulong[])] = WriteUlongArray;
            READ_HANDLERS[PU.TYPE_ARRAY_LONG] = ReadLongArray;

            WRITE_HANDLERS[typeof(float[])] = WriteFloatArray;
            READ_HANDLERS[PU.TYPE_ARRAY_FLOAT] = ReadFloatArray;

            WRITE_HANDLERS[typeof(double[])] = WriteDoubleArray;
            READ_HANDLERS[PU.TYPE_ARRAY_DOUBLE] = ReadDoubleArray;

            // 6. Date array.
            WRITE_HANDLERS[typeof(DateTime?[])] = WriteDateArray;
            READ_HANDLERS[PU.TYPE_ARRAY_DATE] = ReadDateArray;

            // 7. String array.
            WRITE_HANDLERS[typeof(string[])] = WriteStringArray;
            READ_HANDLERS[PU.TYPE_ARRAY_STRING] = ReadStringArray;

            // 8. Guid array.
            WRITE_HANDLERS[typeof(Guid?[])] = WriteGuidArray;
            READ_HANDLERS[PU.TYPE_ARRAY_GUID] = ReadGuidArray;            

            // 9. Object array.

            // 10. Predefined collections.
            
            // 11. Custom collections.
        }

        /**
         * <summary>Write boolean.</summary>
         */
        private static void WriteBool(GridClientPortableWriteContext ctx, Stream stream, int pos, object obj)
        {
            WriteCommonHeader(stream, PU.TYPE_BOOL, obj.GetHashCode(), HDR_LEN + 1);
            PU.WriteBoolean((bool)obj, stream);
        }

        /**
         * <summary>Write sbyte.</summary>
         */
        private static unsafe void WriteSbyte(GridClientPortableWriteContext ctx, Stream stream, int pos, object obj)
        {
            WriteCommonHeader(stream, PU.TYPE_BYTE, obj.GetHashCode(), HDR_LEN + 1);

            sbyte val = (sbyte)obj;

            PU.WriteByte(*(byte*)&val, stream);
        }

        /**
         * <summary>Write byte.</summary>
         */
        private static unsafe void WriteByte(GridClientPortableWriteContext ctx, Stream stream, int pos, object obj)
        {
            WriteCommonHeader(stream, PU.TYPE_BYTE, obj.GetHashCode(), HDR_LEN + 1);

            PU.WriteByte((byte)obj, stream);
        }

        /**
         * <summary>Write short.</summary>
         */
        private static unsafe void WriteShort(GridClientPortableWriteContext ctx, Stream stream, int pos, object obj)
        {
            WriteCommonHeader(stream, PU.TYPE_SHORT, obj.GetHashCode(), HDR_LEN + 2);

            PU.WriteShort((short)obj, stream);
        }

        /**
         * <summary>Write ushort.</summary>
         */
        private static unsafe void WriteUshort(GridClientPortableWriteContext ctx, Stream stream, int pos, object obj)
        {
            WriteCommonHeader(stream, PU.TYPE_SHORT, obj.GetHashCode(), HDR_LEN + 2);

            ushort val = (ushort)obj;

            PU.WriteShort(*(short*)&val, stream);
        }

        /**
         * <summary>Write char.</summary>
         */
        private static unsafe void WriteChar(GridClientPortableWriteContext ctx, Stream stream, int pos, object obj)
        {
            WriteCommonHeader(stream, PU.TYPE_CHAR, obj.GetHashCode(), HDR_LEN + 2);

            char val = (char)obj;

            PU.WriteShort(*(short*)&val, stream);
        }

        /**
         * <summary>Write int.</summary>
         */
        private static unsafe void WriteInt(GridClientPortableWriteContext ctx, Stream stream, int pos, object obj)
        {
            WriteCommonHeader(stream, PU.TYPE_INT, obj.GetHashCode(), HDR_LEN + 4);

            PU.WriteInt((int)obj, stream);
        }

        /**
         * <summary>Write uint.</summary>
         */
        private static unsafe void WriteUint(GridClientPortableWriteContext ctx, Stream stream, int pos, object obj)
        {
            WriteCommonHeader(stream, PU.TYPE_INT, obj.GetHashCode(), HDR_LEN + 4);

            uint val = (uint)obj;

            PU.WriteInt(*(int*)&val, stream);
        }

        /**
         * <summary>Write long.</summary>
         */
        private static unsafe void WriteLong(GridClientPortableWriteContext ctx, Stream stream, int pos, object obj)
        {
            WriteCommonHeader(stream, PU.TYPE_LONG, obj.GetHashCode(), HDR_LEN + 8);

            PU.WriteLong((long)obj, stream);
        }

        /**
         * <summary>Write ulong.</summary>
         */
        private static unsafe void WriteUlong(GridClientPortableWriteContext ctx, Stream stream, int pos, object obj)
        {
            WriteCommonHeader(stream, PU.TYPE_LONG, obj.GetHashCode(), HDR_LEN + 8);

            ulong val = (ulong)obj;

            PU.WriteLong(*(long*)&val, stream);
        }

        /**
         * <summary>Write float.</summary>
         */
        private static unsafe void WriteFloat(GridClientPortableWriteContext ctx, Stream stream, int pos, object obj)
        {
            WriteCommonHeader(stream, PU.TYPE_FLOAT, obj.GetHashCode(), HDR_LEN + 4);

            PU.WriteFloat((float)obj, stream);
        }

        /**
         * <summary>Write double.</summary>
         */
        private static unsafe void WriteDouble(GridClientPortableWriteContext ctx, Stream stream, int pos, object obj)
        {
            WriteCommonHeader(stream, PU.TYPE_DOUBLE, obj.GetHashCode(), HDR_LEN + 8);

            PU.WriteDouble((double)obj, stream);
        }

        /**
         * <summary>Write date.</summary>
         */
        private static unsafe void WriteDate(GridClientPortableWriteContext ctx, Stream stream, int pos, object obj)
        {
            DateTime obj0 = (DateTime)obj;

            WriteCommonHeader(stream, PU.TYPE_DATE, obj0.GetHashCode(), HDR_LEN + 1 + 8);

            PU.WriteDate(obj0, stream);
        }

        /**
         * <summary>Write string.</summary>
         */
        private static unsafe void WriteString(GridClientPortableWriteContext ctx, Stream stream, int pos, object obj)
        {
            string obj0 = (string)obj;

            WriteCommonHeader(stream, PU.TYPE_STRING, PU.StringHashCode(obj0));

            PU.WriteString(obj0, stream);

            WriteLength(stream, pos, stream.Position, 0);
        }

        /**
         * <summary>Write Guid.</summary>
         */
        private static unsafe void WriteGuid(GridClientPortableWriteContext ctx, Stream stream, int pos, object obj)
        {
            Guid obj0 = (Guid)obj;

            WriteCommonHeader(stream, PU.TYPE_GUID, obj0.GetHashCode(), HDR_LEN + 1 + 16);

            PU.WriteGuid(obj0, stream);
        }

        /**
         * <summary>Write bool array.</summary>
         */
        private static unsafe void WriteBoolArray(GridClientPortableWriteContext ctx, Stream stream, int pos, object obj)
        {
            bool[] arr = (bool[])obj;

            WriteCommonHeader(stream, PU.TYPE_ARRAY_BOOL, obj.GetHashCode(), HDR_LEN + 1 + (arr != null ? 4 + arr.Length : 0));

            PU.WriteBooleanArray(arr, stream);
        }

        /**
         * <summary>Write byte array.</summary>
         */
        private static unsafe void WriteByteArray(GridClientPortableWriteContext ctx, Stream stream, int pos, object obj)
        {
            byte[] arr = (byte[])obj;

            WriteCommonHeader(stream, PU.TYPE_ARRAY_BYTE, obj.GetHashCode(), HDR_LEN + 1 + (arr != null ? 4 + arr.Length : 0));

            PU.WriteByteArray(arr, stream);
        }

        /**
         * <summary>Write sbyte array.</summary>
         */
        private static unsafe void WriteSbyteArray(GridClientPortableWriteContext ctx, Stream stream, int pos, object obj)
        {
            byte[] arr = (byte[])(Array)obj;

            WriteCommonHeader(stream, PU.TYPE_ARRAY_BYTE, obj.GetHashCode(), HDR_LEN + 1 + (arr != null ? 4 + arr.Length : 0));

            PU.WriteByteArray(arr, stream);
        }

        /**
         * <summary>Write short array.</summary>
         */
        private static unsafe void WriteShortArray(GridClientPortableWriteContext ctx, Stream stream, int pos, object obj)
        {
            short[] arr = (short[])obj;

            WriteCommonHeader(stream, PU.TYPE_ARRAY_SHORT, obj.GetHashCode(), HDR_LEN + 1 + (arr != null ? 4 + arr.Length * 2 : 0));

            PU.WriteShortArray(arr, stream);
        }

        /**
         * <summary>Write ushort array.</summary>
         */
        private static unsafe void WriteUshortArray(GridClientPortableWriteContext ctx, Stream stream, int pos, object obj)
        {
            short[] arr = (short[])(Array)obj;

            WriteCommonHeader(stream, PU.TYPE_ARRAY_SHORT, obj.GetHashCode(), HDR_LEN + 1 + (arr != null ? 4 + arr.Length * 2 : 0));

            PU.WriteShortArray(arr, stream);
        }

        /**
         * <summary>Write char array.</summary>
         */
        private static unsafe void WriteCharArray(GridClientPortableWriteContext ctx, Stream stream, int pos, object obj)
        {
            char[] arr = (char[])obj;

            WriteCommonHeader(stream, PU.TYPE_ARRAY_CHAR, obj.GetHashCode(), HDR_LEN + 1 + (arr != null ? 4 + arr.Length * 2 : 0));

            PU.WriteCharArray(arr, stream);
        }

        /**
         * <summary>Write int array.</summary>
         */
        private static unsafe void WriteIntArray(GridClientPortableWriteContext ctx, Stream stream, int pos, object obj)
        {
            int[] arr = (int[])obj;

            WriteCommonHeader(stream, PU.TYPE_ARRAY_INT, obj.GetHashCode(), HDR_LEN + 1 + (arr != null ? 4 + arr.Length * 4 : 0));

            PU.WriteIntArray(arr, stream);
        }

        /**
         * <summary>Write uint array.</summary>
         */
        private static unsafe void WriteUintArray(GridClientPortableWriteContext ctx, Stream stream, int pos, object obj)
        {
            int[] arr = (int[])(Array)obj;

            WriteCommonHeader(stream, PU.TYPE_ARRAY_INT, obj.GetHashCode(), HDR_LEN + 1 + (arr != null ? 4 + arr.Length * 4 : 0));

            PU.WriteIntArray(arr, stream);
        }

        /**
         * <summary>Write long array.</summary>
         */
        private static unsafe void WriteLongArray(GridClientPortableWriteContext ctx, Stream stream, int pos, object obj)
        {
            long[] arr = (long[])obj;

            WriteCommonHeader(stream, PU.TYPE_ARRAY_LONG, obj.GetHashCode(), HDR_LEN + 1 + (arr != null ? 4 + arr.Length * 8 : 0));

            PU.WriteLongArray(arr, stream);
        }

        /**
         * <summary>Write ulong array.</summary>
         */
        private static unsafe void WriteUlongArray(GridClientPortableWriteContext ctx, Stream stream, int pos, object obj)
        {
            long[] arr = (long[])(Array)obj;

            WriteCommonHeader(stream, PU.TYPE_ARRAY_LONG, obj.GetHashCode(), HDR_LEN + 1 + (arr != null ? 4 + arr.Length * 8 : 0));

            PU.WriteLongArray(arr, stream);
        }

        /**
         * <summary>Write float array.</summary>
         */
        private static unsafe void WriteFloatArray(GridClientPortableWriteContext ctx, Stream stream, int pos, object obj)
        {
            float[] arr = (float[])obj;

            WriteCommonHeader(stream, PU.TYPE_ARRAY_FLOAT, obj.GetHashCode(), HDR_LEN + 1 + (arr != null ? 4 + arr.Length * 4 : 0));

            PU.WriteFloatArray(arr, stream);
        }

        /**
         * <summary>Write double array.</summary>
         */
        private static unsafe void WriteDoubleArray(GridClientPortableWriteContext ctx, Stream stream, int pos, object obj)
        {
            double[] arr = (double[])obj;

            WriteCommonHeader(stream, PU.TYPE_ARRAY_DOUBLE, obj.GetHashCode(), HDR_LEN + 1 + (arr != null ? 4 + arr.Length * 8 : 0));

            PU.WriteDoubleArray(arr, stream);
        }

        /**
         * <summary>Write date array.</summary>
         */
        private static unsafe void WriteDateArray(GridClientPortableWriteContext ctx, Stream stream, int pos, object obj)
        {
            DateTime?[] arr = (DateTime?[])obj;

            WriteCommonHeader(stream, PU.TYPE_ARRAY_DATE, obj.GetHashCode());

            PU.WriteDateArray(arr, stream);

            WriteLength(stream, pos, stream.Position, 0);
        }

        /**
         * <summary>Write string array.</summary>
         */
        private static unsafe void WriteStringArray(GridClientPortableWriteContext ctx, Stream stream, int pos, object obj)
        {
            string[] arr = (string[])obj;

            WriteCommonHeader(stream, PU.TYPE_ARRAY_STRING, obj.GetHashCode());

            PU.WriteStringArray(arr, stream);

            WriteLength(stream, pos, stream.Position, 0);
        }

        /**
         * <summary>Write Guid array.</summary>
         */
        private static unsafe void WriteGuidArray(GridClientPortableWriteContext ctx, Stream stream, int pos, object obj)
        {
            Guid?[] arr = (Guid?[])obj;

            WriteCommonHeader(stream, PU.TYPE_ARRAY_GUID, obj.GetHashCode());

            PU.WriteGuidArray(arr, stream);

            WriteLength(stream, pos, stream.Position, 0);
        }

        /**
         * <summary>Read bool.</summary>
         */
        private static unsafe void ReadBool(GridClientPortableReadContext ctx, Stream stream, Type type, out object obj)
        {
            obj = PU.ReadBoolean(stream);
        }

        /**
         * <summary>Read byte.</summary>
         */
        private static unsafe void ReadByte(GridClientPortableReadContext ctx, Stream stream, Type type, out object obj)
        {
            byte val = PU.ReadByte(stream);

            if (type == typeof(sbyte) || type == typeof(sbyte?))
                obj = *(sbyte*)&val;
            else
                obj = val;
        }

        /**
         * <summary>Read short.</summary>
         */
        private static unsafe void ReadShort(GridClientPortableReadContext ctx, Stream stream, Type type, out object obj)
        {
            short val = PU.ReadShort(stream);

            if (type == typeof(ushort) || type == typeof(ushort?))
                obj = *(ushort*)&val;
            else
                obj = val;
        }

        /**
         * <summary>Read char.</summary>
         */
        private static unsafe void ReadChar(GridClientPortableReadContext ctx, Stream stream, Type type, out object obj)
        {
            short val = PU.ReadShort(stream);

            obj = *(char*)&val;
        }

        /**
         * <summary>Read int.</summary>
         */
        private static unsafe void ReadInt(GridClientPortableReadContext ctx, Stream stream, Type type, out object obj)
        {
            int val = PU.ReadInt(stream);

            if (type == typeof(uint) || type == typeof(uint?))
                obj = *(uint*)&val;
            else
                obj = val;
        }

        /**
         * <summary>Read long.</summary>
         */
        private static unsafe void ReadLong(GridClientPortableReadContext ctx, Stream stream, Type type, out object obj)
        {
            long val = PU.ReadLong(stream);

            if (type == typeof(ulong) || type == typeof(ulong?))
                obj = *(ulong*)&val;
            else
                obj = val;
        }

        /**
         * <summary>Read float.</summary>
         */
        private static unsafe void ReadFloat(GridClientPortableReadContext ctx, Stream stream, Type type, out object obj)
        {
            obj = PU.ReadFloat(stream);
        }

        /**
         * <summary>Read double.</summary>
         */
        private static unsafe void ReadDouble(GridClientPortableReadContext ctx, Stream stream, Type type, out object obj)
        {
            obj = PU.ReadDouble(stream);
        }

        /**
         * <summary>Read date.</summary>
         */
        private static unsafe void ReadDate(GridClientPortableReadContext ctx, Stream stream, Type type, out object obj)
        {
            obj = PU.ReadDate(stream);
        }

        /**
         * <summary>Read string.</summary>
         */
        private static unsafe void ReadString(GridClientPortableReadContext ctx, Stream stream, Type type, out object obj)
        {
            obj = PU.ReadString(stream);
        }

        /**
         * <summary>Read Guid.</summary>
         */
        private static unsafe void ReadGuid(GridClientPortableReadContext ctx, Stream stream, Type type, out object obj)
        {
            obj = PU.ReadGuid(stream);
        }

        /**
         * <summary>Read bool array.</summary>
         */
        private static unsafe void ReadBoolArray(GridClientPortableReadContext ctx, Stream stream, Type type, out object obj)
        {
            obj = PU.ReadBooleanArray(stream);
        }

        /**
         * <summary>Read byte array.</summary>
         */
        private static unsafe void ReadByteArray(GridClientPortableReadContext ctx, Stream stream, Type type, out object obj)
        {
            obj = PU.ReadByteArray(stream, type == typeof(sbyte[]));
        }

        /**
         * <summary>Read short array.</summary>
         */
        private static unsafe void ReadShortArray(GridClientPortableReadContext ctx, Stream stream, Type type, out object obj)
        {
            obj = PU.ReadShortArray(stream, type == typeof(short[]));
        }

        /**
         * <summary>Read char array.</summary>
         */
        private static unsafe void ReadCharArray(GridClientPortableReadContext ctx, Stream stream, Type type, out object obj)
        {
            obj = PU.ReadCharArray(stream);
        }

        /**
         * <summary>Read int array.</summary>
         */
        private static unsafe void ReadIntArray(GridClientPortableReadContext ctx, Stream stream, Type type, out object obj)
        {
            obj = PU.ReadIntArray(stream, type == typeof(int[]));
        }

        /**
         * <summary>Read long array.</summary>
         */
        private static unsafe void ReadLongArray(GridClientPortableReadContext ctx, Stream stream, Type type, out object obj)
        {
            obj = PU.ReadLongArray(stream, type == typeof(long[]));
        }

        /**
         * <summary>Read float array.</summary>
         */
        private static unsafe void ReadFloatArray(GridClientPortableReadContext ctx, Stream stream, Type type, out object obj)
        {
            obj = PU.ReadFloatArray(stream);
        }

        /**
         * <summary>Read double array.</summary>
         */
        private static unsafe void ReadDoubleArray(GridClientPortableReadContext ctx, Stream stream, Type type, out object obj)
        {
            obj = PU.ReadDoubleArray(stream);
        }

        /**
         * <summary>Read date array.</summary>
         */
        private static unsafe void ReadDateArray(GridClientPortableReadContext ctx, Stream stream, Type type, out object obj)
        {
            obj = PU.ReadDateArray(stream);
        }

        /**
         * <summary>Read string array.</summary>
         */
        private static unsafe void ReadStringArray(GridClientPortableReadContext ctx, Stream stream, Type type, out object obj)
        {
            obj = PU.ReadStringArray(stream);
        }

        /**
         * <summary>Read GUID array.</summary>
         */
        private static unsafe void ReadGuidArray(GridClientPortableReadContext ctx, Stream stream, Type type, out object obj)
        {
            obj = PU.ReadGuidArray(stream);
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
