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
    using System.Reflection;

    using GridGain.Client.Portable;

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
        static GridClientPortableSystemHandlers()
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

            // 9. Array.
            READ_HANDLERS[PU.TYPE_ARRAY] = ReadArray;    

            // 10. Predefined collections.
            WRITE_HANDLERS[typeof(ArrayList)] = WriteArrayList;

            // 11. Predefined dictionaries.
            WRITE_HANDLERS[typeof(Hashtable)] = WriteHashtable;

            // 12. Arbitrary collection.
            READ_HANDLERS[PU.TYPE_COLLECTION] = ReadCollection;    

            // 13. Arbitrary dictionary.
            READ_HANDLERS[PU.TYPE_ARRAY] = ReadDictionary;    
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
                // 1. Array?
                if (type.IsArray)
                    return WriteArray;

                if (type.IsGenericType)
                {
                    // 2. Generic dictionary?
                    if (type.GetGenericTypeDefinition().GetInterface(PU.TYP_GENERIC_DICTIONARY.FullName) != null)
                        return WriteGenericDictionary;

                    // 3. Generic collection?
                    if (type.GetGenericTypeDefinition().GetInterface(PU.TYP_GENERIC_COLLECTION.FullName) != null)
                        return WriteGenericCollection;
                }

                // 4. Dictionary?
                if (type.GetInterface(PU.TYP_DICTIONARY.FullName) != null)
                    return WriteDictionary;

                // 5. Collection?
                if (type.GetInterface(PU.TYP_COLLECTION.FullName) != null)
                    return WriteCollection;
            }

            // No special handler found.
            return null;
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
         * <summary>Write boolean.</summary>
         */
        private static void WriteBool(GridClientPortableWriteContext ctx, Stream stream, int pos, object obj)
        {
            WriteCommonHeader(stream, PU.TYPE_BOOL, obj.GetHashCode(), 1);

            PU.WriteBoolean((bool)obj, stream);
        }

        /**
         * <summary>Write sbyte.</summary>
         */
        private static unsafe void WriteSbyte(GridClientPortableWriteContext ctx, Stream stream, int pos, object obj)
        {
            WriteCommonHeader(stream, PU.TYPE_BYTE, obj.GetHashCode(), 1);

            sbyte val = (sbyte)obj;

            PU.WriteByte(*(byte*)&val, stream);
        }

        /**
         * <summary>Write byte.</summary>
         */
        private static void WriteByte(GridClientPortableWriteContext ctx, Stream stream, int pos, object obj)
        {
            WriteCommonHeader(stream, PU.TYPE_BYTE, obj.GetHashCode(), 1);

            PU.WriteByte((byte)obj, stream);
        }

        /**
         * <summary>Write short.</summary>
         */
        private static void WriteShort(GridClientPortableWriteContext ctx, Stream stream, int pos, object obj)
        {
            WriteCommonHeader(stream, PU.TYPE_SHORT, obj.GetHashCode(), 2);

            PU.WriteShort((short)obj, stream);
        }

        /**
         * <summary>Write ushort.</summary>
         */
        private static unsafe void WriteUshort(GridClientPortableWriteContext ctx, Stream stream, int pos, object obj)
        {
            WriteCommonHeader(stream, PU.TYPE_SHORT, obj.GetHashCode(), 2);

            ushort val = (ushort)obj;

            PU.WriteShort(*(short*)&val, stream);
        }

        /**
         * <summary>Write char.</summary>
         */
        private static unsafe void WriteChar(GridClientPortableWriteContext ctx, Stream stream, int pos, object obj)
        {
            WriteCommonHeader(stream, PU.TYPE_CHAR, obj.GetHashCode(), 2);

            char val = (char)obj;

            PU.WriteShort(*(short*)&val, stream);
        }

        /**
         * <summary>Write int.</summary>
         */
        private static void WriteInt(GridClientPortableWriteContext ctx, Stream stream, int pos, object obj)
        {
            WriteCommonHeader(stream, PU.TYPE_INT, obj.GetHashCode(), 4);

            PU.WriteInt((int)obj, stream);
        }

        /**
         * <summary>Write uint.</summary>
         */
        private static unsafe void WriteUint(GridClientPortableWriteContext ctx, Stream stream, int pos, object obj)
        {
            WriteCommonHeader(stream, PU.TYPE_INT, obj.GetHashCode(), 4);

            uint val = (uint)obj;

            PU.WriteInt(*(int*)&val, stream);
        }

        /**
         * <summary>Write long.</summary>
         */
        private static void WriteLong(GridClientPortableWriteContext ctx, Stream stream, int pos, object obj)
        {
            WriteCommonHeader(stream, PU.TYPE_LONG, obj.GetHashCode(), 8);

            PU.WriteLong((long)obj, stream);
        }

        /**
         * <summary>Write ulong.</summary>
         */
        private static unsafe void WriteUlong(GridClientPortableWriteContext ctx, Stream stream, int pos, object obj)
        {
            WriteCommonHeader(stream, PU.TYPE_LONG, obj.GetHashCode(), 8);

            ulong val = (ulong)obj;

            PU.WriteLong(*(long*)&val, stream);
        }

        /**
         * <summary>Write float.</summary>
         */
        private static void WriteFloat(GridClientPortableWriteContext ctx, Stream stream, int pos, object obj)
        {
            WriteCommonHeader(stream, PU.TYPE_FLOAT, obj.GetHashCode(), 4);

            PU.WriteFloat((float)obj, stream);
        }

        /**
         * <summary>Write double.</summary>
         */
        private static void WriteDouble(GridClientPortableWriteContext ctx, Stream stream, int pos, object obj)
        {
            WriteCommonHeader(stream, PU.TYPE_DOUBLE, obj.GetHashCode(), 8);

            PU.WriteDouble((double)obj, stream);
        }

        /**
         * <summary>Write date.</summary>
         */
        private static void WriteDate(GridClientPortableWriteContext ctx, Stream stream, int pos, object obj)
        {
            DateTime obj0 = (DateTime)obj;

            WriteCommonHeader(stream, PU.TYPE_DATE, obj0.GetHashCode(), 1 + 8);

            PU.WriteDate(obj0, stream);
        }

        /**
         * <summary>Write string.</summary>
         */
        private static void WriteString(GridClientPortableWriteContext ctx, Stream stream, int pos, object obj)
        {
            string obj0 = (string)obj;

            WriteCommonHeader(stream, PU.TYPE_STRING, PU.StringHashCode(obj0));

            PU.WriteString(obj0, stream);

            WriteLength(stream, pos, stream.Position, 0);
        }

        /**
         * <summary>Write Guid.</summary>
         */
        private static void WriteGuid(GridClientPortableWriteContext ctx, Stream stream, int pos, object obj)
        {
            Guid obj0 = (Guid)obj;

            WriteCommonHeader(stream, PU.TYPE_GUID, obj0.GetHashCode(), 1 + 16);

            PU.WriteGuid(obj0, stream);
        }

        /**
         * <summary>Write bool array.</summary>
         */
        private static void WriteBoolArray(GridClientPortableWriteContext ctx, Stream stream, int pos, object obj)
        {
            bool[] arr = (bool[])obj;

            WriteCommonHeader(stream, PU.TYPE_ARRAY_BOOL, obj.GetHashCode(), 4 + (arr != null ? arr.Length : 0));

            PU.WriteBooleanArray(arr, stream);
        }

        /**
         * <summary>Write byte array.</summary>
         */
        private static void WriteByteArray(GridClientPortableWriteContext ctx, Stream stream, int pos, object obj)
        {
            byte[] arr = (byte[])obj;

            WriteCommonHeader(stream, PU.TYPE_ARRAY_BYTE, obj.GetHashCode(), 4 + (arr != null ? 4 + arr.Length : 0));

            PU.WriteByteArray(arr, stream);
        }

        /**
         * <summary>Write sbyte array.</summary>
         */
        private static void WriteSbyteArray(GridClientPortableWriteContext ctx, Stream stream, int pos, object obj)
        {
            byte[] arr = (byte[])(Array)obj;

            WriteCommonHeader(stream, PU.TYPE_ARRAY_BYTE, obj.GetHashCode(), 4 + (arr != null ? arr.Length : 0));

            PU.WriteByteArray(arr, stream);
        }

        /**
         * <summary>Write short array.</summary>
         */
        private static void WriteShortArray(GridClientPortableWriteContext ctx, Stream stream, int pos, object obj)
        {
            short[] arr = (short[])obj;

            WriteCommonHeader(stream, PU.TYPE_ARRAY_SHORT, obj.GetHashCode(), 4 + (arr != null ? 4 + arr.Length * 2 : 0));

            PU.WriteShortArray(arr, stream);
        }

        /**
         * <summary>Write ushort array.</summary>
         */
        private static void WriteUshortArray(GridClientPortableWriteContext ctx, Stream stream, int pos, object obj)
        {
            short[] arr = (short[])(Array)obj;

            WriteCommonHeader(stream, PU.TYPE_ARRAY_SHORT, obj.GetHashCode(), 4 + (arr != null ? 4 + arr.Length * 2 : 0));

            PU.WriteShortArray(arr, stream);
        }

        /**
         * <summary>Write char array.</summary>
         */
        private static void WriteCharArray(GridClientPortableWriteContext ctx, Stream stream, int pos, object obj)
        {
            char[] arr = (char[])obj;

            WriteCommonHeader(stream, PU.TYPE_ARRAY_CHAR, obj.GetHashCode(), 4 + (arr != null ? 4 + arr.Length * 2 : 0));

            PU.WriteCharArray(arr, stream);
        }

        /**
         * <summary>Write int array.</summary>
         */
        private static void WriteIntArray(GridClientPortableWriteContext ctx, Stream stream, int pos, object obj)
        {
            int[] arr = (int[])obj;

            WriteCommonHeader(stream, PU.TYPE_ARRAY_INT, obj.GetHashCode(), 4 + (arr != null ? 4 + arr.Length * 4 : 0));

            PU.WriteIntArray(arr, stream);
        }

        /**
         * <summary>Write uint array.</summary>
         */
        private static void WriteUintArray(GridClientPortableWriteContext ctx, Stream stream, int pos, object obj)
        {
            int[] arr = (int[])(Array)obj;

            WriteCommonHeader(stream, PU.TYPE_ARRAY_INT, obj.GetHashCode(), 4 + (arr != null ? 4 + arr.Length * 4 : 0));

            PU.WriteIntArray(arr, stream);
        }

        /**
         * <summary>Write long array.</summary>
         */
        private static void WriteLongArray(GridClientPortableWriteContext ctx, Stream stream, int pos, object obj)
        {
            long[] arr = (long[])obj;

            WriteCommonHeader(stream, PU.TYPE_ARRAY_LONG, obj.GetHashCode(), 4 + (arr != null ? 4 + arr.Length * 8 : 0));

            PU.WriteLongArray(arr, stream);
        }

        /**
         * <summary>Write ulong array.</summary>
         */
        private static void WriteUlongArray(GridClientPortableWriteContext ctx, Stream stream, int pos, object obj)
        {
            long[] arr = (long[])(Array)obj;

            WriteCommonHeader(stream, PU.TYPE_ARRAY_LONG, obj.GetHashCode(), 4 + (arr != null ? 4 + arr.Length * 8 : 0));

            PU.WriteLongArray(arr, stream);
        }

        /**
         * <summary>Write float array.</summary>
         */
        private static void WriteFloatArray(GridClientPortableWriteContext ctx, Stream stream, int pos, object obj)
        {
            float[] arr = (float[])obj;

            WriteCommonHeader(stream, PU.TYPE_ARRAY_FLOAT, obj.GetHashCode(), 4 + (arr != null ? 4 + arr.Length * 4 : 0));

            PU.WriteFloatArray(arr, stream);
        }

        /**
         * <summary>Write double array.</summary>
         */
        private static void WriteDoubleArray(GridClientPortableWriteContext ctx, Stream stream, int pos, object obj)
        {
            double[] arr = (double[])obj;

            WriteCommonHeader(stream, PU.TYPE_ARRAY_DOUBLE, obj.GetHashCode(), 4 + (arr != null ? 4 + arr.Length * 8 : 0));

            PU.WriteDoubleArray(arr, stream);
        }

        /**
         * <summary>Write date array.</summary>
         */
        private static void WriteDateArray(GridClientPortableWriteContext ctx, Stream stream, int pos, object obj)
        {
            DateTime?[] arr = (DateTime?[])obj;

            WriteCommonHeader(stream, PU.TYPE_ARRAY_DATE, obj.GetHashCode());

            PU.WriteDateArray(arr, stream);

            WriteLength(stream, pos, stream.Position, 0);
        }

        /**
         * <summary>Write string array.</summary>
         */
        private static void WriteStringArray(GridClientPortableWriteContext ctx, Stream stream, int pos, object obj)
        {
            string[] arr = (string[])obj;

            WriteCommonHeader(stream, PU.TYPE_ARRAY_STRING, obj.GetHashCode());

            PU.WriteStringArray(arr, stream);

            WriteLength(stream, pos, stream.Position, 0);
        }

        /**
         * <summary>Write Guid array.</summary>
         */
        private static void WriteGuidArray(GridClientPortableWriteContext ctx, Stream stream, int pos, object obj)
        {
            Guid?[] arr = (Guid?[])obj;

            WriteCommonHeader(stream, PU.TYPE_ARRAY_GUID, obj.GetHashCode());

            PU.WriteGuidArray(arr, stream);

            WriteLength(stream, pos, stream.Position, 0);
        }

        /**
         * <summary>Write array.</summary>
         */
        public static void WriteArray(GridClientPortableWriteContext ctx, Stream stream, int pos, object obj)
        {
            WriteCommonHeader(stream, PU.TYPE_ARRAY, obj.GetHashCode());

            PU.WriteArray((Array)obj, ctx);

            WriteLength(stream, pos, stream.Position, 0);
        }

        /**
         * <summary>Write collection.</summary>
         */
        public static void WriteCollection(GridClientPortableWriteContext ctx, Stream stream, int pos, object obj)
        {
            WriteCommonHeader(stream, PU.TYPE_COLLECTION, obj.GetHashCode());

            PU.WriteCollection((ICollection)obj, ctx);

            WriteLength(stream, pos, stream.Position, 0);
        }

        /**
         * <summary>Write ArrayList.</summary>
         */
        public static void WriteArrayList(GridClientPortableWriteContext ctx, Stream stream, int pos, object obj)
        {
            WriteCommonHeader(stream, PU.TYPE_COLLECTION, obj.GetHashCode());

            PU.WriteTypedCollection((ICollection)obj, ctx, PU.COLLECTION_ARRAY_LIST);

            WriteLength(stream, pos, stream.Position, 0);
        }

        /**
         * <summary>Write generic collection.</summary>
         */
        public static void WriteGenericCollection(GridClientPortableWriteContext ctx, Stream stream, int pos, object obj)
        {
            WriteCommonHeader(stream, PU.TYPE_COLLECTION, obj.GetHashCode());

            Type[] typArgs = obj.GetType().GetInterface(PU.TYP_GENERIC_COLLECTION.FullName).GetGenericArguments();

            MethodInfo mthd = PU.MTDH_WRITE_GENERIC_COLLECTION.MakeGenericMethod(typArgs);

            mthd.Invoke(null, new object[] { obj, ctx });

            WriteLength(stream, pos, stream.Position, 0);
        }

        /**
         * <summary>Write dictionary.</summary>
         */
        public static void WriteDictionary(GridClientPortableWriteContext ctx, Stream stream, int pos, object obj)
        {
            WriteCommonHeader(stream, PU.TYPE_DICTIONARY, obj.GetHashCode());

            PU.WriteDictionary((IDictionary)obj, ctx);

            WriteLength(stream, pos, stream.Position, 0);
        }

        /**
         * <summary>Write Hashtable.</summary>
         */
        public static void WriteHashtable(GridClientPortableWriteContext ctx, Stream stream, int pos, object obj)
        {
            WriteCommonHeader(stream, PU.TYPE_DICTIONARY, obj.GetHashCode());

            PU.WriteTypedDictionary((IDictionary)obj, ctx, PU.MAP_HASH_MAP);

            WriteLength(stream, pos, stream.Position, 0);
        }

        /**
         * <summary>Write generic dictionary.</summary>
         */
        public static void WriteGenericDictionary(GridClientPortableWriteContext ctx, Stream stream, int pos, object obj)
        {
            WriteCommonHeader(stream, PU.TYPE_DICTIONARY, obj.GetHashCode());
            
            Type[] typArgs = obj.GetType().GetInterface(PU.TYP_GENERIC_DICTIONARY.FullName).GetGenericArguments();

            MethodInfo mthd = PU.MTDH_WRITE_GENERIC_DICTIONARY.MakeGenericMethod(typArgs);

            mthd.Invoke(null, new object[] { obj, ctx });

            WriteLength(stream, pos, stream.Position, 0);
        }

        /**
         * <summary>Read bool.</summary>
         */
        private static void ReadBool(GridClientPortableReadContext ctx, Stream stream, Type type, out object obj)
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
        private static void ReadFloat(GridClientPortableReadContext ctx, Stream stream, Type type, out object obj)
        {
            obj = PU.ReadFloat(stream);
        }

        /**
         * <summary>Read double.</summary>
         */
        private static void ReadDouble(GridClientPortableReadContext ctx, Stream stream, Type type, out object obj)
        {
            obj = PU.ReadDouble(stream);
        }

        /**
         * <summary>Read date.</summary>
         */
        private static void ReadDate(GridClientPortableReadContext ctx, Stream stream, Type type, out object obj)
        {
            obj = PU.ReadDate(stream);
        }

        /**
         * <summary>Read string.</summary>
         */
        private static void ReadString(GridClientPortableReadContext ctx, Stream stream, Type type, out object obj)
        {
            obj = PU.ReadString(stream);
        }

        /**
         * <summary>Read Guid.</summary>
         */
        private static void ReadGuid(GridClientPortableReadContext ctx, Stream stream, Type type, out object obj)
        {
            obj = PU.ReadGuid(stream);
        }

        /**
         * <summary>Read bool array.</summary>
         */
        private static void ReadBoolArray(GridClientPortableReadContext ctx, Stream stream, Type type, out object obj)
        {
            obj = PU.ReadBooleanArray(stream);
        }

        /**
         * <summary>Read byte array.</summary>
         */
        private static void ReadByteArray(GridClientPortableReadContext ctx, Stream stream, Type type, out object obj)
        {
            obj = PU.ReadByteArray(stream, type == typeof(sbyte[]));
        }

        /**
         * <summary>Read short array.</summary>
         */
        private static void ReadShortArray(GridClientPortableReadContext ctx, Stream stream, Type type, out object obj)
        {
            obj = PU.ReadShortArray(stream, type == typeof(short[]));
        }

        /**
         * <summary>Read char array.</summary>
         */
        private static void ReadCharArray(GridClientPortableReadContext ctx, Stream stream, Type type, out object obj)
        {
            obj = PU.ReadCharArray(stream);
        }

        /**
         * <summary>Read int array.</summary>
         */
        private static void ReadIntArray(GridClientPortableReadContext ctx, Stream stream, Type type, out object obj)
        {
            obj = PU.ReadIntArray(stream, type == typeof(int[]));
        }

        /**
         * <summary>Read long array.</summary>
         */
        private static void ReadLongArray(GridClientPortableReadContext ctx, Stream stream, Type type, out object obj)
        {
            obj = PU.ReadLongArray(stream, type == typeof(long[]));
        }

        /**
         * <summary>Read float array.</summary>
         */
        private static void ReadFloatArray(GridClientPortableReadContext ctx, Stream stream, Type type, out object obj)
        {
            obj = PU.ReadFloatArray(stream);
        }

        /**
         * <summary>Read double array.</summary>
         */
        private static void ReadDoubleArray(GridClientPortableReadContext ctx, Stream stream, Type type, out object obj)
        {
            obj = PU.ReadDoubleArray(stream);
        }

        /**
         * <summary>Read date array.</summary>
         */
        private static void ReadDateArray(GridClientPortableReadContext ctx, Stream stream, Type type, out object obj)
        {
            obj = PU.ReadDateArray(stream);
        }

        /**
         * <summary>Read string array.</summary>
         */
        private static void ReadStringArray(GridClientPortableReadContext ctx, Stream stream, Type type, out object obj)
        {
            obj = PU.ReadStringArray(stream);
        }

        /**
         * <summary>Read GUID array.</summary>
         */
        private static  void ReadGuidArray(GridClientPortableReadContext ctx, Stream stream, Type type, out object obj)
        {
            obj = PU.ReadGuidArray(stream);
        }

        /**
         * <summary>Read array.</summary>
         */
        private static void ReadArray(GridClientPortableReadContext ctx, Stream stream, Type type, out object obj)
        {
            obj = PU.ReadArray(ctx);
        }

        /**
         * <summary>Read collection.</summary>
         */
        private static void ReadCollection(GridClientPortableReadContext ctx, Stream stream, Type type, out object obj)
        {
            // 1. Generic?
            if (type.IsGenericType)
            {
                Type genTyp = type.GetInterface(PU.TYP_GENERIC_COLLECTION.FullName);

                if (genTyp != null)
                {
                    obj = PU.MTDH_READ_GENERIC_COLLECTION.MakeGenericMethod(genTyp.GetGenericArguments()).Invoke(null, new object[] { ctx, null });

                    return;
                }
            }

            // 2. Non-generic.
            obj = PU.ReadCollection(ctx, CreateArrayList, AddToArrayList);
        }

        /**
         * <summary>Read dictionary.</summary>
         */
        private static void ReadDictionary(GridClientPortableReadContext ctx, Stream stream, Type type, out object obj)
        {
            // 1. Generic?
            if (type.IsGenericType)
            {
                Type genTyp = type.GetInterface(PU.TYP_GENERIC_DICTIONARY.FullName);

                if (genTyp != null)
                {
                    obj = PU.MTDH_READ_GENERIC_DICTIONARY.MakeGenericMethod(genTyp.GetGenericArguments()).Invoke(null, new object[] { ctx, null });

                    return;
                }
            }

            // 2. Non-generic.
            obj = PU.ReadDictionary(ctx, CreateHashtable);
        }
        
        /**
         * <summary>Write common header without length.</summary>
         * <param name="stream">Stream.</param>
         * <param name="typeId">Type ID.</param>
         * <param name="hashCode">Hash code.</param>
         */
        private static void WriteCommonHeader(Stream stream, int typeId, int hashCode)
        {
            stream.WriteByte(PU.HDR_FULL);       // 0: Full form.
            PU.WriteBoolean(false, stream);      // 1: System type.
            PU.WriteInt(typeId, stream);         // 1: Type ID.
            PU.WriteInt(hashCode, stream);       // 6: Hash code.
            stream.Seek(8, SeekOrigin.Current);  // 10: Length is not know nad no raw data.
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
            stream.WriteByte(PU.HDR_FULL);       // 0: Full form.
            PU.WriteBoolean(false, stream);      // 1: System type.
            PU.WriteInt(typeId, stream);         // 2: Type ID.
            PU.WriteInt(hashCode, stream);       // 6: Hash code.
            PU.WriteInt(HDR_LEN + len, stream);  // 10: Length = HDR_LEN(18) + data length.
            stream.Seek(4, SeekOrigin.Current);  // 14: No raw data.
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
         * <summary>Create new ArrayList.</summary>
         * <param name="len">Length.</param>
         * <returns>ArrayList.</returns>
         */
        public static ICollection CreateArrayList(int len)
        {
            return new ArrayList(len);
        }

        /**
         * <summary>Add element to array list.</summary>
         * <param name="col">Array list.</param>
         * <param name="elem">Element.</param>
         */
        public static void AddToArrayList(ICollection col, object elem)
        {
            (col as ArrayList).Add(elem);
        }

        /**
         * <summary>Create new List.</summary>
         * <param name="len">Length.</param>
         * <returns>List.</returns>
         */
        public static ICollection<T> CreateList<T>(int len)
        {
            return new List<T>(len);
        }

        /**
         * <summary>Create new LinkedList.</summary>
         * <param name="len">Length.</param>
         * <returns>LinkedList.</returns>
         */
        public static ICollection<T> CreateLinkedList<T>(int len)
        {
            return new LinkedList<T>();
        }

        /**
         * <summary>Create new HashSet.</summary>
         * <param name="len">Length.</param>
         * <returns>HashSet.</returns>
         */
        public static ICollection<T> CreateHashSet<T>(int len)
        {
            return new HashSet<T>();
        }

        /**
         * <summary>Create new SortedSet.</summary>
         * <param name="len">Length.</param>
         * <returns>SortedSet.</returns>
         */
        public static ICollection<T> CreateSortedSet<T>(int len)
        {
            return new SortedSet<T>(); 
        }

        /**
         * <summary>Create new Hashtable.</summary>
         * <param name="len">Length.</param>
         * <returns>Hashtable.</returns>
         */
        public static IDictionary CreateHashtable(int len)
        {
            return new Hashtable(len);
        }

        /**
         * <summary>Create new Dictionary.</summary>
         * <param name="len">Length.</param>
         * <returns>Dictionary.</returns>
         */
        public static IDictionary<K, V> CreateDictionary<K, V>(int len)
        {
            return new Dictionary<K, V>(len);
        }

        /**
         * <summary>Create new SortedDictionary.</summary>
         * <param name="len">Length.</param>
         * <returns>SortedDictionary.</returns>
         */
        public static IDictionary<K, V> CreateSortedDictionary<K, V>(int len)
        {
            return new SortedDictionary<K, V>();
        }

        /**
         * <summary>Create new ConcurrentDictionary.</summary>
         * <param name="len">Length.</param>
         * <returns>ConcurrentDictionary.</returns>
         */
        public static IDictionary<K, V> CreateConcurrentDictionary<K, V>(int len)
        {
            return new ConcurrentDictionary<K, V>(Environment.ProcessorCount, len);
        }
    }
}
