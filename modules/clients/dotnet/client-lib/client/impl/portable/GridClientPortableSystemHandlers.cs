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
    using System.Diagnostics;
    using System.IO;
    using System.Reflection;

    using GridGain.Client.Portable;

    using PU = GridGain.Client.Impl.Portable.GridClientPortableUilts;

    /**
     * <summary>Write delegate.</summary> 
     * <param name="ctx">Write context.</param>
     * <param name="pos">Position where write started.</param>
     * <param name="obj">Object to write.</param>
     */
    internal delegate void GridClientPortableSystemWriteDelegate(GridClientPortableWriteContext ctx, int pos, 
        object obj);

    /**
     * <summary>Read delegate.</summary> 
     * <param name="ctx">Read context.</param>
     * <param name="type">Type.</param>
     * <param name="obj">Object to read to.</param>
     */
    internal delegate void GridClientPortableSystemReadDelegate(GridClientPortableReadContext ctx, Type type,
        out object obj);

    /**
     * <summary>FIeld delegate.</summary> 
     * <param name="stream">Memory stream.</param>
     * <param name="marsh">Marshaller.</param>
     * <returns>Object.</returns>
     */
    internal delegate object GridClientPortableSystemFieldDelegate(MemoryStream stream, 
        GridClientPortableMarshaller marsh);

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

        /** Field handlers. */
        private static readonly IDictionary<int, GridClientPortableSystemFieldDelegate> FIELD_HANDLERS =
            new Dictionary<int, GridClientPortableSystemFieldDelegate>(); 

        /**
         * <summary>Static initializer.</summary>
         */ 
        static GridClientPortableSystemHandlers()
        {
            // 1. Primitives.
            WRITE_HANDLERS[typeof(bool)] = WriteBool;
            READ_HANDLERS[PU.TYPE_BOOL] = ReadBool;
            FIELD_HANDLERS[PU.TYPE_BOOL] = ReadBoolField;

            WRITE_HANDLERS[typeof(sbyte)] = WriteSbyte;
            WRITE_HANDLERS[typeof(byte)] = WriteByte;
            READ_HANDLERS[PU.TYPE_BYTE] = ReadByte;
            FIELD_HANDLERS[PU.TYPE_BYTE] = ReadByteField;

            WRITE_HANDLERS[typeof(short)] = WriteShort;
            WRITE_HANDLERS[typeof(ushort)] = WriteUshort;
            READ_HANDLERS[PU.TYPE_SHORT] = ReadShort;
            FIELD_HANDLERS[PU.TYPE_SHORT] = ReadShortField;

            WRITE_HANDLERS[typeof(char)] = WriteChar;
            READ_HANDLERS[PU.TYPE_CHAR] = ReadChar;
            FIELD_HANDLERS[PU.TYPE_CHAR] = ReadCharField;

            WRITE_HANDLERS[typeof(int)] = WriteInt;
            WRITE_HANDLERS[typeof(uint)] = WriteUint;
            READ_HANDLERS[PU.TYPE_INT] = ReadInt;
            FIELD_HANDLERS[PU.TYPE_SHORT] = ReadShortField;

            WRITE_HANDLERS[typeof(long)] = WriteLong;
            WRITE_HANDLERS[typeof(ulong)] = WriteUlong;
            READ_HANDLERS[PU.TYPE_LONG] = ReadLong;
            FIELD_HANDLERS[PU.TYPE_LONG] = ReadLongField;

            WRITE_HANDLERS[typeof(float)] = WriteFloat;
            READ_HANDLERS[PU.TYPE_FLOAT] = ReadFloat;
            FIELD_HANDLERS[PU.TYPE_FLOAT] = ReadFloatField;

            WRITE_HANDLERS[typeof(double)] = WriteDouble;
            READ_HANDLERS[PU.TYPE_DOUBLE] = ReadDouble;
            FIELD_HANDLERS[PU.TYPE_DOUBLE] = ReadDoubleField;

            // 2. Date.
            WRITE_HANDLERS[typeof(DateTime)] = WriteDate;
            READ_HANDLERS[PU.TYPE_DATE] = ReadDate;
            FIELD_HANDLERS[PU.TYPE_DATE] = ReadDateField;

            // 3. String.
            WRITE_HANDLERS[typeof(string)] = WriteString;
            READ_HANDLERS[PU.TYPE_STRING] = ReadString;
            FIELD_HANDLERS[PU.TYPE_STRING] = ReadStringField;

            // 4. Guid.
            WRITE_HANDLERS[typeof(Guid)] = WriteGuid;
            READ_HANDLERS[PU.TYPE_GUID] = ReadGuid;
            FIELD_HANDLERS[PU.TYPE_GUID] = ReadGuidField;
            
            // 5. Primitive arrays.
            WRITE_HANDLERS[typeof(bool[])] = WriteBoolArray;
            READ_HANDLERS[PU.TYPE_ARRAY_BOOL] = ReadBoolArray;
            FIELD_HANDLERS[PU.TYPE_ARRAY_BOOL] = ReadBoolArrayField; 

            WRITE_HANDLERS[typeof(byte[])] = WriteByteArray;
            WRITE_HANDLERS[typeof(sbyte[])] = WriteSbyteArray;
            READ_HANDLERS[PU.TYPE_ARRAY_BYTE] = ReadByteArray;
            FIELD_HANDLERS[PU.TYPE_ARRAY_BYTE] = ReadByteArrayField; 

            WRITE_HANDLERS[typeof(short[])] = WriteShortArray;
            WRITE_HANDLERS[typeof(ushort[])] = WriteUshortArray;
            READ_HANDLERS[PU.TYPE_ARRAY_SHORT] = ReadShortArray;
            FIELD_HANDLERS[PU.TYPE_ARRAY_SHORT] = ReadShortArrayField; 

            WRITE_HANDLERS[typeof(char[])] = WriteCharArray;
            READ_HANDLERS[PU.TYPE_ARRAY_CHAR] = ReadCharArray;
            FIELD_HANDLERS[PU.TYPE_ARRAY_CHAR] = ReadCharArrayField; 

            WRITE_HANDLERS[typeof(int[])] = WriteIntArray;
            WRITE_HANDLERS[typeof(uint[])] = WriteUintArray;
            READ_HANDLERS[PU.TYPE_ARRAY_INT] = ReadIntArray;
            FIELD_HANDLERS[PU.TYPE_ARRAY_INT] = ReadIntArrayField; 

            WRITE_HANDLERS[typeof(long[])] = WriteLongArray;
            WRITE_HANDLERS[typeof(ulong[])] = WriteUlongArray;
            READ_HANDLERS[PU.TYPE_ARRAY_LONG] = ReadLongArray;
            FIELD_HANDLERS[PU.TYPE_ARRAY_LONG] = ReadLongArrayField; 

            WRITE_HANDLERS[typeof(float[])] = WriteFloatArray;
            READ_HANDLERS[PU.TYPE_ARRAY_FLOAT] = ReadFloatArray;
            FIELD_HANDLERS[PU.TYPE_ARRAY_FLOAT] = ReadFloatArrayField; 

            WRITE_HANDLERS[typeof(double[])] = WriteDoubleArray;
            READ_HANDLERS[PU.TYPE_ARRAY_DOUBLE] = ReadDoubleArray;
            FIELD_HANDLERS[PU.TYPE_ARRAY_DOUBLE] = ReadDoubleArrayField; 

            // 6. Date array.
            WRITE_HANDLERS[typeof(DateTime?[])] = WriteDateArray;
            READ_HANDLERS[PU.TYPE_ARRAY_DATE] = ReadDateArray;
            FIELD_HANDLERS[PU.TYPE_ARRAY_DATE] = ReadDateArrayField; 

            // 7. String array.
            WRITE_HANDLERS[typeof(string[])] = WriteStringArray;
            READ_HANDLERS[PU.TYPE_ARRAY_STRING] = ReadStringArray;
            FIELD_HANDLERS[PU.TYPE_ARRAY_STRING] = ReadStringArrayField; 

            // 8. Guid array.
            WRITE_HANDLERS[typeof(Guid?[])] = WriteGuidArray;
            READ_HANDLERS[PU.TYPE_ARRAY_GUID] = ReadGuidArray;
            FIELD_HANDLERS[PU.TYPE_ARRAY_GUID] = ReadGuidArrayField;   

            // 9. Array.
            READ_HANDLERS[PU.TYPE_ARRAY] = ReadArray;
            FIELD_HANDLERS[PU.TYPE_ARRAY] = ReadArrayField;   

            // 10. Predefined collections.
            WRITE_HANDLERS[typeof(ArrayList)] = WriteArrayList;

            // 11. Predefined dictionaries.
            WRITE_HANDLERS[typeof(Hashtable)] = WriteHashtable;

            // 12. Arbitrary collection.
            READ_HANDLERS[PU.TYPE_COLLECTION] = ReadCollection;
            FIELD_HANDLERS[PU.TYPE_COLLECTION] = ReadCollectionField;   

            // 13. Arbitrary dictionary.
            READ_HANDLERS[PU.TYPE_DICTIONARY] = ReadDictionary;
            FIELD_HANDLERS[PU.TYPE_DICTIONARY] = ReadDictionaryField;   
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

                // 2. Collection?
                GridClientPortableCollectionInfo info = GridClientPortableCollectionInfo.Info(type);

                if (info.IsAny)
                    return info.WriteHandler;
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
         * <summary>Get field handler for type ID.</summary>
         * <param name="typeId">Type ID.</param>
         * <returns>Handler or null if cannot be hanled in special way.</returns>
         */
        public static GridClientPortableSystemFieldDelegate FieldHandler(int typeId)
        {
            GridClientPortableSystemFieldDelegate handler;

            FIELD_HANDLERS.TryGetValue(typeId, out handler);

            return handler;
        }

        /**
         * <summary>Write boolean.</summary>
         */
        private static void WriteBool(GridClientPortableWriteContext ctx, int pos, object obj)
        {
            WriteCommonHeader(ctx.Stream, PU.TYPE_BOOL, obj.GetHashCode(), 1);

            PU.WriteBoolean((bool)obj, ctx.Stream);
        }

        /**
         * <summary>Write sbyte.</summary>
         */
        private static unsafe void WriteSbyte(GridClientPortableWriteContext ctx, int pos, object obj)
        {
            WriteCommonHeader(ctx.Stream, PU.TYPE_BYTE, obj.GetHashCode(), 1);

            sbyte val = (sbyte)obj;

            PU.WriteByte(*(byte*)&val, ctx.Stream);
        }

        /**
         * <summary>Write byte.</summary>
         */
        private static void WriteByte(GridClientPortableWriteContext ctx, int pos, object obj)
        {
            WriteCommonHeader(ctx.Stream, PU.TYPE_BYTE, obj.GetHashCode(), 1);

            PU.WriteByte((byte)obj, ctx.Stream);
        }

        /**
         * <summary>Write short.</summary>
         */
        private static void WriteShort(GridClientPortableWriteContext ctx, int pos, object obj)
        {
            WriteCommonHeader(ctx.Stream, PU.TYPE_SHORT, obj.GetHashCode(), 2);

            PU.WriteShort((short)obj, ctx.Stream);
        }

        /**
         * <summary>Write ushort.</summary>
         */
        private static unsafe void WriteUshort(GridClientPortableWriteContext ctx, int pos, object obj)
        {
            WriteCommonHeader(ctx.Stream, PU.TYPE_SHORT, obj.GetHashCode(), 2);

            ushort val = (ushort)obj;

            PU.WriteShort(*(short*)&val, ctx.Stream);
        }

        /**
         * <summary>Write char.</summary>
         */
        private static unsafe void WriteChar(GridClientPortableWriteContext ctx, int pos, object obj)
        {
            WriteCommonHeader(ctx.Stream, PU.TYPE_CHAR, obj.GetHashCode(), 2);

            char val = (char)obj;

            PU.WriteShort(*(short*)&val, ctx.Stream);
        }

        /**
         * <summary>Write int.</summary>
         */
        private static void WriteInt(GridClientPortableWriteContext ctx, int pos, object obj)
        {
            WriteCommonHeader(ctx.Stream, PU.TYPE_INT, obj.GetHashCode(), 4);

            PU.WriteInt((int)obj, ctx.Stream);
        }

        /**
         * <summary>Write uint.</summary>
         */
        private static unsafe void WriteUint(GridClientPortableWriteContext ctx, int pos, object obj)
        {
            WriteCommonHeader(ctx.Stream, PU.TYPE_INT, obj.GetHashCode(), 4);

            uint val = (uint)obj;

            PU.WriteInt(*(int*)&val, ctx.Stream);
        }

        /**
         * <summary>Write long.</summary>
         */
        private static void WriteLong(GridClientPortableWriteContext ctx, int pos, object obj)
        {
            WriteCommonHeader(ctx.Stream, PU.TYPE_LONG, obj.GetHashCode(), 8);

            PU.WriteLong((long)obj, ctx.Stream);
        }

        /**
         * <summary>Write ulong.</summary>
         */
        private static unsafe void WriteUlong(GridClientPortableWriteContext ctx, int pos, object obj)
        {
            WriteCommonHeader(ctx.Stream, PU.TYPE_LONG, obj.GetHashCode(), 8);

            ulong val = (ulong)obj;

            PU.WriteLong(*(long*)&val, ctx.Stream);
        }

        /**
         * <summary>Write float.</summary>
         */
        private static void WriteFloat(GridClientPortableWriteContext ctx, int pos, object obj)
        {
            WriteCommonHeader(ctx.Stream, PU.TYPE_FLOAT, obj.GetHashCode(), 4);

            PU.WriteFloat((float)obj, ctx.Stream);
        }

        /**
         * <summary>Write double.</summary>
         */
        private static void WriteDouble(GridClientPortableWriteContext ctx, int pos, object obj)
        {
            WriteCommonHeader(ctx.Stream, PU.TYPE_DOUBLE, obj.GetHashCode(), 8);

            PU.WriteDouble((double)obj, ctx.Stream);
        }

        /**
         * <summary>Write date.</summary>
         */
        private static void WriteDate(GridClientPortableWriteContext ctx, int pos, object obj)
        {
            DateTime obj0 = (DateTime)obj;

            WriteCommonHeader(ctx.Stream, PU.TYPE_DATE, obj0.GetHashCode(), 1 + 8);

            PU.WriteDate(obj0, ctx.Stream);
        }

        /**
         * <summary>Write string.</summary>
         */
        private static void WriteString(GridClientPortableWriteContext ctx, int pos, object obj)
        {
            string obj0 = (string)obj;

            WriteCommonHeader(ctx.Stream, PU.TYPE_STRING, PU.StringHashCode(obj0));

            PU.WriteString(obj0, ctx.Stream);

            WriteLength(ctx.Stream, pos, ctx.Stream.Position, pos + HDR_LEN);
        }

        /**
         * <summary>Write Guid.</summary>
         */
        private static void WriteGuid(GridClientPortableWriteContext ctx, int pos, object obj)
        {
            Guid obj0 = (Guid)obj;

            WriteCommonHeader(ctx.Stream, PU.TYPE_GUID, obj0.GetHashCode(), 1 + 16);

            PU.WriteGuid(obj0, ctx.Stream);
        }

        /**
         * <summary>Write bool array.</summary>
         */
        private static void WriteBoolArray(GridClientPortableWriteContext ctx, int pos, object obj)
        {
            bool[] arr = (bool[])obj;

            WriteCommonHeader(ctx.Stream, PU.TYPE_ARRAY_BOOL, obj.GetHashCode(), 4 + (arr != null ? arr.Length : 0));

            PU.WriteBooleanArray(arr, ctx.Stream);
        }

        /**
         * <summary>Write byte array.</summary>
         */
        private static void WriteByteArray(GridClientPortableWriteContext ctx, int pos, object obj)
        {
            byte[] arr = (byte[])obj;

            WriteCommonHeader(ctx.Stream, PU.TYPE_ARRAY_BYTE, obj.GetHashCode(), 4 + (arr != null ? 4 + arr.Length : 0));

            PU.WriteByteArray(arr, ctx.Stream);
        }

        /**
         * <summary>Write sbyte array.</summary>
         */
        private static void WriteSbyteArray(GridClientPortableWriteContext ctx, int pos, object obj)
        {
            byte[] arr = (byte[])(Array)obj;

            WriteCommonHeader(ctx.Stream, PU.TYPE_ARRAY_BYTE, obj.GetHashCode(), 4 + (arr != null ? arr.Length : 0));

            PU.WriteByteArray(arr, ctx.Stream);
        }

        /**
         * <summary>Write short array.</summary>
         */
        private static void WriteShortArray(GridClientPortableWriteContext ctx, int pos, object obj)
        {
            short[] arr = (short[])obj;

            WriteCommonHeader(ctx.Stream, PU.TYPE_ARRAY_SHORT, obj.GetHashCode(), 4 + (arr != null ? 4 + arr.Length * 2 : 0));

            PU.WriteShortArray(arr, ctx.Stream);
        }

        /**
         * <summary>Write ushort array.</summary>
         */
        private static void WriteUshortArray(GridClientPortableWriteContext ctx, int pos, object obj)
        {
            short[] arr = (short[])(Array)obj;

            WriteCommonHeader(ctx.Stream, PU.TYPE_ARRAY_SHORT, obj.GetHashCode(), 4 + (arr != null ? 4 + arr.Length * 2 : 0));

            PU.WriteShortArray(arr, ctx.Stream);
        }

        /**
         * <summary>Write char array.</summary>
         */
        private static void WriteCharArray(GridClientPortableWriteContext ctx, int pos, object obj)
        {
            char[] arr = (char[])obj;

            WriteCommonHeader(ctx.Stream, PU.TYPE_ARRAY_CHAR, obj.GetHashCode(), 4 + (arr != null ? 4 + arr.Length * 2 : 0));

            PU.WriteCharArray(arr, ctx.Stream);
        }

        /**
         * <summary>Write int array.</summary>
         */
        private static void WriteIntArray(GridClientPortableWriteContext ctx, int pos, object obj)
        {
            int[] arr = (int[])obj;

            WriteCommonHeader(ctx.Stream, PU.TYPE_ARRAY_INT, obj.GetHashCode(), 4 + (arr != null ? 4 + arr.Length * 4 : 0));

            PU.WriteIntArray(arr, ctx.Stream);
        }

        /**
         * <summary>Write uint array.</summary>
         */
        private static void WriteUintArray(GridClientPortableWriteContext ctx, int pos, object obj)
        {
            int[] arr = (int[])(Array)obj;

            WriteCommonHeader(ctx.Stream, PU.TYPE_ARRAY_INT, obj.GetHashCode(), 4 + (arr != null ? 4 + arr.Length * 4 : 0));

            PU.WriteIntArray(arr, ctx.Stream);
        }

        /**
         * <summary>Write long array.</summary>
         */
        private static void WriteLongArray(GridClientPortableWriteContext ctx, int pos, object obj)
        {
            long[] arr = (long[])obj;

            WriteCommonHeader(ctx.Stream, PU.TYPE_ARRAY_LONG, obj.GetHashCode(), 4 + (arr != null ? 4 + arr.Length * 8 : 0));

            PU.WriteLongArray(arr, ctx.Stream);
        }

        /**
         * <summary>Write ulong array.</summary>
         */
        private static void WriteUlongArray(GridClientPortableWriteContext ctx, int pos, object obj)
        {
            long[] arr = (long[])(Array)obj;

            WriteCommonHeader(ctx.Stream, PU.TYPE_ARRAY_LONG, obj.GetHashCode(), 4 + (arr != null ? 4 + arr.Length * 8 : 0));

            PU.WriteLongArray(arr, ctx.Stream);
        }

        /**
         * <summary>Write float array.</summary>
         */
        private static void WriteFloatArray(GridClientPortableWriteContext ctx, int pos, object obj)
        {
            float[] arr = (float[])obj;

            WriteCommonHeader(ctx.Stream, PU.TYPE_ARRAY_FLOAT, obj.GetHashCode(), 4 + (arr != null ? 4 + arr.Length * 4 : 0));

            PU.WriteFloatArray(arr, ctx.Stream);
        }

        /**
         * <summary>Write double array.</summary>
         */
        private static void WriteDoubleArray(GridClientPortableWriteContext ctx, int pos, object obj)
        {
            double[] arr = (double[])obj;

            WriteCommonHeader(ctx.Stream, PU.TYPE_ARRAY_DOUBLE, obj.GetHashCode(), 4 + (arr != null ? 4 + arr.Length * 8 : 0));

            PU.WriteDoubleArray(arr, ctx.Stream);
        }

        /**
         * <summary>Write date array.</summary>
         */
        private static void WriteDateArray(GridClientPortableWriteContext ctx, int pos, object obj)
        {
            DateTime?[] arr = (DateTime?[])obj;

            WriteCommonHeader(ctx.Stream, PU.TYPE_ARRAY_DATE, obj.GetHashCode());

            PU.WriteDateArray(arr, ctx.Stream);

            WriteLength(ctx.Stream, pos, ctx.Stream.Position, pos + HDR_LEN);
        }

        /**
         * <summary>Write string array.</summary>
         */
        private static void WriteStringArray(GridClientPortableWriteContext ctx, int pos, object obj)
        {
            string[] arr = (string[])obj;

            WriteCommonHeader(ctx.Stream, PU.TYPE_ARRAY_STRING, obj.GetHashCode());

            PU.WriteStringArray(arr, ctx.Stream);

            WriteLength(ctx.Stream, pos, ctx.Stream.Position, pos + HDR_LEN);
        }

        /**
         * <summary>Write Guid array.</summary>
         */
        private static void WriteGuidArray(GridClientPortableWriteContext ctx, int pos, object obj)
        {
            Guid?[] arr = (Guid?[])obj;

            WriteCommonHeader(ctx.Stream, PU.TYPE_ARRAY_GUID, obj.GetHashCode());

            PU.WriteGuidArray(arr, ctx.Stream);

            WriteLength(ctx.Stream, pos, ctx.Stream.Position, pos + HDR_LEN);
        }

        /**
         * <summary>Write array.</summary>
         */
        public static void WriteArray(GridClientPortableWriteContext ctx, int pos, object obj)
        {
            WriteCommonHeader(ctx.Stream, PU.TYPE_ARRAY, obj.GetHashCode());

            PU.WriteArray((Array)obj, ctx);

            WriteLength(ctx.Stream, pos, ctx.Stream.Position, pos + HDR_LEN);
        }

        /**
         * <summary>Write collection.</summary>
         */
        public static void WriteCollection(GridClientPortableWriteContext ctx, int pos, object obj)
        {
            WriteCommonHeader(ctx.Stream, PU.TYPE_COLLECTION, obj.GetHashCode());

            PU.WriteCollection((ICollection)obj, ctx);

            WriteLength(ctx.Stream, pos, ctx.Stream.Position, pos + HDR_LEN);
        }

        /**
         * <summary>Write generic collection.</summary>
         */
        public static void WriteGenericCollection(GridClientPortableWriteContext ctx, int pos, object obj)
        {
            GridClientPortableCollectionInfo info = GridClientPortableCollectionInfo.Info(obj.GetType());

            Debug.Assert(info.IsGenericCollection, "Not generic collection: " + obj.GetType().FullName);

            WriteCommonHeader(ctx.Stream, PU.TYPE_COLLECTION, obj.GetHashCode());

            info.GenericWriteMethod.Invoke(null, new object[] { obj, ctx });

            WriteLength(ctx.Stream, pos, ctx.Stream.Position, pos + HDR_LEN);
        }

        /**
         * <summary>Write dictionary.</summary>
         */
        public static void WriteDictionary(GridClientPortableWriteContext ctx, int pos, object obj)
        {
            WriteCommonHeader(ctx.Stream, PU.TYPE_DICTIONARY, obj.GetHashCode());

            PU.WriteDictionary((IDictionary)obj, ctx);

            WriteLength(ctx.Stream, pos, ctx.Stream.Position, pos + HDR_LEN);
        }

        /**
         * <summary>Write generic dictionary.</summary>
         */
        public static void WriteGenericDictionary(GridClientPortableWriteContext ctx, int pos, object obj)
        {
            GridClientPortableCollectionInfo info = GridClientPortableCollectionInfo.Info(obj.GetType());

            Debug.Assert(info.IsGenericDictionary, "Not generic dictionary: " + obj.GetType().FullName);

            WriteCommonHeader(ctx.Stream, PU.TYPE_DICTIONARY, obj.GetHashCode());

            info.GenericWriteMethod.Invoke(null, new object[] { obj, ctx });

            WriteLength(ctx.Stream, pos, ctx.Stream.Position, pos + HDR_LEN);
        }

        /**
         * <summary>Write ArrayList.</summary>
         */
        public static void WriteArrayList(GridClientPortableWriteContext ctx, int pos, object obj)
        {
            WriteCommonHeader(ctx.Stream, PU.TYPE_COLLECTION, obj.GetHashCode());

            PU.WriteTypedCollection((ICollection)obj, ctx, PU.COLLECTION_ARRAY_LIST);

            WriteLength(ctx.Stream, pos, ctx.Stream.Position, pos + HDR_LEN);
        }

        /**
         * <summary>Write Hashtable.</summary>
         */
        public static void WriteHashtable(GridClientPortableWriteContext ctx, int pos, object obj)
        {
            WriteCommonHeader(ctx.Stream, PU.TYPE_DICTIONARY, obj.GetHashCode());

            PU.WriteTypedDictionary((IDictionary)obj, ctx, PU.MAP_HASH_MAP);

            WriteLength(ctx.Stream, pos, ctx.Stream.Position, pos + HDR_LEN);
        }

        /**
         * <summary>Read bool.</summary>
         */
        private static void ReadBool(GridClientPortableReadContext ctx, Type type, out object obj)
        {
            obj = PU.ReadBoolean(ctx.Stream);
        }

        /**
         * <summary>Read bool field.</summary>
         */
        private static object ReadBoolField(MemoryStream stream, GridClientPortableMarshaller marsh)
        {
            return PU.ReadBoolean(stream);
        }

        /**
         * <summary>Read byte.</summary>
         */
        private static unsafe void ReadByte(GridClientPortableReadContext ctx, Type type, out object obj)
        {
            byte val = PU.ReadByte(ctx.Stream);

            if (type == typeof(sbyte) || type == typeof(sbyte?))
                obj = *(sbyte*)&val;
            else
                obj = val;
        }

        /**
         * <summary>Read byte field.</summary>
         */
        private static object ReadByteField(MemoryStream stream, GridClientPortableMarshaller marsh)
        {
            return PU.ReadByte(stream);
        }

        /**
         * <summary>Read short.</summary>
         */
        private static unsafe void ReadShort(GridClientPortableReadContext ctx, Type type, out object obj)
        {
            short val = PU.ReadShort(ctx.Stream);

            if (type == typeof(ushort) || type == typeof(ushort?))
                obj = *(ushort*)&val;
            else
                obj = val;
        }

        /**
         * <summary>Read short field.</summary>
         */
        private static object ReadShortField(MemoryStream stream, GridClientPortableMarshaller marsh)
        {
            return PU.ReadShort(stream);
        }

        /**
         * <summary>Read char.</summary>
         */
        private static unsafe void ReadChar(GridClientPortableReadContext ctx, Type type, out object obj)
        {
            short val = PU.ReadShort(ctx.Stream);

            obj = *(char*)&val;
        }

        /**
         * <summary>Read char field.</summary>
         */
        private static unsafe object ReadCharField(MemoryStream stream, GridClientPortableMarshaller marsh)
        {
            short val = PU.ReadShort(stream);

            return *(char*)&val;
        }

        /**
         * <summary>Read int.</summary>
         */
        private static unsafe void ReadInt(GridClientPortableReadContext ctx, Type type, out object obj)
        {
            int val = PU.ReadInt(ctx.Stream);

            if (type == typeof(uint) || type == typeof(uint?))
                obj = *(uint*)&val;
            else
                obj = val;
        }

        /**
         * <summary>Read int field.</summary>
         */
        private static object ReadIntField(MemoryStream stream, GridClientPortableMarshaller marsh)
        {
            return PU.ReadInt(stream);
        }

        /**
         * <summary>Read long.</summary>
         */
        private static unsafe void ReadLong(GridClientPortableReadContext ctx, Type type, out object obj)
        {
            long val = PU.ReadLong(ctx.Stream);

            if (type == typeof(ulong) || type == typeof(ulong?))
                obj = *(ulong*)&val;
            else
                obj = val;
        }

        /**
         * <summary>Read long field.</summary>
         */
        private static object ReadLongField(MemoryStream stream, GridClientPortableMarshaller marsh)
        {
            return PU.ReadLong(stream);
        }

        /**
         * <summary>Read float.</summary>
         */
        private static void ReadFloat(GridClientPortableReadContext ctx, Type type, out object obj)
        {
            obj = PU.ReadFloat(ctx.Stream);
        }

        /**
         * <summary>Read float field.</summary>
         */
        private static object ReadFloatField(MemoryStream stream, GridClientPortableMarshaller marsh)
        {
            return PU.ReadFloat(stream);
        }

        /**
         * <summary>Read double.</summary>
         */
        private static void ReadDouble(GridClientPortableReadContext ctx, Type type, out object obj)
        {
            obj = PU.ReadDouble(ctx.Stream);
        }

        /**
         * <summary>Read double field.</summary>
         */
        private static object ReadDoubleField(MemoryStream stream, GridClientPortableMarshaller marsh)
        {
            return PU.ReadDouble(stream);
        }

        /**
         * <summary>Read date.</summary>
         */
        private static void ReadDate(GridClientPortableReadContext ctx, Type type, out object obj)
        {
            obj = PU.ReadDate(ctx.Stream);
        }

        /**
         * <summary>Read date field.</summary>
         */
        private static object ReadDateField(MemoryStream stream, GridClientPortableMarshaller marsh)
        {
            return PU.ReadDate(stream);
        }

        /**
         * <summary>Read string.</summary>
         */
        private static void ReadString(GridClientPortableReadContext ctx, Type type, out object obj)
        {
            obj = PU.ReadString(ctx.Stream);
        }

        /**
         * <summary>Read date field.</summary>
         */
        private static object ReadStringField(MemoryStream stream, GridClientPortableMarshaller marsh)
        {
            return PU.ReadString(stream);
        }

        /**
         * <summary>Read Guid.</summary>
         */
        private static void ReadGuid(GridClientPortableReadContext ctx, Type type, out object obj)
        {
            obj = PU.ReadGuid(ctx.Stream);
        }

        /**
         * <summary>Read Guid field.</summary>
         */
        private static object ReadGuidField(MemoryStream stream, GridClientPortableMarshaller marsh)
        {
            return PU.ReadGuid(stream);
        }

        /**
         * <summary>Read bool array.</summary>
         */
        private static void ReadBoolArray(GridClientPortableReadContext ctx, Type type, out object obj)
        {
            obj = PU.ReadBooleanArray(ctx.Stream);
        }

        /**
         * <summary>Read bool array field.</summary>
         */
        private static object ReadBoolArrayField(MemoryStream stream, GridClientPortableMarshaller marsh)
        {
            return PU.ReadBooleanArray(stream);
        }

        /**
         * <summary>Read byte array.</summary>
         */
        private static void ReadByteArray(GridClientPortableReadContext ctx, Type type, out object obj)
        {
            obj = PU.ReadByteArray(ctx.Stream, type == typeof(sbyte[]));
        }

        /**
         * <summary>Read byte array field.</summary>
         */
        private static object ReadByteArrayField(MemoryStream stream, GridClientPortableMarshaller marsh)
        {
            return PU.ReadByteArray(stream, false);
        }

        /**
         * <summary>Read short array.</summary>
         */
        private static void ReadShortArray(GridClientPortableReadContext ctx, Type type, out object obj)
        {
            obj = PU.ReadShortArray(ctx.Stream, type == typeof(short[]));
        }

        /**
         * <summary>Read short array field.</summary>
         */
        private static object ReadShortArrayField(MemoryStream stream, GridClientPortableMarshaller marsh)
        {
            return PU.ReadShortArray(stream, true);
        }

        /**
         * <summary>Read char array.</summary>
         */
        private static void ReadCharArray(GridClientPortableReadContext ctx, Type type, out object obj)
        {
            obj = PU.ReadCharArray(ctx.Stream);
        }

        /**
         * <summary>Read char array field.</summary>
         */
        private static object ReadCharArrayField(MemoryStream stream, GridClientPortableMarshaller marsh)
        {
            return PU.ReadCharArray(stream);
        }

        /**
         * <summary>Read int array.</summary>
         */
        private static void ReadIntArray(GridClientPortableReadContext ctx, Type type, out object obj)
        {
            obj = PU.ReadIntArray(ctx.Stream, type == typeof(int[]));
        }

        /**
         * <summary>Read int array field.</summary>
         */
        private static object ReadIntArrayField(MemoryStream stream, GridClientPortableMarshaller marsh)
        {
            return PU.ReadIntArray(stream, true);
        }

        /**
         * <summary>Read long array.</summary>
         */
        private static void ReadLongArray(GridClientPortableReadContext ctx, Type type, out object obj)
        {
            obj = PU.ReadLongArray(ctx.Stream, type == typeof(long[]));
        }

        /**
         * <summary>Read long array field.</summary>
         */
        private static object ReadLongArrayField(MemoryStream stream, GridClientPortableMarshaller marsh)
        {
            return PU.ReadLongArray(stream, true);
        }

        /**
         * <summary>Read float array.</summary>
         */
        private static void ReadFloatArray(GridClientPortableReadContext ctx, Type type, out object obj)
        {
            obj = PU.ReadFloatArray(ctx.Stream);
        }

        /**
         * <summary>Read float array field.</summary>
         */
        private static object ReadFloatArrayField(MemoryStream stream, GridClientPortableMarshaller marsh)
        {
            return PU.ReadFloatArray(stream);
        }

        /**
         * <summary>Read double array.</summary>
         */
        private static void ReadDoubleArray(GridClientPortableReadContext ctx, Type type, out object obj)
        {
            obj = PU.ReadDoubleArray(ctx.Stream);
        }

        /**
         * <summary>Read double array field.</summary>
         */
        private static object ReadDoubleArrayField(MemoryStream stream, GridClientPortableMarshaller marsh)
        {
            return PU.ReadDoubleArray(stream);
        }

        /**
         * <summary>Read date array.</summary>
         */
        private static void ReadDateArray(GridClientPortableReadContext ctx, Type type, out object obj)
        {
            obj = PU.ReadDateArray(ctx.Stream);
        }

        /**
         * <summary>Read date array field.</summary>
         */
        private static object ReadDateArrayField(MemoryStream stream, GridClientPortableMarshaller marsh)
        {
            return PU.ReadDateArray(stream);
        }

        /**
         * <summary>Read string array.</summary>
         */
        private static void ReadStringArray(GridClientPortableReadContext ctx, Type type, out object obj)
        {
            obj = PU.ReadStringArray(ctx.Stream);
        }

        /**
         * <summary>Read string array field.</summary>
         */
        private static object ReadStringArrayField(MemoryStream stream, GridClientPortableMarshaller marsh)
        {
            return PU.ReadStringArray(stream);
        }

        /**
         * <summary>Read GUID array.</summary>
         */
        private static  void ReadGuidArray(GridClientPortableReadContext ctx, Type type, out object obj)
        {
            obj = PU.ReadGuidArray(ctx.Stream);
        }

        /**
         * <summary>Read GUID array field.</summary>
         */
        private static object ReadGuidArrayField(MemoryStream stream, GridClientPortableMarshaller marsh)
        {
            return PU.ReadGuidArray(stream);
        }

        /**
         * <summary>Read array.</summary>
         */
        private static void ReadArray(GridClientPortableReadContext ctx, Type type, out object obj)
        {
            obj = PU.ReadArray(ctx);
        }

        /**
         * <summary>Read array field.</summary>
         */
        private static object ReadArrayField(MemoryStream stream, GridClientPortableMarshaller marsh)
        {
            return PU.ReadArrayPortable(stream, marsh);
        }

        /**
         * <summary>Read collection.</summary>
         */
        private static void ReadCollection(GridClientPortableReadContext ctx, Type type, out object obj)
        {
            GridClientPortableCollectionInfo info = GridClientPortableCollectionInfo.Info(type);

            if (info.IsGenericCollection)
                obj = info.GenericReadMethod.Invoke(null, new object[] { ctx, null });
            else
                obj = PU.ReadCollection(ctx, CreateArrayList, AddToArrayList);
        }

        /**
         * <summary>Read collection field.</summary>
         */
        private static object ReadCollectionField(MemoryStream stream, GridClientPortableMarshaller marsh)
        {
            return PU.ReadCollectionPortable(stream, marsh);
        }

        /**
         * <summary>Read dictionary.</summary>
         */
        private static void ReadDictionary(GridClientPortableReadContext ctx, Type type, out object obj)
        {
            GridClientPortableCollectionInfo info = GridClientPortableCollectionInfo.Info(type);

            if (info.IsGenericDictionary)
                obj = info.GenericReadMethod.Invoke(null, new object[] { ctx, null });
            else
                obj = PU.ReadDictionary(ctx, CreateHashtable);
        }

        /**
         * <summary>Read collection field.</summary>
         */
        private static object ReadDictionaryField(MemoryStream stream, GridClientPortableMarshaller marsh)
        {
            return PU.ReadDictionaryPortable(stream, marsh);
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
