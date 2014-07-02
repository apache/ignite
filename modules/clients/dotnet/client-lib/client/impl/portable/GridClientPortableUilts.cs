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
    using System.Text;
    using GridGain.Client.Portable;

    using PSH = GridGain.Client.Impl.Portable.GridClientPortableSystemHandlers;

    /**
     * <summary>Utilities for portable serialization.</summary>
     */ 
    static class GridClientPortableUilts
    {
        /** Header of NULL object. */
        public const byte HDR_NULL = 101;

        /** Header of object handle. */
        public const byte HDR_HND = 102;

        /** Header of object in fully serialized form. */
        public const byte HDR_FULL = 103;

        /** Header of object in fully serailized form with metadata. */
        public const byte HDR_META = 104;

        /** Type: unsigned byte. */
        public const byte TYPE_BYTE = 1;
        
        /** Type: short. */
        public const byte TYPE_SHORT = 2;

        /** Type: int. */
        public const byte TYPE_INT = 3;

        /** Type: long. */
        public const byte TYPE_LONG = 4;

        /** Type: float. */
        public const byte TYPE_FLOAT = 5;

        /** Type: double. */
        public const byte TYPE_DOUBLE = 6;

        /** Type: char. */
        public const byte TYPE_CHAR = 7;

        /** Type: boolean. */
        public const byte TYPE_BOOL = 8;

        /** Type: string. */
        public const byte TYPE_STRING = 9;

        /** Type: GUID. */
        public const byte TYPE_GUID = 10;

        /** Type: date. */
        public const byte TYPE_DATE = 11;

        /** Type: unsigned byte array. */
        public const byte TYPE_ARRAY_BYTE = 12;

        /** Type: short array. */
        public const byte TYPE_ARRAY_SHORT = 13;

        /** Type: int array. */
        public const byte TYPE_ARRAY_INT = 14;

        /** Type: long array. */
        public const byte TYPE_ARRAY_LONG = 15;

        /** Type: float array. */
        public const byte TYPE_ARRAY_FLOAT = 16;

        /** Type: double array. */
        public const byte TYPE_ARRAY_DOUBLE = 17;

        /** Type: char array. */
        public const byte TYPE_ARRAY_CHAR = 18;

        /** Type: boolean array. */
        public const byte TYPE_ARRAY_BOOL = 19;

        /** Type: string array. */
        public const byte TYPE_ARRAY_STRING = 20;

        /** Type: GUID array. */
        public const byte TYPE_ARRAY_GUID = 21;

        /** Type: date array. */
        public const byte TYPE_ARRAY_DATE = 22;

        /** Type: object array. */
        public const byte TYPE_ARRAY = 23;

        /** Type: collection. */
        public const byte TYPE_COLLECTION = 24; 

        /** Type: map. */
        public const byte TYPE_DICTIONARY = 25;

        /** Type: map entry. */
        public const byte TYPE_MAP_ENTRY = 26;

        /** Type: portable object. */
        public const byte TYPE_PORTABLE = 27;

        /** Type: authentication request. */
        public const byte TYPE_AUTH_REQ = 51;

        /** Type: topology request. */
        public const byte TYPE_TOP_REQ = 52;

        /** Type: task request. */
        public const byte TYPE_TASK_REQ = 53;

        /** Type: cache request. */
        public const byte TYPE_CACHE_REQ = 54;
        
        /** Type: log request. */
        public const byte TYPE_LOG_REQ = 55;

        /** Type: response. */
        public const byte TYPE_RESP = 56;

        /** Type: node bean. */
        public const byte TYPE_NODE_BEAN = 57;

        /** Type: node metrics bean. */
        public const byte TYPE_NODE_METRICS_BEAN = 58;

        /** Type: task result bean. */
        public const byte TYPE_TASK_RES_BEAN = 59;

        /** Collection: custom. */
        public const byte COLLECTION_CUSTOM = 0;

        /** Collection: array list. */
        public const byte COLLECTION_ARRAY_LIST = 1;

        /** Collection: linked list. */
        public const byte COLLECTION_LINKED_LIST = 2;

        /** Collection: hash set. */
        public const byte COLLECTION_HASH_SET = 3;

        /** Collection: hash set. */
        public const byte COLLECTION_LINKED_HASH_SET = 4;

        /** Collection: sorted set. */
        public const byte COLLECTION_SORTED_SET = 5;

        /** Collection: concurrent bag. */
        public const byte COLLECTION_CONCURRENT_BAG = 6;

        /** Map: custom. */
        public const byte MAP_CUSTOM = 0;

        /** Map: hash map. */
        public const byte MAP_HASH_MAP = 1;

        /** Map: linked hash map. */
        public const byte MAP_LINKED_HASH_MAP = 2;

        /** Map: sorted map. */
        public const byte MAP_SORTED_MAP = 3;

        /** Map: concurrent hash map. */
        public const byte MAP_CONCURRENT_HASH_MAP = 4;

        /** Byte "0". */
        public const byte BYTE_ZERO = (byte)0;

        /** Byte "1". */
        public const byte BYTE_ONE = (byte)1;
        
        /** Collection type. */
        public static readonly Type TYP_COLLECTION = typeof(ICollection);

        /** Dictionary type. */
        public static readonly Type TYP_DICTIONARY = typeof(IDictionary);

        /** Generic collection type. */
        public static readonly Type TYP_GENERIC_COLLECTION = typeof(ICollection<>);

        /** Generic dictionary type. */
        public static readonly Type TYP_GENERIC_DICTIONARY = typeof(IDictionary<,>);

        /** Ticks for Java epoch. */
        private static readonly long JAVA_DATE_TICKS = new DateTime(1970, 1, 1, 0, 0, 0, 0).Ticks;

        /** java date multiplier. */
        private const long JAVA_DATE_MULTIPLIER = 10000;
        
        /** Bindig flags for instance search. */
        private static BindingFlags BIND_FLAGS_INSTANCE = BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic;

        /** Bindig flags for static search. */
        private static BindingFlags BIND_FLAGS_STATIC = BindingFlags.Static | BindingFlags.Public | BindingFlags.NonPublic;

        /** Filed: MemoryStream byte buffer (array). */
        private static FieldInfo FIELD_MEM_BUF = typeof(MemoryStream).GetField("_buffer", BIND_FLAGS_INSTANCE);

        /** Method: WriteGenericCollection. */
        public static MethodInfo MTDH_WRITE_GENERIC_COLLECTION =
            typeof(GridClientPortableUilts).GetMethod("WriteGenericCollection", BIND_FLAGS_STATIC);

        /** Method: WriteTypedGenericCollection. */
        public static MethodInfo MTDH_WRITE_TYPED_GENERIC_COLLECTION =
            typeof(GridClientPortableUilts).GetMethod("WriteTypedGenericCollection", BIND_FLAGS_STATIC);

        /** Method: ReadGenericCollection. */
        public static MethodInfo MTDH_READ_GENERIC_COLLECTION =
            typeof(GridClientPortableUilts).GetMethod("ReadGenericCollection", BIND_FLAGS_STATIC);

        /** Method: WriteGenericDictionary. */
        public static MethodInfo MTDH_WRITE_GENERIC_DICTIONARY = 
            typeof(GridClientPortableUilts).GetMethod("WriteGenericDictionary", BIND_FLAGS_STATIC);

        /** Method: WriteTypedGenericDictionary. */
        public static MethodInfo MTDH_WRITE_TYPED_GENERIC_DICTIONARY = 
            typeof(GridClientPortableUilts).GetMethod("WriteTypedGenericDictionary", BIND_FLAGS_STATIC);

        /** Method: ReadGenericDictionary. */
        public static MethodInfo MTDH_READ_GENERIC_DICTIONARY =
            typeof(GridClientPortableUilts).GetMethod("ReadGenericDictionary", BIND_FLAGS_STATIC);

        /**
         * <summary>Write boolean.</summary>
         * <param name="val">Value.</param>
         * <param name="stream">Output stream.</param>
         */
        public static void WriteBoolean(bool val, Stream stream)
        {
            stream.WriteByte(val ? BYTE_ONE : BYTE_ZERO);
        }

        /**
         * <summary>Read boolean.</summary>
         * <param name="stream">Output stream.</param>
         * <returns>Value.</returns>
         */
        public static bool ReadBoolean(Stream stream)
        {
            return stream.ReadByte() == BYTE_ONE;
        }

        /**
         * <summary>Write boolean array.</summary>
         * <param name="vals">Value.</param>
         * <param name="stream">Output stream.</param>
         */
        public static void WriteBooleanArray(bool[] vals, Stream stream)
        {            
            WriteInt(vals.Length, stream);

            for (int i = 0; i < vals.Length; i++)
                WriteBoolean(vals[i], stream);
        }

        /**
         * <summary>Read boolean array.</summary>
         * <param name="stream">Output stream.</param>
         * <returns>Value.</returns>
         */
        public static bool[] ReadBooleanArray(Stream stream)
        {
            int len = ReadInt(stream);
            
            bool[] vals = new bool[len];

            for (int i = 0; i < vals.Length; i++)
                vals[i] = ReadBoolean(stream);

            return vals;
        }

        /**
         * <summary>Write byte.</summary>
         * <param name="val">Value.</param>
         * <param name="stream">Output stream.</param>
         */
        public static void WriteByte(byte val, Stream stream)
        {
            stream.WriteByte(val);
        }

        /**
         * <summary>Read byte.</summary>
         * <param name="stream">Output stream.</param>
         * <returns>Value.</returns>
         */
        public static byte ReadByte(Stream stream)
        {
            return (byte)stream.ReadByte();
        }

        /**
         * <summary>Write byte array.</summary>
         * <param name="vals">Value.</param>
         * <param name="stream">Output stream.</param>
         * <returns>Length of written data.</returns>
         */
        public static void WriteByteArray(byte[] vals, Stream stream)
        {            
            WriteInt(vals.Length, stream);

            stream.Write(vals, 0, vals.Length);
        }

        /**
         * <summary>Read byte array.</summary>
         * <param name="stream">Output stream.</param>
         * <param name="signed">Signed flag.</param>
         * <returns>Value.</returns>
         */
        public static unsafe object ReadByteArray(Stream stream, bool signed)
        {
            int len = ReadInt(stream);
                        
            if (signed)
            {
                sbyte[] vals = new sbyte[len];

                for (int i = 0; i < len; i++)
                {
                    byte val = (byte)stream.ReadByte();

                    vals[i] = *(sbyte*)&val;
                }

                return vals;
            }
            else
            {
                byte[] vals = new byte[len];

                stream.Read(vals, 0, len);

                return vals;
            }
        }

        /**
         * <summary>Write short value.</summary>
         * <param name="val">Value.</param>
         * <param name="stream">Output stream.</param>
         */
        public static void WriteShort(short val, Stream stream)
        {
            stream.WriteByte((byte)(val & 0xFF));
            stream.WriteByte((byte)(val >> 8 & 0xFF));
        }

        /**
         * <summary>Read short value.</summary>
         * <param name="stream">Output stream.</param>
         * <returns>Value.</returns>
         */
        public static short ReadShort(Stream stream)
        {
            short val = (short)stream.ReadByte();
            val |= (short)(stream.ReadByte() << 8);

            return val;
        }

        /**
         * <summary>Write short array.</summary>
         * <param name="vals">Value.</param>
         * <param name="stream">Output stream.</param>
         */
        public static void WriteShortArray(short[] vals, Stream stream)
        {            
            WriteInt(vals.Length, stream);

            for (int i = 0; i < vals.Length; i++)
                WriteShort(vals[i], stream);
        }

        /**
         * <summary>Read short array.</summary>
         * <param name="stream">Stream.</param>
         * <param name="signed">Signed flag.</param>
         * <returns>Value.</returns>
         */
        public static unsafe object ReadShortArray(Stream stream, bool signed)
        {
            int len = ReadInt(stream);
                        
            if (signed)
            {
                short[] vals = new short[len];

                for (int i = 0; i < len; i++)
                    vals[i] = ReadShort(stream);

                return vals;
            }
            else
            {
                ushort[] vals = new ushort[len];

                for (int i = 0; i < len; i++)
                {
                    short val = ReadShort(stream);

                    vals[i] = *(ushort*)&val;
                }

                return vals;
            }
        }

        /**
         * <summary>Write int value.</summary>
         * <param name="val">Value.</param>
         * <param name="stream">Output stream.</param>
         */
        public static void WriteInt(int val, Stream stream)
        {            
            stream.WriteByte((byte)(val & 0xFF));
            stream.WriteByte((byte)(val >> 8 & 0xFF));
            stream.WriteByte((byte)(val >> 16 & 0xFF));
            stream.WriteByte((byte)(val >> 24 & 0xFF));
        }

        /**
         * <summary>Read int value.</summary>
         * <param name="stream">Output stream.</param>
         * <returns>Value.</returns>
         */
        public static int ReadInt(Stream stream)
        {
            int val = stream.ReadByte();
            val |= stream.ReadByte() << 8;
            val |= stream.ReadByte() << 16;
            val |= stream.ReadByte() << 24;

            return val;
        }

        /**
         * <summary>Write int array.</summary>
         * <param name="vals">Value.</param>
         * <param name="stream">Output stream.</param>
         */
        public static void WriteIntArray(int[] vals, Stream stream)
        {
            WriteInt(vals.Length, stream);

            for (int i = 0; i < vals.Length; i++)
                WriteInt(vals[i], stream);
        }

        /**
         * <summary>Read int array.</summary>
         * <param name="stream">Stream.</param>
         * <param name="signed">Signed flag.</param>
         * <returns>Value.</returns>
         */
        public static unsafe object ReadIntArray(Stream stream, bool signed)
        {
            int len = ReadInt(stream);
                        
            if (signed)
            {
                int[] vals = new int[len];

                for (int i = 0; i < len; i++)
                    vals[i] = ReadInt(stream);

                return vals;
            }
            else
            {
                uint[] vals = new uint[len];

                for (int i = 0; i < len; i++)
                {
                    int val = ReadInt(stream);

                    vals[i] = *(uint*)&val;
                }

                return vals;
            } 
        }

        /**
         * <summary>Write long value.</summary>
         * <param name="val">Value.</param>
         * <param name="stream">Output stream.</param>
         */
        public static void WriteLong(long val, Stream stream)
        {            
            stream.WriteByte((byte)(val & 0xFF));
            stream.WriteByte((byte)(val >> 8 & 0xFF));
            stream.WriteByte((byte)(val >> 16 & 0xFF));
            stream.WriteByte((byte)(val >> 24 & 0xFF));
            stream.WriteByte((byte)(val >> 32 & 0xFF));
            stream.WriteByte((byte)(val >> 40 & 0xFF));
            stream.WriteByte((byte)(val >> 48 & 0xFF));
            stream.WriteByte((byte)(val >> 56 & 0xFF));            
        }

        /**
         * <summary>Read long value.</summary>
         * <param name="stream">Output stream.</param>
         * <returns>Value.</returns>
         */
        public static long ReadLong(Stream stream)
        {
            long val = (long)(stream.ReadByte()) << 0;
            val |= (long)(stream.ReadByte()) << 8;
            val |= (long)(stream.ReadByte()) << 16;
            val |= (long)(stream.ReadByte()) << 24;
            val |= (long)(stream.ReadByte()) << 32;
            val |= (long)(stream.ReadByte()) << 40;
            val |= (long)(stream.ReadByte()) << 48;
            val |= (long)(stream.ReadByte()) << 56; 

            return val;
        }

        /**
         * <summary>Write long array.</summary>
         * <param name="vals">Value.</param>
         * <param name="stream">Output stream.</param>
         */
        public static void WriteLongArray(long[] vals, Stream stream)
        {            
            WriteInt(vals.Length, stream);

            for (int i = 0; i < vals.Length; i++)
                WriteLong(vals[i], stream);
        }

        /**
         * <summary>Read long array.</summary>
         * <param name="stream">Stream.</param>
         * <param name="signed">Signed flag.</param>
         * <returns>Value.</returns>
         */
        public static unsafe object ReadLongArray(Stream stream, bool signed)
        {
            int len = ReadInt(stream);
                        
            if (signed)
            {
                long[] vals = new long[len];

                for (int i = 0; i < len; i++)
                    vals[i] = ReadLong(stream);

                return vals;
            }
            else
            {
                ulong[] vals = new ulong[len];

                for (int i = 0; i < len; i++)
                {
                    long val = ReadLong(stream);

                    vals[i] = *(ulong*)&val;
                }

                return vals;
            }
        }

        /**
         * <summary>Write char array.</summary>
         * <param name="vals">Value.</param>
         * <param name="stream">Output stream.</param>
         */
        public static void WriteCharArray(char[] vals, Stream stream)
        {            
            WriteInt(vals.Length, stream);

            for (int i = 0; i < vals.Length; i++)
                WriteShort((short)vals[i], stream);
        }

        /**
         * <summary>Read char array.</summary>
         * <param name="stream">Stream.</param>
         * <returns>Value.</returns>
         */
        public static char[] ReadCharArray(Stream stream)
        {
            int len = ReadInt(stream);
                        
            char[] vals = new char[len];

            for (int i = 0; i < len; i++)
                vals[i] = (char)ReadShort(stream);

            return vals;
        }

        /**
         * <summary>Write float value.</summary>
         * <param name="val">Value.</param>
         * <param name="stream">Output stream.</param>
         */
        public static unsafe void WriteFloat(float val, Stream stream)
        {
            WriteInt(*(int*)&val, stream);
        }

        /**
         * <summary>Read float value.</summary>
         * <param name="stream">Output stream.</param>
         * <returns>Value.</returns>
         */
        public static unsafe float ReadFloat(Stream stream)
        {
            int val = ReadInt(stream);

            return *(float*)&val;
        }

        /**
         * <summary>Write float array.</summary>
         * <param name="vals">Value.</param>
         * <param name="stream">Output stream.</param>
         */
        public static void WriteFloatArray(float[] vals, Stream stream)
        {            
            WriteInt(vals.Length, stream);

            for (int i = 0; i < vals.Length; i++)
                WriteFloat((float)vals[i], stream);
        }

        /**
         * <summary>Read float array.</summary>
         * <param name="stream">Stream.</param>
         * <returns>Value.</returns>
         */
        public static float[] ReadFloatArray(Stream stream)
        {
            int len = ReadInt(stream);
                        
            float[] vals = new float[len];

            for (int i = 0; i < len; i++)
                vals[i] = ReadFloat(stream);

            return vals;
        }

        /**
         * <summary>Write double value.</summary>
         * <param name="val">Value.</param>
         * <param name="stream">Output stream.</param>
         */
        public static unsafe void WriteDouble(double val, Stream stream)
        {
            WriteLong(*(long*)&val, stream);
        }

        /**
         * <summary>Read double value.</summary>
         * <param name="stream">Output stream.</param>
         * <returns>Value.</returns>
         */
        public static unsafe double ReadDouble(Stream stream)
        {
            long val = ReadLong(stream);

            return *(double*)&val;
        }

        /**
         * <summary>Write double array.</summary>
         * <param name="vals">Value.</param>
         * <param name="stream">Output stream.</param>
         */
        public static void WriteDoubleArray(double[] vals, Stream stream)
        {            
            WriteInt(vals.Length, stream);

            for (int i = 0; i < vals.Length; i++)
                WriteDouble((double)vals[i], stream);
        }

        /**
         * <summary>Read double array.</summary>
         * <param name="stream">Stream.</param>
         * <returns>Value.</returns>
         */ 
        public static double[] ReadDoubleArray(Stream stream)
        {
            int len = ReadInt(stream);
                        
            double[] vals = new double[len];

            for (int i = 0; i < len; i++)
                vals[i] = ReadDouble(stream);

            return vals;
        }

        /**
         * <summary>Write date.</summary>
         * <param name="val">Date.</param>
         * <param name="stream">Stream.</param>
         */
        public static void WriteDate(DateTime? val, Stream stream)
        {
            long high;
            short low;

            ToJavaDate(val.Value, out high, out low);

            WriteLong(high, stream);
            WriteShort(low, stream);               
        }

        /**
         * <summary>Read date.</summary>
         * <param name="stream">Stream.</param>
         * <returns>Date</returns>
         */
        public static DateTime? ReadDate(Stream stream)
        {
            long high = ReadLong(stream);
            short low = ReadShort(stream);

            return ToDotNetDate(high, low);
        }

        /**
         * <summary>Write date array.</summary>
         * <param name="vals">Date array.</param>
         * <param name="stream">Stream.</param>
         */
        public static void WriteDateArray(DateTime?[] vals, Stream stream)
        {            
            WriteInt(vals.Length, stream);

            for (int i = 0; i < vals.Length; i++)
                WriteDate(vals[i], stream);
        }

        /**
         * <summary>Read date array.</summary>
         * <param name="stream">Stream.</param>
         * <returns>Date array.</returns>
         */
        public static DateTime?[] ReadDateArray(Stream stream)
        {
            int len = ReadInt(stream);
                        
            DateTime?[] vals = new DateTime?[len];

            for (int i = 0; i < len; i++)
                vals[i] = ReadDate(stream);

            return vals;
        }

        /**
         * <summary>Write string in UTF8 encoding.</summary>
         * <param name="val">String.</param>
         * <param name="stream">Stream.</param>
         */
        public static void WriteString(string val, Stream stream)
        {
            WriteByteArray(val != null ? Encoding.UTF8.GetBytes(val) : null, stream);
        }

        /**
         * <summary>Read string in UTF8 encoding.</summary>
         * <param name="stream">Stream.</param>
         * <returns>String.</returns>
         */
        public static string ReadString(Stream stream)
        {
            byte[] bytes = (byte[])ReadByteArray(stream, false);

            return bytes != null ? Encoding.UTF8.GetString(bytes) : null;
        }

        /**
         * <summary>Write string array in UTF8 encoding.</summary>
         * <param name="vals">String array.</param>
         * <param name="stream">Stream.</param>
         */
        public static void WriteStringArray(string[] vals, Stream stream)
        {            
            WriteInt(vals.Length, stream);

            for (int i = 0; i < vals.Length; i++)
                WriteString(vals[i], stream);
        }

        /**
         * <summary>Read string array in UTF8 encoding.</summary>
         * <param name="stream">Stream.</param>
         * <returns>String array.</returns>
         */
        public static string[] ReadStringArray(Stream stream)
        {
            int len = ReadInt(stream);
                        
            string[] vals = new string[len];

            for (int i = 0; i < len; i++)
                vals[i] = ReadString(stream);

            return vals;
        }

        /**
         * <summary>Write GUID.</summary>
         * <param name="val">GUID.</param>
         * <param name="stream">Stream.</param>
         */
        public static void WriteGuid(Guid? val, Stream stream)
        {            
            byte[] bytes = val.Value.ToByteArray();

            // .Net returns bytes in the following order: _a(4), _b(2), _c(2), _d, _e, _g, _h, _i, _j, _k.
            // And _a, _b and _c are always in little endian format irrespective of system configuration.
            // To be compliant with Java we rearrange them as follows: _c, _b_, a_, _k, _j, _i, _h, _g, _e, _d.
            stream.Write(bytes, 6, 2);   // _c
            stream.Write(bytes, 4, 2);   // _b
            stream.Write(bytes, 0, 4);   // _a

            stream.WriteByte(bytes[15]); // _k
            stream.WriteByte(bytes[14]); // _j
            stream.WriteByte(bytes[13]); // _i
            stream.WriteByte(bytes[12]); // _h

            stream.WriteByte(bytes[11]); // _g
            stream.WriteByte(bytes[10]); // _f
            stream.WriteByte(bytes[9]);  // _e
            stream.WriteByte(bytes[8]);  // _d
        }

        /**
         * <summary>Read GUID.</summary>
         * <param name="stream">Stream.</param>
         * <returns>GUID</returns>
         */
        public static Guid? ReadGuid(Stream stream)
        {            
            byte[] bytes = new byte[16];

            // Perform conversion opposite to what write does.
            bytes[6] = (byte)stream.ReadByte();  // _c
            bytes[7] = (byte)stream.ReadByte();

            bytes[4] = (byte)stream.ReadByte();  // _b
            bytes[5] = (byte)stream.ReadByte();

            bytes[0] = (byte)stream.ReadByte();  // _a
            bytes[1] = (byte)stream.ReadByte();
            bytes[2] = (byte)stream.ReadByte();
            bytes[3] = (byte)stream.ReadByte();

            bytes[15] = (byte)stream.ReadByte();  // _k
            bytes[14] = (byte)stream.ReadByte();  // _j
            bytes[13] = (byte)stream.ReadByte();  // _i
            bytes[12] = (byte)stream.ReadByte();  // _h

            bytes[11] = (byte)stream.ReadByte();  // _g
            bytes[10] = (byte)stream.ReadByte();  // _f
            bytes[9] = (byte)stream.ReadByte();   // _e
            bytes[8] = (byte)stream.ReadByte();   // _d

            return new Guid(bytes);
        }

        /**
         * <summary>Write GUID array.</summary>
         * <param name="vals">GUID array.</param>
         * <param name="stream">Stream.</param>
         */
        public static void WriteGuidArray(Guid?[] vals, Stream stream)
        {            
            WriteInt(vals.Length, stream);

            for (int i = 0; i < vals.Length; i++)
                WriteGuid(vals[i], stream);
        }

        /**
         * <summary>Read GUID array.</summary>
         * <param name="stream">Stream.</param>
         * <returns>GUID array.</returns>
         */
        public static Guid?[] ReadGuidArray(Stream stream)
        {
            int len = ReadInt(stream);
                        
            Guid?[] vals = new Guid?[len];

            for (int i = 0; i < len; i++)
                vals[i] = ReadGuid(stream);

            return vals;
        }

        /**
         * <summary>Write array.</summary>
         * <param name="val">Array.</param>
         * <param name="ctx">Write context.</param>
         */
        public static void WriteArray(Array val, GridClientPortableWriteContext ctx)
        {
            Stream stream = ctx.Stream;

            WriteInt(val.Length, stream);

            for (int i = 0; i < val.Length; i++)
                ctx.Write(val.GetValue(i));
        }

        /**
         * <summary>Read array.</summary>
         * <param name="ctx">Read context.</param>
         * <returns>Array.</returns>
         */
        public static Array ReadArray<T>(GridClientPortableReadContext ctx)
        {
            MemoryStream stream = ctx.Stream;

            int len = ReadInt(stream);

            Array vals = Array.CreateInstance(typeof(T), len);

            for (int i = 0; i < len; i++)
                vals.SetValue(ctx.Deserialize<T>(stream), i);

            return vals;
        }

        /**
         * <summary>Read array in portable form.</summary>
         * <param name="stream">Stream.</param>
         * <param name="marsh">Marshaller.</param>
         * <returns>Array.</returns>
         */
        public static T[] ReadArrayPortable<T>(MemoryStream stream, 
            GridClientPortableMarshaller marsh)
        {
            int len = ReadInt(stream);
                        
            T[] vals = new T[len];

            for (int i = 0; i < len; i++)
            {
                IGridClientPortableObject portObj = ReadPortable(stream, marsh, false);

                vals.SetValue(PortableOrPredefined<T>(portObj), i);
            }

            return vals;
        }

        /**
         * <summary>Write collection.</summary>
         * <param name="val">Value.</param>
         * <param name="ctx">Write context.</param>
         */
        public static void WriteCollection(ICollection val, GridClientPortableWriteContext ctx) 
        {            
            byte colType = val.GetType() == typeof(ArrayList) ? COLLECTION_ARRAY_LIST : COLLECTION_CUSTOM;

            WriteTypedCollection(val, ctx, colType);
        }

        /**
         * <summary>Write non-null collection with known type.</summary>
         * <param name="val">Value.</param>
         * <param name="ctx">Write context.</param>
         * <param name="colType">Collection type.</param>
         */
        public static void WriteTypedCollection(ICollection val, GridClientPortableWriteContext ctx, byte colType)
        {
            WriteInt(val.Count, ctx.Stream);

            WriteByte(colType, ctx.Stream);

            foreach (object elem in val)
                ctx.Write(elem);
        }

        /**
         * <summary>Read collection.</summary>
         * <param name="ctx">Context.</param>
         * <param name="factory">Factory delegate.</param>
         * <param name="adder">Adder delegate.</param>
         * <returns>Collection.</returns>
         */
        public static ICollection ReadCollection(GridClientPortableReadContext ctx, 
            GridClientPortableCollectionFactory factory, GridClientPortableCollectionAdder adder)
        {
            if (factory == null)
                factory = PSH.CreateArrayList;

            if (adder == null)
                adder = PSH.AddToArrayList;

            MemoryStream stream = ctx.Stream;

            int len = ReadInt(stream);
                        
            ctx.Stream.Seek(1, SeekOrigin.Current);

            ICollection res = factory.Invoke(len);

            for (int i = 0; i < len; i++)
                adder.Invoke(res, ctx.Deserialize<object>(ctx.Stream));

            return res;
        }

        /**
         * <summary>Read collection in portable form.</summary>
         * <param name="stream">Stream.</param>
         * <param name="marsh">Marshaller.</param>
         * <returns>Collection.</returns>
         */
        public static ICollection<T> ReadCollectionPortable<T>(MemoryStream stream, 
            GridClientPortableMarshaller marsh)
        {
            int len = ReadInt(stream);
                        
            stream.Seek(1, SeekOrigin.Current); // Skip collection type.

            ICollection<T> res = new List<T>(len);

            for (int i = 0; i < len; i++)
            {
                IGridClientPortableObject portObj = ReadPortable(stream, marsh, false);

                res.Add(PortableOrPredefined<T>(portObj));
            }

            return res;
        }

        /**
         * <summary>Write generic collection.</summary>
         * <param name="val">Value.</param>
         * <param name="ctx">Write context.</param>
         */
        public static void WriteGenericCollection<T>(ICollection<T> val, GridClientPortableWriteContext ctx)
        {            
            Type type = val.GetType().GetGenericTypeDefinition();

            byte colType;

            if (type == typeof(List<>))
                colType = COLLECTION_ARRAY_LIST;
            else if (type == typeof(LinkedList<>))
                colType = COLLECTION_LINKED_LIST;
            else if (type == typeof(HashSet<>))
                colType = COLLECTION_HASH_SET;
            else if (type == typeof(SortedSet<>))
                colType = COLLECTION_SORTED_SET;
            else
                colType = COLLECTION_CUSTOM;

            WriteTypedGenericCollection(val, ctx, colType);            
        }

        /**
         * <summary>Write generic non-null collection with known type.</summary>
         * <param name="val">Value.</param>
         * <param name="ctx">Write context.</param>
         * <param name="colType">Collection type.</param>
         */
        public static void WriteTypedGenericCollection<T>(ICollection<T> val, GridClientPortableWriteContext ctx, 
            byte colType)
        {
            WriteInt(val.Count, ctx.Stream);

            WriteByte(colType, ctx.Stream);

            foreach (T elem in val)
                ctx.Write(elem);
        }

        /**
         * <summary>Read generic collection.</summary>
         * <param name="ctx">Context.</param>
         * <param name="factory">Factory delegate.</param>
         * <returns>Collection.</returns>
         */
        public static ICollection<T> ReadGenericCollection<T>(GridClientPortableReadContext ctx, 
            GridClientPortableGenericCollectionFactory<T> factory)
        {
            int len = ReadInt(ctx.Stream);

            if (len >= 0)
            {
                byte colType = ReadByte(ctx.Stream);

                if (factory == null)
                {
                    // Need to detect factory automatically.
                    if (colType == COLLECTION_LINKED_LIST)
                        factory = PSH.CreateLinkedList<T>;
                    else if (colType == COLLECTION_HASH_SET)
                        factory = PSH.CreateHashSet<T>;
                    else if (colType == COLLECTION_SORTED_SET)
                        factory = PSH.CreateSortedSet<T>;
                    else
                        factory = PSH.CreateList<T>;
                }

                ICollection<T> res = factory.Invoke(len);

                for (int i = 0; i < len; i++)
                    res.Add(ctx.Deserialize<T>(ctx.Stream));

                return res;
            }
            else
                return null;
        }

        /**
         * <summary>Write dictionary.</summary>
         * <param name="val">Value.</param>
         * <param name="ctx">Write context.</param>
         */
        public static void WriteDictionary(IDictionary val, GridClientPortableWriteContext ctx)
        {            
            byte dictType = val.GetType() == typeof(Hashtable) ? MAP_HASH_MAP : MAP_CUSTOM;

            WriteTypedDictionary(val, ctx, dictType);
        }

        /**
         * <summary>Write non-null dictionary with known type.</summary>
         * <param name="val">Value.</param>
         * <param name="ctx">Write context.</param>
         * <param name="dictType">Dictionary type.</param>
         */
        public static void WriteTypedDictionary(IDictionary val, GridClientPortableWriteContext ctx, byte dictType)
        {
            WriteInt(val.Count, ctx.Stream);

            WriteByte(dictType, ctx.Stream);

            foreach (DictionaryEntry entry in val)
            {
                ctx.Write(entry.Key);
                ctx.Write(entry.Value);
            }
        }

        /**
         * <summary>Read dictionary.</summary>
         * <param name="ctx">Context.</param>
         * <param name="factory">Factory delegate.</param>
         * <returns>Dictionary.</returns>
         */
        public static IDictionary ReadDictionary(GridClientPortableReadContext ctx,
            GridClientPortableDictionaryFactory factory)
        {
            if (factory == null)
                factory = PSH.CreateHashtable;

            MemoryStream stream = ctx.Stream;

            int len = ReadInt(stream);
                        
            ctx.Stream.Seek(1, SeekOrigin.Current);

            IDictionary res = factory.Invoke(len);

            for (int i = 0; i < len; i++)
            {
                object key = ctx.Deserialize<object>(ctx.Stream);
                object val = ctx.Deserialize<object>(ctx.Stream);

                res[key] = val;
            }
                    
            return res;
        }

        /**
         * <summary>Read dictionary in portable form.</summary>
         * <param name="stream">Stream.</param>
         * <param name="marsh">Marshaller.</param>
         * <returns>Dictionary.</returns>
         */
        public static IDictionary<K, V> ReadDictionaryPortable<K, V>(
            MemoryStream stream, GridClientPortableMarshaller marsh)
        {
            int len = ReadInt(stream);
                        
            stream.Seek(1, SeekOrigin.Current); // Skip dictionary type.

            IDictionary<K, V> res = new Dictionary<K, V>(len);

            for (int i = 0; i < len; i++)
            {
                IGridClientPortableObject keyPortObj = ReadPortable(stream, marsh, false);
                IGridClientPortableObject valPortObj = ReadPortable(stream, marsh, false);

                res.Add(PortableOrPredefined<K>(keyPortObj), PortableOrPredefined<V>(valPortObj));
            }
                    

            return res;
        }

        /**
         * <summary>Write generic dictionary.</summary>
         * <param name="val">Value.</param>
         * <param name="ctx">Write context.</param>
         */
        public static void WriteGenericDictionary<K, V>(IDictionary<K, V> val, GridClientPortableWriteContext ctx)
        {            
            Type type = val.GetType().GetGenericTypeDefinition();

            byte dictType;

            if (type == typeof(Dictionary<,>))
                dictType = MAP_HASH_MAP;
            else if (type == typeof(SortedDictionary<,>))
                dictType = MAP_SORTED_MAP;
            else if (type == typeof(ConcurrentDictionary<,>))
                dictType = MAP_CONCURRENT_HASH_MAP;
            else
                dictType = MAP_CUSTOM;

            WriteTypedGenericDictionary<K, V>(val, ctx, dictType);
        }

        /**
         * <summary>Write generic non-null dictionary with known type.</summary>
         * <param name="val">Value.</param>
         * <param name="ctx">Write context.</param>
         * <param name="dictType">Dictionary type.</param>
         */
        public static void WriteTypedGenericDictionary<K, V>(IDictionary<K, V> val, 
            GridClientPortableWriteContext ctx, byte dictType)
        {
            WriteInt(val.Count, ctx.Stream);
            WriteByte(dictType, ctx.Stream);

            foreach (KeyValuePair<K, V> entry in val)
            {
                ctx.Write(entry.Key);
                ctx.Write(entry.Value);
            }
        }

        /**
         * <summary>Read generic dictionary.</summary>
         * <param name="ctx">Context.</param>
         * <param name="factory">Factory delegate.</param>
         * <returns>Collection.</returns>
         */
        public static IDictionary<K, V> ReadGenericDictionary<K, V>(GridClientPortableReadContext ctx, 
            GridClientPortableGenericDictionaryFactory<K, V> factory)
        {
            int len = ReadInt(ctx.Stream);

            if (len >= 0)
            {
                byte colType = ReadByte(ctx.Stream);

                if (factory == null)
                {
                    if (colType == MAP_SORTED_MAP)
                        factory = PSH.CreateSortedDictionary<K, V>;
                    else if (colType == MAP_CONCURRENT_HASH_MAP)
                        factory = PSH.CreateConcurrentDictionary<K, V>;
                    else
                        factory = PSH.CreateDictionary<K, V>;
                }

                IDictionary<K, V> res = factory.Invoke(len);

                for (int i = 0; i < len; i++)
                {
                    K key = ctx.Deserialize<K>(ctx.Stream);
                    V val = ctx.Deserialize<V>(ctx.Stream);

                    res[key] = val;
                }

                return res;
            }
            else
                return null;
        }

        /**
         * <summary>Write map entry.</summary>
         * <param name="ctx">Write context.</param>
         * <param name="val">Value.</param>
         */
        public static void WriteMapEntry(GridClientPortableWriteContext ctx, DictionaryEntry val)
        {
            ctx.Write(val.Key);
            ctx.Write(val.Value);
        }

        /**
         * <summary>Read map entry.</summary>
         * <param name="ctx">Context.</param>
         * <returns>Map entry.</returns>
         */
        public static DictionaryEntry ReadMapEntry(GridClientPortableReadContext ctx)
        {
            object key = ctx.Deserialize<object>(ctx.Stream);
            object val = ctx.Deserialize<object>(ctx.Stream);

            return new DictionaryEntry(key, val);
        }

        /**
         * <summary>Read map entry in portable form.</summary>
         * <param name="stream">Stream.</param>
         * <param name="marsh">Marshaller.</param>
         * <returns>Map entry.</returns>
         */
        public static DictionaryEntry ReadMapEntryPortable(MemoryStream stream, GridClientPortableMarshaller marsh)
        {
            IGridClientPortableObject key = ReadPortable(stream, marsh, false);
            IGridClientPortableObject val = ReadPortable(stream, marsh, false);

            return new DictionaryEntry(PortableOrPredefined<object>(key), PortableOrPredefined<object>(val));
        }

        /**
         * <summary>Write portable object.</summary>
         * <param name="stream">Stream.</param>
         * <param name="val">Value.</param>
         */
        public static void WritePortable(Stream stream, GridClientPortableObjectImpl val)
        {
            WriteByteArray(val.Data, stream);
            WriteInt(val.Offset, stream);
        }

        /**
         * <summary>Read portable object.</summary>
         * <param name="stream">Stream.</param>
         * <param name="marsh">Marshaller.</param>
         * <param name="detach">Detach flag.</param>
         * <returns>Portable object.</returns>
         */
        public static IGridClientPortableObject ReadPortable(MemoryStream stream,
            GridClientPortableMarshaller marsh, bool detach)
        {
            IGridClientPortableObject obj;

            byte hdr = ReadByte(stream);

            if (hdr == HDR_NULL)
                obj = null;
            else if (hdr == HDR_HND || hdr == HDR_FULL || IsPredefinedType(hdr))
            {
                obj = marsh.Unmarshal0(stream, false, stream.Position - 1, hdr);

                if (detach)
                    ((GridClientPortableObjectImpl)obj).Detachable = true;
            }
            else
                throw new GridClientPortableException("Unexpected header: " + hdr);

            return obj;
        }

        /**
         * <summary>Gets type key.</summary>
         * <param name="userType">User type flag.</param>
         * <param name="typeId">Type ID.</param>
         * <returns>Type key.</returns>
         */ 
        public static long TypeKey(bool userType, int typeId)
        {
            long res = typeId;

            if (userType)
                res |= 1 << 32;

            return res;
        }

        /**
         * <summary>Extract underlying array from memory stream.</summary>
         * <param name="stream">Memory stream.</param>
         * <returns>Extracted array.</returns>
         */ 
        public static byte[] MemoryBuffer(MemoryStream stream)
        {
            return (byte[])FIELD_MEM_BUF.GetValue(stream);
        }

        /**
         * <summary>Get string hash code.</summary> 
         * <param name="val">Value.</param>
         * <returns>Hash code.</returns>
         */
        public static int StringHashCode(string val)
        {
            if (val == null || val.Length == 0)
                return 0;
            else
            {
                char[] arr = val.ToCharArray();

                int hash = 0;

                foreach (char c in val.ToCharArray())
                    hash = 31 * hash + c;

                return hash;
            }
        }
        
        /**
         * <summary>Get Guid hash code.</summary> 
         * <param name="val">Value.</param>
         * <returns>Hash code.</returns>
         */
        public static int GuidHashCode(Guid val)
        {
            byte[] arr = val.ToByteArray();

            long msb = 0;
            long lsb = 0;

            for (int i = 0; i < 8; i++)
                msb = (msb << 8) | ((uint)arr[i] & 0xff);

            for (int i = 8; i < 16; i++)
                lsb = (lsb << 8) | ((uint)arr[i] & 0xff);

            long hilo = msb ^ lsb;

            return ((int)(hilo >> 32)) ^ (int)hilo;
        }

        /**
         * <summary>Check whether this is predefined type.</summary>
         * <param name="hdr">Header.</param>
         * <returns>True is this is one of predefined types with special semantics.</returns>
         */
        public static bool IsPredefinedType(byte hdr)
        {
            switch (hdr)
            {
                case TYPE_BYTE:
                case TYPE_SHORT:
                case TYPE_INT:
                case TYPE_LONG:
                case TYPE_FLOAT:
                case TYPE_DOUBLE:
                case TYPE_CHAR:
                case TYPE_BOOL:
                case TYPE_STRING:
                case TYPE_GUID:
                case TYPE_DATE:
                case TYPE_ARRAY_BYTE:
                case TYPE_ARRAY_SHORT:
                case TYPE_ARRAY_INT:
                case TYPE_ARRAY_LONG:
                case TYPE_ARRAY_FLOAT:
                case TYPE_ARRAY_DOUBLE:
                case TYPE_ARRAY_CHAR:
                case TYPE_ARRAY_BOOL:
                case TYPE_ARRAY_STRING:
                case TYPE_ARRAY_GUID:
                case TYPE_ARRAY_DATE:
                case TYPE_ARRAY:
                case TYPE_COLLECTION:
                case TYPE_DICTIONARY:
                    return true;
                default:
                    return false;
            }
        }

        /**
         * <summary>Return either portable object as is or it's deserialized object or predefined type.</summary>
         * <param name="portObj">Portable object.</param>
         * <returns>Portable object or one of predefined types.</returns>
         */ 
        public static T PortableOrPredefined<T>(IGridClientPortableObject portObj)
        {
            if (portObj != null && !portObj.IsUserType() && IsPredefinedType((byte)portObj.TypeId()))
                return portObj.Deserialize<T>();
            else
                return (T)(object)portObj;
        }

        /**
         * <summary>Convert date to Java ticks.</summary>
         * <param name="date">Date</param>
         * <param name="high">High part (milliseconds).</param>
         * <param name="low">Low part (100ns chunks)</param>
         */
        private static void ToJavaDate(DateTime date, out long high, out short low)
        {
            long diff = date.Ticks - JAVA_DATE_TICKS;

            high = diff / JAVA_DATE_MULTIPLIER;

            low = (short)(diff % JAVA_DATE_MULTIPLIER); 
        }

        /**
         * <summary>Convert Java ticks to date.</summary>
         * <param name="high">High part (milliseconds).</param>
         * <param name="low">Low part (100ns chunks).</param>
         * <returns>Date.</returns>
         */
        private static DateTime ToDotNetDate(long high, short low)
        {
            return new DateTime(JAVA_DATE_TICKS + high * JAVA_DATE_MULTIPLIER + low);
        }
    }
}
