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

    /**
     * <summary>Utilities for portable serialization.</summary>
     */ 
    static class GridClientPortableUilts
    {
        /** Header of NULL object. */
        public const byte HDR_NULL = 80;

        /** Header of object handle. */
        public const byte HDR_HND = 81;

        /** Header of object in fully serialized form. */
        public const byte HDR_FULL = 82;

        /** Header of object in fully serailized form with metadata. */
        public const byte HDR_META = 83;

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
        public const byte TYPE_MAP = 25;

        /** Type: authentication request. */
        public const byte TYPE_AUTH_REQ = 100;

        /** Type: topology request. */
        public const byte TYPE_TOP_REQ = 101;

        /** Type: task request. */
        public const byte TYPE_TASK_REQ = 102;

        /** Type: cache request. */
        public const byte TYPE_CACHE_REQ = 103;
        
        /** Type: log request. */
        public const byte TYPE_LOG_REQ = 104;

        /** Type: response. */
        public const byte TYPE_RESP = 105;

        /** Type: node bean. */
        public const byte TYPE_NODE_BEAN = 106;

        /** Type: node metrics bean. */
        public const byte TYPE_NODE_METRICS_BEAN = 107;

        /** Type: task result bean. */
        public const byte TYPE_TASK_RES_BEAN = 108;

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
        
        /** Whether little endian is set. */
        private static readonly bool LITTLE_ENDIAN = BitConverter.IsLittleEndian;
        
        /** Memory stream buffer field info. */
        private static FieldInfo MEM_BUF_FIELD = typeof(MemoryStream).GetField("_buffer", 
            BindingFlags.Instance | BindingFlags.NonPublic);
        
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
            if (vals == null)
                stream.WriteByte(BYTE_ZERO);
            else
            {
                stream.WriteByte(BYTE_ONE);

                WriteInt(vals.Length, stream);

                for (int i = 0; i < vals.Length; i++)
                    stream.WriteByte(vals[i] ? BYTE_ONE : BYTE_ZERO);
            }
        }

        /**
         * <summary>Read boolean array.</summary>
         * <param name="stream">Output stream.</param>
         * <returns>Value.</returns>
         */
        public static bool[] ReadBooleanArray(Stream stream)
        {
            if (stream.ReadByte() == BYTE_ZERO)
                return null;
            else 
            {                
                bool[] vals = new bool[ReadInt(stream)];

                for (int i = 0; i < vals.Length; i++)
                    vals[i] = stream.ReadByte() == BYTE_ONE;

                return vals;
            }
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
            if (vals == null)
                stream.WriteByte(BYTE_ZERO);
            else
            {
                stream.WriteByte(BYTE_ONE);

                WriteInt(vals.Length, stream);

                stream.Write(vals, 0, vals.Length);
            }
        }

        /**
         * <summary>Read byte array.</summary>
         * <param name="stream">Output stream.</param>
         * <param name="signed">Signed flag.</param>
         * <returns>Value.</returns>
         */
        public static unsafe object ReadByteArray(Stream stream, bool signed)
        {
            if (stream.ReadByte() == BYTE_ONE)
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
            else
                return null;
        }

        /**
         * <summary>Write short value.</summary>
         * <param name="val">Value.</param>
         * <param name="stream">Output stream.</param>
         */
        public static void WriteShort(short val, Stream stream)
        {
            if (LITTLE_ENDIAN)
            {
                stream.WriteByte((byte)(val & 0xFF));
                stream.WriteByte((byte)(val >> 8 & 0xFF));
            }
            else
            {
                stream.WriteByte((byte)(val >> 8 & 0xFF));
                stream.WriteByte((byte)(val & 0xFF));
            }
        }

        /**
         * <summary>Read short value.</summary>
         * <param name="stream">Output stream.</param>
         * <returns>Value.</returns>
         */
        public static short ReadShort(Stream stream)
        {
            short val = 0;

            if (LITTLE_ENDIAN)
            {
                val |= (short)stream.ReadByte();
                val |= (short)(stream.ReadByte() << 8);
            }
            else
            {
                val |= (short)(stream.ReadByte() << 8);
                val |= (short)stream.ReadByte();
            }

            return val;
        }

        /**
         * <summary>Write short array.</summary>
         * <param name="vals">Value.</param>
         * <param name="stream">Output stream.</param>
         */
        public static void WriteShortArray(short[] vals, Stream stream)
        {
            if (vals == null)
                stream.WriteByte(BYTE_ZERO);
            else
            {
                stream.WriteByte(BYTE_ONE);

                WriteInt(vals.Length, stream);

                for (int i = 0; i < vals.Length; i++)
                    WriteShort(vals[i], stream);                   
            }
        }

        /**
         * <summary>Read short array.</summary>
         * <param name="stream">Stream.</param>
         * <param name="signed">Signed flag.</param>
         * <returns>Value.</returns>
         */
        public static unsafe object ReadShortArray(Stream stream, bool signed)
        {
            if (stream.ReadByte() == BYTE_ONE)
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
            else
                return null;
        }

        /**
         * <summary>Write int value.</summary>
         * <param name="val">Value.</param>
         * <param name="stream">Output stream.</param>
         */
        public static void WriteInt(int val, Stream stream)
        {
            if (LITTLE_ENDIAN)
            {
                stream.WriteByte((byte)(val & 0xFF));
                stream.WriteByte((byte)(val >> 8 & 0xFF));
                stream.WriteByte((byte)(val >> 16 & 0xFF));
                stream.WriteByte((byte)(val >> 24 & 0xFF));
            }
            else
            {
                stream.WriteByte((byte)(val >> 24 & 0xFF));
                stream.WriteByte((byte)(val >> 16 & 0xFF));
                stream.WriteByte((byte)(val >> 8 & 0xFF));
                stream.WriteByte((byte)(val & 0xFF));                
            }
        }

        /**
         * <summary>Read int value.</summary>
         * <param name="stream">Output stream.</param>
         * <returns>Value.</returns>
         */
        public static int ReadInt(Stream stream)
        {
            int val = 0;

            if (LITTLE_ENDIAN)
            {
                val |= stream.ReadByte();
                val |= stream.ReadByte() << 8;
                val |= stream.ReadByte() << 16;
                val |= stream.ReadByte() << 24;
            }
            else
            {
                val |= stream.ReadByte() << 24;
                val |= stream.ReadByte() << 16;
                val |= stream.ReadByte() << 8;
                val |= stream.ReadByte();                
            }

            return val;
        }

        /**
         * <summary>Write int array.</summary>
         * <param name="vals">Value.</param>
         * <param name="stream">Output stream.</param>
         */
        public static void WriteIntArray(int[] vals, Stream stream)
        {
            if (vals == null)
                stream.WriteByte(BYTE_ZERO);
            else
            {
                stream.WriteByte(BYTE_ONE);

                WriteInt(vals.Length, stream);

                for (int i = 0; i < vals.Length; i++)
                    WriteInt(vals[i], stream);
            }
        }

        /**
         * <summary>Read int array.</summary>
         * <param name="stream">Stream.</param>
         * <param name="signed">Signed flag.</param>
         * <returns>Value.</returns>
         */
        public static unsafe object ReadIntArray(Stream stream, bool signed)
        {
            if (stream.ReadByte() == BYTE_ONE)
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
            else
                return null;
        }

        /**
         * <summary>Write long value.</summary>
         * <param name="val">Value.</param>
         * <param name="stream">Output stream.</param>
         */
        public static void WriteLong(long val, Stream stream)
        {
            if (LITTLE_ENDIAN)
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
            else
            {
                stream.WriteByte((byte)(val >> 56 & 0xFF));
                stream.WriteByte((byte)(val >> 48 & 0xFF));
                stream.WriteByte((byte)(val >> 40 & 0xFF));
                stream.WriteByte((byte)(val >> 32 & 0xFF));
                stream.WriteByte((byte)(val >> 24 & 0xFF));
                stream.WriteByte((byte)(val >> 16 & 0xFF));
                stream.WriteByte((byte)(val >> 8 & 0xFF));
                stream.WriteByte((byte)(val & 0xFF));
            }
        }

        /**
         * <summary>Read long value.</summary>
         * <param name="stream">Output stream.</param>
         * <returns>Value.</returns>
         */
        public static long ReadLong(Stream stream)
        {
            long val = 0;

            if (LITTLE_ENDIAN)
            {
                val |= (long)(stream.ReadByte()) << 0;
                val |= (long)(stream.ReadByte()) << 8;
                val |= (long)(stream.ReadByte()) << 16;
                val |= (long)(stream.ReadByte()) << 24;
                val |= (long)(stream.ReadByte()) << 32;
                val |= (long)(stream.ReadByte()) << 40;
                val |= (long)(stream.ReadByte()) << 48;
                val |= (long)(stream.ReadByte()) << 56; 
            }
            else
            {
                val |= (long)(stream.ReadByte()) << 56;
                val |= (long)(stream.ReadByte()) << 48;
                val |= (long)(stream.ReadByte()) << 40;
                val |= (long)(stream.ReadByte()) << 32;
                val |= (long)(stream.ReadByte()) << 24;
                val |= (long)(stream.ReadByte()) << 16;
                val |= (long)(stream.ReadByte()) << 8;
                val |= (long)(stream.ReadByte()) << 0;              
            }

            return val;
        }

        /**
         * <summary>Write long array.</summary>
         * <param name="vals">Value.</param>
         * <param name="stream">Output stream.</param>
         */
        public static void WriteLongArray(long[] vals, Stream stream)
        {
            if (vals == null)
                stream.WriteByte(BYTE_ZERO);
            else
            {
                stream.WriteByte(BYTE_ONE);

                WriteInt(vals.Length, stream);

                for (int i = 0; i < vals.Length; i++)
                    WriteLong(vals[i], stream);
            }
        }

        /**
         * <summary>Read long array.</summary>
         * <param name="stream">Stream.</param>
         * <param name="signed">Signed flag.</param>
         * <returns>Value.</returns>
         */
        public static unsafe object ReadLongArray(Stream stream, bool signed)
        {
            if (stream.ReadByte() == BYTE_ONE)
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
            else
                return null;
        }

        /**
         * <summary>Write char array.</summary>
         * <param name="vals">Value.</param>
         * <param name="stream">Output stream.</param>
         */
        public static void WriteCharArray(char[] vals, Stream stream)
        {
            if (vals == null)
                stream.WriteByte(BYTE_ZERO);
            else
            {
                stream.WriteByte(BYTE_ONE);

                WriteInt(vals.Length, stream);

                for (int i = 0; i < vals.Length; i++)
                    WriteShort((short)vals[i], stream);
            }
        }

        /**
         * <summary>Read char array.</summary>
         * <param name="stream">Stream.</param>
         * <returns>Value.</returns>
         */
        public static char[] ReadCharArray(Stream stream)
        {
            if (stream.ReadByte() == BYTE_ONE)
            {
                int len = ReadInt(stream);

                char[] vals = new char[len];

                for (int i = 0; i < len; i++)
                    vals[i] = (char)ReadShort(stream);

                return vals;
            }
            else
                return null;
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
            if (vals == null)
                stream.WriteByte(BYTE_ZERO);
            else
            {
                stream.WriteByte(BYTE_ONE);

                WriteInt(vals.Length, stream);

                for (int i = 0; i < vals.Length; i++)
                    WriteFloat((float)vals[i], stream);
            }
        }

        /**
         * <summary>Read float array.</summary>
         * <param name="stream">Stream.</param>
         * <returns>Value.</returns>
         */
        public static float[] ReadFloatArray(Stream stream)
        {
            if (stream.ReadByte() == BYTE_ONE)
            {
                int len = ReadInt(stream);

                float[] vals = new float[len];

                for (int i = 0; i < len; i++)
                    vals[i] = ReadFloat(stream);

                return vals;
            }
            else
                return null;
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
            if (vals == null)
                stream.WriteByte(BYTE_ZERO);
            else
            {
                stream.WriteByte(BYTE_ONE);

                WriteInt(vals.Length, stream);

                for (int i = 0; i < vals.Length; i++)
                    WriteDouble((double)vals[i], stream);
            }
        }

        /**
         * <summary>Read double array.</summary>
         * <param name="stream">Stream.</param>
         * <returns>Value.</returns>
         */ 
        public static double[] ReadDoubleArray(Stream stream)
        {
            if (stream.ReadByte() == BYTE_ONE)
            {
                int len = ReadInt(stream);

                double[] vals = new double[len];

                for (int i = 0; i < len; i++)
                    vals[i] = ReadDouble(stream);

                return vals;
            }
            else
                return null;
        }

        /**
         * <summary>Write string in UTF8 encoding.</summary>
         * <param name="val">String.</param>
         * <param name="stream">Stream.</param>
         */
        public static void WriteString(string val, Stream stream)
        {
            if (val == null)
                stream.WriteByte(BYTE_ZERO);
            else
            {
                byte[] bytes = Encoding.UTF8.GetBytes(val);

                stream.WriteByte(BYTE_ONE);

                WriteInt(bytes.Length, stream);

                stream.Write(bytes, 0, bytes.Length);
            }
        }

        /**
         * <summary>Read string in UTF8 encoding.</summary>
         * <param name="stream">Stream.</param>
         * <returns>String.</returns>
         */
        public static string ReadString(Stream stream)
        {
            if (stream.ReadByte() == BYTE_ZERO)
                return null;
            else
            {
                int len = ReadInt(stream);

                byte[] bytes = new byte[len];

                stream.Read(bytes, 0, len);

                return Encoding.UTF8.GetString(bytes);
            }
        }

        /**
         * <summary>Write string array in UTF8 encoding.</summary>
         * <param name="vals">String array.</param>
         * <param name="stream">Stream.</param>
         */
        public static void WriteStringArray(string[] vals, Stream stream)
        {
            if (vals != null)
            {
                stream.WriteByte(BYTE_ONE);

                WriteInt(vals.Length, stream);

                foreach (string val in vals)
                    WriteString(val, stream);
            }
            else
                stream.WriteByte(BYTE_ZERO);
        }

        /**
         * <summary>Read string array in UTF8 encoding.</summary>
         * <param name="stream">Stream.</param>
         * <returns>String array.</returns>
         */
        public static string[] ReadStringArray(Stream stream)
        {
            if (stream.ReadByte() == BYTE_ONE)
            {
                int len = ReadInt(stream);

                string[] vals = new string[len];

                for (int i = 0; i < len; i++)
                    vals[i] = ReadString(stream);

                return vals;
            }
            else
                return null;
        }

        /**
         * <summary>Write GUID.</summary>
         * <param name="val">GUID.</param>
         * <param name="stream">Stream.</param>
         */
        public static void WriteGuid(Guid? val, Stream stream)
        {
            if (val.HasValue)
            {
                byte[] bytes = val.Value.ToByteArray();

                stream.WriteByte(BYTE_ONE);
                stream.Write(bytes, 0, bytes.Length);
            }
            else
                stream.WriteByte(BYTE_ZERO);
        }

        /**
         * <summary>Read GUID.</summary>
         * <param name="stream">Stream.</param>
         * <returns>GUID</returns>
         */
        public static Guid? ReadGuid(Stream stream)
        {
            if (stream.ReadByte() == BYTE_ZERO)
                return null;
            else
            {
                byte[] bytes = new byte[16];

                stream.Read(bytes, 0, 16);

                return new Guid(bytes);
            }
        }

        /**
         * <summary>Write GUID array.</summary>
         * <param name="vals">GUID array.</param>
         * <param name="stream">Stream.</param>
         */
        public static void WriteGuidArray(Guid?[] vals, Stream stream)
        {
            if (vals != null)
            {
                stream.WriteByte(BYTE_ONE);

                WriteInt(vals.Length, stream);

                foreach (Guid? val in vals)
                    WriteGuid(val, stream);
            }
            else
                stream.WriteByte(BYTE_ZERO);
        }

        /**
         * <summary>Read GUID array.</summary>
         * <param name="stream">Stream.</param>
         * <returns>GUID array.</returns>
         */
        public static Guid?[] ReadGuidArray(Stream stream)
        {
            if (stream.ReadByte() == BYTE_ONE)
            {
                int len = ReadInt(stream);

                Guid?[] vals = new Guid?[len];

                for (int i = 0; i < len; i++)
                    vals[i] = ReadGuid(stream);

                return vals;
            }
            else
                return null;
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
            return (byte[])MEM_BUF_FIELD.GetValue(stream);
        }
        /**
         * <summary>Get string hash code.</summary> 
         * <param name="val">Value.</param>
         * <returns>Hash code.</returns>
         */
        public static int StringHashCode(string val)
        {
            if (val.Length == 0)
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
         * 
         */
        public static int ArrayHashCode<T>(T[] arr)
        {
            int hash = 1;

            for (int i = 0; i < arr.Length; i++)
            {
                T item = arr[i];

                hash = 31 * hash + (item == null ? 0 : item.GetHashCode());
            }

            return hash;
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

    }
}
