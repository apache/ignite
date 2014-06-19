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
    using System.Text;
    using GridGain.Client.Portable;    

    /**
     * <summary>Utilities for portable serialization.</summary>
     */ 
    static class GridClientPortableUilts
    {
        /** Type: boolean. */
        public const int TYPE_BOOL = 1;

        /** Type: unsigned byte. */
        public const int TYPE_BYTE = 2;
        
        /** Type: short. */
        public const int TYPE_SHORT = 5;

        /** Type: int. */
        public const int TYPE_INT = 7;

        /** Type: long. */
        public const int TYPE_LONG = 9;

        /** Type: char. */
        public const int TYPE_CHAR = 10;

        /** Type: float. */
        public const int TYPE_FLOAT = 11;

        /** Type: double. */
        public const int TYPE_DOUBLE = 12;

        /** Type: string. */
        public const int TYPE_STRING = 13;

        /** Type: GUID. */
        public const int TYPE_GUID = 14;

        /** Type: boolean array. */
        public const int TYPE_ARRAY_BOOL = 101;

        /** Type: unsigned byte array. */
        public const int TYPE_ARRAY_BYTE = 102;

        /** Type: short array. */
        public const int TYPE_ARRAY_SHORT = 105;

        /** Type: int array. */
        public const int TYPE_ARRAY_INT = 107;

        /** Type: long array. */
        public const int TYPE_ARRAY_LONG = 109;

        /** Type: char array. */
        public const int TYPE_ARRAY_CHAR = 110;

        /** Type: float array. */
        public const int TYPE_ARRAY_FLOAT = 111;

        /** Type: double array. */
        public const int TYPE_ARRAY_DOUBLE = 112;

        /** Type: string array. */
        public const int TYPE_ARRAY_STRING = 113;

        /** Type: GUID array. */
        public const int TYPE_ARRAY_GUID = 114;

        /** Type: object array. */
        public const int TYPE_ARRAY = 200;

        /** Type: collection. */
        public const int TYPE_COLLECTION = 201;

        /** Type: map. */
        public const int TYPE_MAP = 202;

        /** Type: authentication request. */
        public const int TYPE_AUTH_REQ = 300;

        /** Type: topology request. */
        public const int TYPE_TOP_REQ = 301;

        /** Type: task request. */
        public const int TYPE_TASK_REQ = 302;

        /** Type: cache request. */
        public const int TYPE_CACHE_REQ = 303;
        
        /** Type: log request. */
        public const int TYPE_LOG_REQ = 304;

        /** Type: response. */
        public const int TYPE_RESP = 305;

        /** Type: node bean. */
        public const int TYPE_NODE_BEAN = 306;

        /** Type: node metrics bean. */
        public const int TYPE_NODE_METRICS_BEAN = 307;

        /** Byte "0". */
        private const byte BYTE_ZERO = (byte)0;

        /** Byte "1". */
        private const byte BYTE_ONE = (byte)1;
        
        /** Whether little endian is set. */
        private static readonly bool LITTLE_ENDIAN = BitConverter.IsLittleEndian;

        private static readonly Dictionary<Type, int> SYSTEM_TYPES = new Dictionary<Type,int>();

        /**
         * Static initializer.
         */ 
        static GridClientPortableUilts()
        {
            // 1. Add primitive types.
            SYSTEM_TYPES[typeof(bool)] = TYPE_BOOL;
            SYSTEM_TYPES[typeof(sbyte)] = TYPE_BYTE;
            SYSTEM_TYPES[typeof(byte)] = TYPE_BYTE;
            SYSTEM_TYPES[typeof(short)] = TYPE_SHORT;
            SYSTEM_TYPES[typeof(ushort)] = TYPE_SHORT;
            SYSTEM_TYPES[typeof(int)] = TYPE_INT;
            SYSTEM_TYPES[typeof(uint)] = TYPE_INT;            
            SYSTEM_TYPES[typeof(long)] = TYPE_LONG;
            SYSTEM_TYPES[typeof(ulong)] = TYPE_LONG;
            SYSTEM_TYPES[typeof(char)] = TYPE_CHAR;
            SYSTEM_TYPES[typeof(float)] = TYPE_FLOAT;
            SYSTEM_TYPES[typeof(double)] = TYPE_DOUBLE;            
        }   

        /**
         * <summary>Get primitive type ID.</summary>
         * <param name="type">Type.</param>
         * <returns>Primitive type ID or 0 if this is not primitive type.</returns>
         */ 
        public static int PrimitiveTypeId(Type type)
        {
            if (type == typeof(Boolean))
                return TYPE_BOOL;
            else if (type == typeof(Byte) || type == typeof(SByte))
                return TYPE_BYTE;
            else if (type == typeof(Int16) || type == typeof(UInt16))
                return TYPE_SHORT;
            else if (type == typeof(Int32) || type == typeof(Int32))
                return TYPE_INT;
            else if (type == typeof(Int64) || type == typeof(Int64))
                return TYPE_LONG;
            else if (type == typeof(Char))
                return TYPE_CHAR;
            else if (type == typeof(Single))
                return TYPE_FLOAT;
            else if (type == typeof(Double))
                return TYPE_DOUBLE;
            else
                return 0;
        }

        /**
         * <summary>Get primitive type length.</summary>
         * <param name="typeId">Type ID.</param>
         * <returns>Primitive type length.</returns>
         */
        public static int PrimitiveLength(int typeId)
        {
            switch (typeId) {
                case TYPE_BOOL:
                case TYPE_BYTE:
                    return 1;
                case TYPE_SHORT:
                case TYPE_CHAR:
                    return 2;
                case TYPE_INT:
                case TYPE_FLOAT:
                    return 4;
                case TYPE_LONG:
                case TYPE_DOUBLE:
                    return 8;
                default:
                    throw new GridClientPortableException("Type ID doesn't refer to primitive type: " + typeId);
            }
        }

        /**
         * <summary>Write primitive value to the underlying output.</summary>
         * <param name="typeId">Primitive type ID</param>
         * <param name="obj">Object.</param>
         * <param name="stream">Output stream.</param>
         */
        public static unsafe void WritePrimitive(int typeId, object obj, Stream stream)
        {
            WriteBoolean(false, stream);
            WriteInt(typeId, stream);

            unchecked
            {
                switch (typeId)
                {
                    case TYPE_BOOL:
                        WriteBoolean((bool)obj, stream);

                        break;

                    case TYPE_BYTE:
                        stream.WriteByte((byte)obj);

                        break;

                    case TYPE_SHORT:
                    case TYPE_CHAR:
                        WriteShort((short)obj, stream);

                        break;

                    case TYPE_INT:
                        WriteInt((int)obj, stream);

                        break;

                    case TYPE_LONG:
                        WriteLong((long)obj, stream);

                        break;

                    case TYPE_FLOAT:
                        float floatVal = (float)obj;

                        WriteInt(*(int*)&floatVal, stream);

                        break;

                    case TYPE_DOUBLE:
                        double doubleVal = (double)obj;

                        WriteLong(*(long*)&doubleVal, stream);

                        break;

                    default:
                        throw new GridClientPortableException("Type ID doesn't refer to primitive type: " + typeId);
                }
            }
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
                stream.Write(bytes, 0, bytes.Length);
            }
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

            for (int i = 0; i < arr.Length; i++) {
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

            for (int i=8; i<16; i++)
                lsb = (lsb << 8) | ((uint)arr[i] & 0xff);

            long hilo = msb ^ lsb;

            return ((int)(hilo >> 32)) ^ (int)hilo;
        }

        /**
         * <summary>Write GUID.</summary>
         * <param name="val">GUID.</param>
         * <param name="stream">Stream.</param>
         */
        public static void WriteGuid(Guid val, Stream stream)
        {
            if (val == null)
                stream.WriteByte(BYTE_ZERO);
            else
            {
                byte[] bytes = val.ToByteArray();

                stream.WriteByte(BYTE_ONE);
                stream.Write(bytes, 0, bytes.Length);
            }
        }

        /**
         * <summary>Get primitive array type ID.</summary>
         * <param name="type">Type.</param>
         * <returns>Primitive array type ID or 0 if this is not primitive array type.</returns>
         */
        public static int PrimitiveArrayTypeId(Type type)
        {
            if (type.IsArray)
            {
                Type elemType = type.GetElementType();

                if (elemType == typeof(Boolean))
                    return TYPE_ARRAY_BOOL;
                else if (elemType == typeof(Byte) || type == typeof(SByte))
                    return TYPE_ARRAY_BYTE;
                else if (elemType == typeof(Int16) || type == typeof(UInt16))
                    return TYPE_ARRAY_SHORT;
                else if (elemType == typeof(Int32) || type == typeof(Int32))
                    return TYPE_ARRAY_INT;
                else if (elemType == typeof(Int64) || type == typeof(Int64))
                    return TYPE_ARRAY_LONG;
                else if (elemType == typeof(Char))
                    return TYPE_ARRAY_CHAR;
                else if (elemType == typeof(Single))
                    return TYPE_ARRAY_FLOAT;
                else if (elemType == typeof(Double))
                    return TYPE_ARRAY_DOUBLE;
            }

            return 0;
        }

        /**
         * <summary>Write primitive array to the underlying output.</summary>
         * <param name="typeId">Primitive array type ID</param>
         * <param name="obj">Array object.</param>
         * <param name="stream">Output stream.</param>
         */
        public static void WritePrimitiveArray(int typeId, object obj, Stream stream)
        {
            switch (typeId) {
                case TYPE_ARRAY_BOOL:
                    WriteBooleanArray((bool[])obj, stream);

                    break;

                case TYPE_ARRAY_BYTE:
                    WriteByteArray((byte[])obj, stream);

                    break;

                case TYPE_ARRAY_SHORT:
                    WriteShortArray((short[])obj, stream);

                    break;

                case TYPE_ARRAY_INT:                
                    

                    break;

                case TYPE_ARRAY_LONG:
                    

                    break;

                case TYPE_ARRAY_CHAR:
                case TYPE_ARRAY_FLOAT:
                case TYPE_ARRAY_DOUBLE:
                default:
                    throw new GridClientPortableException("Type ID doesn't refer to primitive type: " + typeId);
            }
        }

        /**
         * <summary>Write boolean.</summary>
         * <param name="val">Value.</param>
         * <param name="stream">Output stream.</param>
         * <returns>Length of written data.</returns>
         */
        public static int WriteBoolean(bool val, Stream stream)
        {
            stream.WriteByte(val ? BYTE_ONE : BYTE_ZERO);

            return 1;
        }

        /**
         * <summary>Write boolean array.</summary>
         * <param name="vals">Value.</param>
         * <param name="stream">Output stream.</param>
         * <returns>Length of written data.</returns>
         */
        public static int WriteBooleanArray(bool[] vals, Stream stream)
        {
            byte[] bytes = new byte[vals.Length];

            for (int i = 0; i < vals.Length; i++) 
            {
                if (vals[i])
                    bytes[i] = BYTE_ONE;
            }

            stream.Write(bytes, 0, bytes.Length);

            return vals.Length;
        }

        /**
         * <summary>Write byte.</summary>
         * <param name="val">Value.</param>
         * <param name="stream">Output stream.</param>
         * <returns>Length of written data.</returns>
         */
        public static int WriteByte(byte val, Stream stream)
        {
            stream.WriteByte(val);

            return 1;
        }
        
        /**
         * <summary>Write byte array.</summary>
         * <param name="vals">Value.</param>
         * <param name="stream">Output stream.</param>
         * <returns>Length of written data.</returns>
         */
        public static int WriteByteArray(byte[] vals, Stream stream)
        {
            stream.Write(vals, 0, vals.Length);

            return vals.Length;
        }

        /**
         * <summary>Write short value.</summary>
         * <param name="val">Value.</param>
         * <param name="stream">Output stream.</param>
         * <returns>Length of written data.</returns>
         */
        public static unsafe int WriteShort(short val, Stream stream)
        {
            byte[] bytes = new byte[2];

            unchecked
            {
                if (LITTLE_ENDIAN)
                {
                    fixed (byte* b = bytes)
                    {
                        *((short*)b) = val;
                    }
                }
                else
                {
                    bytes[0] = (byte)(val & 0xFF);
                    bytes[1] = (byte)(val >> 8 & 0xFF);
                }

                stream.Write(bytes, 0, 2);
            }

            return 2;
        }

        /**
         * <summary>Write byte array.</summary>
         * <param name="vals">Value.</param>
         * <param name="stream">Output stream.</param>
         * <returns>Length of written data.</returns>
         */
        public static void WriteShortArray(short[] vals, Stream stream)
        {
            
        }

        /**
         * <summary>Write int value.</summary>
         * <param name="val">Value.</param>
         * <param name="stream">Output stream.</param>
         */
        public static unsafe int WriteInt(int val, Stream stream)
        {
            unchecked
            {
                byte[] bytes = new byte[4];

                if (LITTLE_ENDIAN)
                {
                    fixed (byte* b = bytes)
                    {
                        *((int*)b) = val;
                    }
                }
                else
                {
                    bytes[0] = (byte)(val & 0xFF);
                    bytes[1] = (byte)(val >> 8 & 0xFF);
                    bytes[2] = (byte)(val >> 16 & 0xFF);
                    bytes[3] = (byte)(val >> 24 & 0xFF);
                }

                stream.Write(bytes, 0, 4);
            }

            return 4;
        }

        /**
         * <summary>Write long value.</summary>
         * <param name="val">Value.</param>
         * <param name="stream">Output stream.</param>
         */
        public static unsafe int WriteLong(long val, Stream stream)
        {
            unchecked
            {
                byte[] bytes = new byte[8];

                if (LITTLE_ENDIAN)
                {
                    fixed (byte* b = bytes)
                    {
                        *((long*)b) = val;
                    }
                }
                else
                {
                    bytes[0] = (byte)(val & 0xFF);
                    bytes[1] = (byte)(val >> 8 & 0xFF);
                    bytes[2] = (byte)(val >> 16 & 0xFF);
                    bytes[3] = (byte)(val >> 24 & 0xFF);
                    bytes[4] = (byte)(val >> 32 & 0xFF);
                    bytes[5] = (byte)(val >> 40 & 0xFF);
                    bytes[6] = (byte)(val >> 48 & 0xFF);
                    bytes[7] = (byte)(val >> 54 & 0xFF);
                }

                stream.Write(bytes, 0, 8);
            }

            return 8;
        }
    }
}
