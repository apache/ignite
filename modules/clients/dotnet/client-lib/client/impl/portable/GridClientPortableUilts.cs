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
    using GridGain.Client.Portable;

    /*
    sysPortableTypes.Add(GridClientAuthenticationRequest.PORTABLE_TYPE_ID, typeof(GridClientAuthenticationRequest));
           sysPortableTypes.Add(GridClientCacheRequest.PORTABLE_TYPE_ID, typeof(GridClientCacheRequest));
            sysPortableTypes.Add(GridClientLogRequest.PORTABLE_TYPE_ID, typeof(GridClientLogRequest));
            sysPortableTypes.Add(GridClientNodeBean.PORTABLE_TYPE_ID, typeof(GridClientNodeBean));
            sysPortableTypes.Add(GridClientNodeMetricsBean.PORTABLE_TYPE_ID, typeof(GridClientNodeMetricsBean));
            sysPortableTypes.Add(GridClientResponse.PORTABLE_TYPE_ID, typeof(GridClientResponse));
            sysPortableTypes.Add(GridClientTaskRequest.PORTABLE_TYPE_ID, typeof(GridClientTaskRequest));
            sysPortableTypes.Add(GridClientTopologyRequest.PORTABLE_TYPE_ID, typeof(GridClientTopologyRequest));

     */
 
    /**
     * <summary>Utilities for portable serialization.</summary>
     */ 
    static class GridClientPortableUilts
    {
        /** Type: boolean. */
        private const int TYPE_BOOL = 1;

        /** Type: unsigned byte. */
        private const int TYPE_BYTE = 2;
        
        /** Type: short. */
        private const int TYPE_SHORT = 5;

        /** Type: int. */
        private const int TYPE_INT = 7;

        /** Type: long. */
        private const int TYPE_LONG = 9;

        /** Type: char. */
        private const int TYPE_CHAR = 10;

        /** Type: float. */
        private const int TYPE_FLOAT = 11;

        /** Type: double. */
        private const int TYPE_DOUBLE = 12;

        /** Type: string. */
        private const int TYPE_STRING = 13;

        /** Type: GUID. */
        private const int TYPE_GUID = 14;

        /** Type: boolean array. */
        private const int TYPE_ARRAY_BOOL = 101;

        /** Type: unsigned byte array. */
        private const int TYPE_ARRAY_BYTE = 102;

        /** Type: short array. */
        private const int TYPE_ARRAY_SHORT = 105;

        /** Type: int array. */
        private const int TYPE_ARRAY_INT = 107;

        /** Type: long array. */
        private const int TYPE_ARRAY_LONG = 109;

        /** Type: char array. */
        private const int TYPE_ARRAY_CHAR = 110;

        /** Type: float array. */
        private const int TYPE_ARRAY_FLOAT = 111;

        /** Type: double array. */
        private const int TYPE_ARRAY_DOUBLE = 112;

        /** Type: string array. */
        private const int TYPE_ARRAY_STRING = 113;

        /** Type: GUID array. */
        private const int TYPE_ARRA_GUID = 114;

        /** Type: object array. */
        private const int TYPE_ARRAY = 200;

        /** Type: collection. */
        private const int TYPE_COLLECTION = 201;

        /** Type: map. */
        private const int TYPE_MAP = 202;

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
         * <returns>Primitive type ID.</returns>
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
         * <returns>Length of written data.</returns>
         */
        public static unsafe int WritePrimitive(int typeId, object obj, Stream stream)
        {
            unchecked
            {
                switch (typeId)
                {
                    case TYPE_BOOL:
                        return WriteBoolean((bool)obj, stream);

                    case TYPE_BYTE:
                        stream.WriteByte((byte)obj);

                        return 1;

                    case TYPE_SHORT:
                    case TYPE_CHAR:
                        return WriteShort((short)obj, stream);

                    case TYPE_INT:
                        return WriteInt((int)obj, stream);

                    case TYPE_LONG:
                        return WriteLong((long)obj, stream);

                    case TYPE_FLOAT:
                        float floatVal = (float)obj;

                        return WriteInt(*(int*)&floatVal, stream);

                    case TYPE_DOUBLE:
                        double doubleVal = (double)obj;

                        return WriteLong(*(long*)&doubleVal, stream);

                    default:
                        throw new GridClientPortableException("Type ID doesn't refer to primitive type: " + typeId);
                }
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
        public static int writeByte(byte val, Stream stream)
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
