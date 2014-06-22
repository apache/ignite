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
    using System.Reflection;
    using System.Text;
    using GridGain.Client.Portable;

    /**
     * <summary>Utilities for portable serialization.</summary>
     */ 
    static class GridClientPortableUilts
    {
        /** Header of NULL object. */
        public const byte HDR_NULL = 0x80;

        /** Header of object handle. */
        public const byte HDR_HND = 0x81;

        /** Header of object in fully serialized form. */
        public const byte HDR_FULL = 0x82;

        /** Header of object in fully serailized form with metadata. */
        public const byte HDR_META = 0x83;

        /** Type: boolean. */
        public const byte TYPE_BOOL = 1;

        /** Type: unsigned byte. */
        public const byte TYPE_BYTE = 2;
        
        /** Type: short. */
        public const byte TYPE_SHORT = 5;

        /** Type: int. */
        public const byte TYPE_INT = 7;

        /** Type: long. */
        public const byte TYPE_LONG = 9;

        /** Type: char. */
        public const byte TYPE_CHAR = 10;

        /** Type: float. */
        public const byte TYPE_FLOAT = 11;

        /** Type: double. */
        public const byte TYPE_DOUBLE = 12;

        /** Type: string. */
        public const byte TYPE_STRING = 13;

        /** Type: GUID. */
        public const byte TYPE_GUID = 14;

        /** Type: boolean array. */
        public const byte TYPE_ARRAY_BOOL = 15;

        /** Type: unsigned byte array. */
        public const byte TYPE_ARRAY_BYTE = 16;

        /** Type: short array. */
        public const byte TYPE_ARRAY_SHORT = 17;

        /** Type: int array. */
        public const byte TYPE_ARRAY_INT = 18;

        /** Type: long array. */
        public const byte TYPE_ARRAY_LONG = 19;

        /** Type: char array. */
        public const byte TYPE_ARRAY_CHAR = 20;

        /** Type: float array. */
        public const byte TYPE_ARRAY_FLOAT = 21;

        /** Type: double array. */
        public const byte TYPE_ARRAY_DOUBLE = 22;

        /** Type: string array. */
        public const byte TYPE_ARRAY_STRING = 23;

        /** Type: GUID array. */
        public const byte TYPE_ARRAY_GUID = 24;

        /** Type: object array. */
        public const byte TYPE_ARRAY = 25;

        /** Type: collection. */
        public const byte TYPE_COLLECTION = 26;

        /** Type: map. */
        public const byte TYPE_MAP = 27;

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

        /** Byte "0". */
        public const byte BYTE_ZERO = (byte)0;

        /** Byte "1". */
        public const byte BYTE_ONE = (byte)1;
        
        /** Whether little endian is set. */
        private static readonly bool LITTLE_ENDIAN = BitConverter.IsLittleEndian;
        
        /** Memory stream buffer field info. */
        private static FieldInfo MEM_BUF_FIELD = typeof(MemoryStream).GetField("_buffer", 
            BindingFlags.Instance | BindingFlags.NonPublic);

        
        public static unsafe object ReadPrimitive(int typeId, Type type, Stream stream, out bool processed) 
        {
            if (type.IsGenericType)
                type = type.GetGenericArguments()[0];

            switch (typeId)
            {
                case TYPE_BOOL:
                    processed = true;

                    return ReadBoolean(stream);

                case TYPE_BYTE:
                    processed = true;

                    byte byteVal = (byte)stream.ReadByte();

                    if (type == typeof(byte))
                        return byteVal;
                    else
                        return *(sbyte*)&byteVal;

                case TYPE_SHORT:
                    processed = true;

                    short shortVal = ReadShort(stream);

                    if (type == typeof(short))
                        return shortVal;
                    else
                        return *(ushort*)&shortVal;

                case TYPE_INT:
                    processed = true;

                    int intVal = ReadInt(stream);

                    if (type == typeof(int))
                        return intVal;
                    else
                        return *(uint*)&intVal;

                case TYPE_LONG:
                    processed = true;

                    long longVal = ReadLong(stream);

                    if (type == typeof(long))
                        return longVal;
                    else
                        return *(ulong*)&longVal;

                case TYPE_CHAR:
                    processed = true;

                    short charVal = ReadShort(stream);

                    return *(char*)&charVal;

                case TYPE_FLOAT:
                    processed = true;

                    int floatVal = ReadInt(stream);

                    return *(float*)&floatVal;

                case TYPE_DOUBLE:
                    processed = true;

                    long doubleVal = ReadLong(stream);

                    return *(double*)&doubleVal;

                default:
                    processed = false;

                    return null;
            }
        }

        /**
         * <summary>Get primitive type ID.</summary>
         * <param name="type">Type.</param>
         * <returns>Primitive type ID or 0 if this is not primitive type.</returns>
         */ 
        public static byte PrimitiveTypeId(Type type)
        {
            if (type == typeof(Boolean))
                return TYPE_BOOL;
            else if (type == typeof(Byte) || type == typeof(SByte))
                return TYPE_BYTE;
            else if (type == typeof(Int16) || type == typeof(UInt16))
                return TYPE_SHORT;
            else if (type == typeof(Int32) || type == typeof(UInt32))
                return TYPE_INT;
            else if (type == typeof(Int64) || type == typeof(UInt64))
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
            unchecked
            {
                switch (typeId)
                {
                    case TYPE_BOOL:
                        WriteBoolean((bool)obj, stream);

                        break;

                    case TYPE_BYTE:
                        byte byteVal;

                        if (obj is Byte)
                            byteVal = (byte)obj;
                        else
                        {
                            sbyte sByteVal = (sbyte)obj;

                            byteVal = *(byte*)&sByteVal;
                        }

                        stream.WriteByte(byteVal);

                        break;

                    case TYPE_SHORT:
                        short shortVal;

                        if (obj is Int16)
                            shortVal = (short)obj;
                        else
                        {
                            ushort uShortVal = (ushort)obj;

                            shortVal = *(short*)&uShortVal;
                        }

                        WriteShort(shortVal, stream);

                        break;

                    case TYPE_CHAR:
                        char charVal = (char)obj;

                        WriteShort(*(short*)&charVal, stream);

                        break;

                    case TYPE_INT:
                        int intVal;

                        if (obj is Int32)
                            intVal = (int)obj;
                        else
                        {
                            uint uIntVal = (uint)obj;

                            intVal = *(int*)&uIntVal;
                        }

                        WriteInt(intVal, stream);

                        break;

                    case TYPE_LONG:
                        long longVal;

                        if (obj is Int64)
                            longVal = (long)obj;
                        else
                        {
                            ulong uLongVal = (ulong)obj;

                            longVal = *(long*)&uLongVal;
                        }

                        WriteLong(longVal, stream);

                        break;

                    case TYPE_FLOAT:
                        WriteFloat((float)obj, stream);

                        break;

                    case TYPE_DOUBLE:
                        WriteDouble((double)obj, stream);

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

        public static void WriteStringArray(string[] val, Stream stream)
        {
            if (val == null)
                stream.WriteByte(BYTE_ZERO);
            else
            {
                stream.WriteByte(BYTE_ONE);
                
                // TODO: Finish.

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
        public static void WriteGuid(Guid? val, Stream stream)
        {
            if (val.HasValue)
                stream.WriteByte(BYTE_ZERO);
            else
            {
                byte[] bytes = val.Value.ToByteArray();

                stream.WriteByte(BYTE_ONE);
                stream.Write(bytes, 0, bytes.Length);
            }
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
         * <summary>Get primitive array type ID.</summary>
         * <param name="type">Type.</param>
         * <returns>Primitive array type ID or 0 if this is not primitive array type.</returns>
         */
        public static byte PrimitiveArrayTypeId(Type type)
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
                    WriteIntArray((int[])obj, stream);

                    break;

                case TYPE_ARRAY_LONG:
                    WriteLongArray((long[])obj, stream);

                    break;

                case TYPE_ARRAY_CHAR:
                    WriteCharArray((char[])obj, stream);

                    break;

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
                {
                    vals[i] = stream.ReadByte() == BYTE_ONE;
                }

                return vals;
            }
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
        public static void WriteByteArray(byte[] vals, Stream stream)
        {
            if (vals == null)
                stream.WriteByte(BYTE_ZERO);
            else
            {
                stream.WriteByte(BYTE_ONE);
                stream.Write(vals, 0, vals.Length);
            }
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

                for (int i = 0; i < vals.Length; i++)
                    WriteShort(vals[i], stream);                   
            }
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

                for (int i = 0; i < vals.Length; i++)
                    WriteInt(vals[i], stream);
            }
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

                for (int i = 0; i < vals.Length; i++)
                    WriteLong(vals[i], stream);
            }
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

                for (int i = 0; i < vals.Length; i++)
                    WriteShort((short)vals[i], stream);
            }
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

                for (int i = 0; i < vals.Length; i++)
                    WriteFloat((float)vals[i], stream);
            }
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

                for (int i = 0; i < vals.Length; i++)
                    WriteDouble((double)vals[i], stream);
            }
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
         * 
         */ 
        public static byte[] MemoryBuffer(MemoryStream stream)
        {
            return (byte[])MEM_BUF_FIELD.GetValue(stream);
        }
    }
}
