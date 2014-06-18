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
    using GridGain.Client.Portable;

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
                throw new GridClientPortableException("Type is not primitive: " + type);
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
         * <param name="output">Output.</param>
         */
        public static unsafe void WritePrimitive(int typeId, object obj, IGridClientPortableMarshallerOutput output)
        {
            unchecked
            {
                switch (typeId)
                {
                    case TYPE_BOOL:
                        WriteBoolean((bool)obj, output);

                        break;

                    case TYPE_BYTE:
                        output.WriteByte((byte)obj);

                        break;

                    case TYPE_SHORT:
                    case TYPE_CHAR:
                        WriteShort((short)obj, output);

                        break;

                    case TYPE_INT:
                        WriteInt((int)obj, output);

                        break;

                    case TYPE_LONG:
                        WriteLong((long)obj, output);

                        break;

                    case TYPE_FLOAT:
                        float floatVal = (float)obj;

                        WriteInt(*(int*)&floatVal, output);

                        break;

                    case TYPE_DOUBLE:
                        double doubleVal = (double)obj;

                        WriteLong(*(long*)&doubleVal, output);

                        break;

                    default:
                        throw new GridClientPortableException("Type ID doesn't refer to primitive type: " + typeId);
                }
            }
        }

        /**
         * <summary>Write boolean value.</summary>
         * <param name="val">Value.</param>
         * <param name="output">Output.</param>
         */
        public static unsafe void WriteBoolean(bool val, IGridClientPortableMarshallerOutput output)
        {
            output.WriteByte(val ? BYTE_ONE : BYTE_ZERO);
        }

        /**
         * <summary>Write short value.</summary>
         * <param name="val">Value.</param>
         * <param name="output">Output.</param>
         */
        public static unsafe void WriteShort(short val, IGridClientPortableMarshallerOutput output)
        {
            unchecked
            {
                if (LITTLE_ENDIAN)
                {
                    byte[] bytes = new byte[2];

                    fixed (byte* b = bytes)
                    {
                        *((short*)b) = val;
                    }

                    output.WriteBytes(bytes);
                }
                else
                {
                    output.WriteByte((byte)(val & 0xFF));
                    output.WriteByte((byte)(val >> 8 & 0xFF));
                }
            }
        }

        /**
         * <summary>Write int value.</summary>
         * <param name="val">Value.</param>
         * <param name="output">Output.</param>
         */
        public static unsafe void WriteInt(int val, IGridClientPortableMarshallerOutput output)
        {
            unchecked
            {
                if (LITTLE_ENDIAN)
                {
                    byte[] bytes = new byte[4];

                    fixed (byte* b = bytes)
                    {
                        *((int*)b) = val;
                    }

                    output.WriteBytes(bytes);
                }
                else
                {
                    output.WriteByte((byte)(val & 0xFF));
                    output.WriteByte((byte)(val >> 8 & 0xFF));
                    output.WriteByte((byte)(val >> 16 & 0xFF));
                    output.WriteByte((byte)(val >> 24 & 0xFF));
                }
            }
        }

        /**
         * <summary>Write long value.</summary>
         * <param name="val">Value.</param>
         * <param name="output">Output.</param>
         */
        public static unsafe void WriteLong(long val, IGridClientPortableMarshallerOutput output)
        {
            unchecked
            {
                if (LITTLE_ENDIAN)
                {
                    byte[] bytes = new byte[8];

                    fixed (byte* b = bytes)
                    {
                        *((long*)b) = val;
                    }

                    output.WriteBytes(bytes);
                }
                else
                {
                    output.WriteByte((byte)(val & 0xFF));
                    output.WriteByte((byte)(val >> 8 & 0xFF));
                    output.WriteByte((byte)(val >> 16 & 0xFF));
                    output.WriteByte((byte)(val >> 24 & 0xFF));
                    output.WriteByte((byte)(val >> 32 & 0xFF));
                    output.WriteByte((byte)(val >> 40 & 0xFF));
                    output.WriteByte((byte)(val >> 48 & 0xFF));
                    output.WriteByte((byte)(val >> 54 & 0xFF));
                }
            } 
        }
    }
}
