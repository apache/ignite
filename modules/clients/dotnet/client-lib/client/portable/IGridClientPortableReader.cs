/* @csharp.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Client.Portable
{
    using System;
    using System.Collections.Generic;

    /**
     * <summary>Reader for portable objects.</summary>
     */
    public interface IGridClientPortableReader {
        /**
         * <summary>Read named byte value.</summary>
         * <param name="fieldName">Field name.</param>
         * <returns>Byte value.</returns>
         */
        byte ReadByte(string fieldName);

        /**
         * <summary>Read byte value.</summary>
         * <returns>Byte value.</returns>
         */
        byte ReadByte();

        /**
         * <summary>Read named byte array.</summary>
         * <returns>Byte array.</returns>
         */
        byte[] ReadByteArray(string fieldName);

        /**
         * <summary>Read byte array.</summary>
         * <returns>Byte array.</returns>
         */
        byte[] ReadByteArray();

        /**
         * <summary>Read named char value.</summary>
         * <param name="fieldName">Field name.</param>
         * <returns>Char value.</returns>
         */
        char ReadChar(string fieldName);

        /**
         * <summary>Read char value.</summary>
         * <returns>Char value.</returns>
         */
        char ReadChar();

        /**
         * <summary>Read named char array.</summary>
         * <returns>Char array.</returns>
         */
        char[] ReadCharArray(string fieldName);

        /**
         * <summary>Read char array.</summary>
         * <returns>Char array.</returns>
         */
        char[] ReadCharArray();

        /**
         * <summary>Read named short value.</summary>
         * <param name="fieldName">Field name.</param>
         * <returns>Short value.</returns>
         */
        short ReadShort(string fieldName);

        /**
         * <summary>Read short value.</summary>
         * <returns>Short value.</returns>
         */
        short ReadShort();

        /**
         * <summary>Read named short array.</summary>
         * <returns>Short array.</returns>
         */
        short[] ReadShortArray(string fieldName);

        /**
         * <summary>Read short array.</summary>
         * <returns>Short array.</returns>
         */
        short[] ReadShortArray();

        /**
         * <summary>Read named int value.</summary>
         * <param name="fieldName">Field name.</param>
         * <returns>Int value.</returns>
         */
        int ReadInt(string fieldName);

        /**
         * <summary>Read int value.</summary>
         * <returns>Int value.</returns>
         */
        int ReadInt();

        /**
         * <summary>Read named int array.</summary>
         * <param name="fieldName">Field name.</param>
         * <returns>Int array.</returns>
         */
        int[] ReadIntArray(string fieldName);

        /**
         * <summary>Read int array.</summary>
         * <returns>Int array.</returns>
         */
        int[] ReadIntArray();

        /**
         * <summary>Read named long value.</summary>
         * <param name="fieldName">Field name.</param>
         * <returns>Long value.</returns>
         */
        long ReadLong(string fieldName);

        /**
         * <summary>Read long value.</summary>
         * <returns>Long value.</returns>
         */
        long ReadLong();

        /**
         * <summary>Read named long array.</summary>
         * <param name="fieldName">Field name.</param>
         * <returns>Long array.</returns>
         */
        long[] ReadLongArray(string fieldName);

        /**
         * <summary>Read long array.</summary>
         * <returns>Long array.</returns>
         */
        long[] ReadLongArray();

        /**
         * <summary>Read named boolean value.</summary>
         * <param name="fieldName">Field name.</param>
         * <returns>Boolean value.</returns>
         */
        bool ReadBoolean(string fieldName);

        /**
         * <summary>Read boolean value.</summary>
         * <returns>Boolean value.</returns>
         */
        bool ReadBoolean();

        /**
         * <summary>Read named boolean array.</summary>
         * <param name="fieldName">Field name.</param>
         * <returns>Boolean array.</returns>
         */
        bool[] ReadBooleanArray(string fieldName);

        /**
         * <summary>Read boolean array.</summary>
         * <returns>Boolean array.</returns>
         */
        bool[] ReadBooleanArray();

        /**
         * <summary>Read named float value.</summary>
         * <param name="fieldName">Field name.</param>
         * <returns>Float value.</returns>
         */
        float ReadFloat(string fieldName);

        /**
         * <summary>Read float value.</summary>
         * <returns>Float value.</returns>
         */
        float ReadFloat();

        /**
         * <summary>Read named float array.</summary>
         * <param name="fieldName">Field name.</param>
         * <returns>Float array.</returns>
         */
        float[] ReadFloatArray(string fieldName);

        /**
         * <summary>Read float array.</summary>
         * <returns>Float array.</returns>
         */
        float[] ReadFloatArray();

        /**
         * <summary>Read named double value.</summary>
         * <param name="fieldName">Field name.</param>
         * <returns>Double value.</returns>
         */
        double ReadDouble(string fieldName);

        /**
         * <summary>Read double value.</summary>
         * <returns>Double value.</returns>
         */
        double ReadDouble();

        /**
         * <summary>Read named double array.</summary>
         * <param name="fieldName">Field name.</param>
         * <returns>Double array.</returns>
         */
        double[] ReadDoubleArray(string fieldName);

        /**
         * <summary>Read double array.</summary>
         * <returns>Double array.</returns>
         */
        double[] ReadDoubleArray();

        /**
         * <summary>Read named string value.</summary>
         * <param name="fieldName">Field name.</param>
         * <returns>String value.</returns>
         */
        string ReadString(string fieldName);

        /**
         * <summary>Read string value.</summary>
         * <returns>String value.</returns>
         */
        string ReadString();

        /**
         * <summary>Read named string array.</summary>
         * <param name="fieldName">Field name.</param>
         * <returns>String array.</returns>
         */
        string[] ReadStringArray(string fieldName);

        /**
         * <summary>Read string array.</summary>
         * <returns>String array.</returns>
         */
        string[] ReadStringArray();

        /**
         * <summary>Read named GUID value.</summary>
         * <param name="fieldName">Field name.</param>
         * <returns>GUID value.</returns>
         */
        Guid ReadGuid(string fieldName);

        /**
         * <summary>Read GUID value.</summary>
         * <returns>GUID value.</returns>
         */
        Guid ReadGuid();

        /**
         * <summary>Read named GUID array.</summary>
         * <param name="fieldName">Field name.</param>
         * <returns>GUID array.</returns>
         */
        Guid[] ReadGuidArray(string fieldName);

        /**
         * <summary>Read GUID array.</summary>
         * <returns>GUID array.</returns>
         */
        Guid[] ReadGuidArray();

        /**
         * <summary>Read named object.</summary>
         * <param name="fieldName">Field name.</param>
         * <returns>Object.</returns>
         */
        T ReadObject<T>(string fieldName);

        /**
         * <summary>Read object.</summary>
         * <returns>Object.</returns>
         */
        T ReadObject<T>();

        /**
         * <summary>Read named object array.</summary>
         * <param name="fieldName">Field name.</param>
         * <returns>Object array.</returns>
         */
        T[] ReadObjectArray<T>(string fieldName);

        /**
         * <summary>Read object array.</summary>
         * <returns>Object array.</returns>
         */
        T[] ReadObjectArray<T>();

        /**
         * <summary>Read named collection.</summary>
         * <param name="fieldName">Field name.</param>
         * <returns>Collection.</returns>
         */
        ICollection<T> ReadCollection<T>(string fieldName);

        /**
         * <summary>Read collection.</summary>
         * <returns>Collection.</returns>
         */
        ICollection<T> ReadCollection<T>();

        /**
         * <summary>Read named map.</summary>
         * <param name="fieldName">Field name.</param>
         * <returns>Map.</returns>
         */
        IDictionary<K, V> ReadMap<K, V>(string fieldName);

        /**
         * <summary>Read map.</summary>
         * <returns>Map.</returns>
         */
        IDictionary<K, V> ReadMap<K, V>();
    }
}