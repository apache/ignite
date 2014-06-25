/* @csharp.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Client.Portable {
    using System;
    using System.Collections;
    using System.Collections.Generic;

    /**
     * <summary>Writer for portable objects.</summary>
     */
    public interface IGridClientPortableWriter {
        /**
         * <summary>Write named byte value.</summary>
         * <param name="fieldName">Field name.</param>
         * <param name="val">Byte value.</param>
         */
        void WriteByte(string fieldName, byte val);

        /**
         * <summary>Write named byte array.</summary>
         * <param name="fieldName">Field name.</param>
         * <param name="val">Byte array.</param>
         */
        void WriteByteArray(string fieldName, byte[] val);

        /**
         * <summary>Write named char value.</summary>
         * <param name="fieldName">Field name.</param>
         * <param name="val">Char value.</param>
         */
        void WriteChar(string fieldName, char val);

        /**
         * <summary>Write named char array.</summary>
         * <param name="fieldName">Field name.</param>
         * <param name="val">Char array.</param>
         */
        void WriteCharArray(string fieldName, char[] val);

        /**
         * <summary>Write named short value.</summary>
         * <param name="fieldName">Field name.</param>
         * <param name="val">Short value.</param>
         */
        void WriteShort(string fieldName, short val);

        /**
         * <summary>Write named short array.</summary>
         * <param name="fieldName">Field name.</param>
         * <param name="val">Short array.</param>
         */
        void WriteShortArray(string fieldName, short[] val);

        /**
         * <summary>Write named int value.</summary>
         * <param name="fieldName">Field name.</param>
         * <param name="val">Int value.</param>
         */
        void WriteInt(string fieldName, int val);

        /**
         * <summary>Write named int array.</summary>
         * <param name="fieldName">Field name.</param>
         * <param name="val">Int array.</param>
         */
        void WriteIntArray(string fieldName, int[] val);

        /**
         * <summary>Write named long value.</summary>
         * <param name="fieldName">Field name.</param>
         * <param name="val">Long value.</param>
         */
        void WriteLong(string fieldName, long val);

        /**
         * <summary>Write named long array.</summary>
         * <param name="fieldName">Field name.</param>
         * <param name="val">Long array.</param>
         */
        void WriteLongArray(string fieldName, long[] val);

        /**
         * <summary>Write named boolean value.</summary>
         * <param name="fieldName">Field name.</param>
         * <param name="val">Boolean value.</param>
         */
        void WriteBoolean(string fieldName, bool val);

        /**
         * <summary>Write named boolean array.</summary>
         * <param name="fieldName">Field name.</param>
         * <param name="val">Boolean array.</param>
         */
        void WriteBooleanArray(string fieldName, bool[] val);

        /**
         * <summary>Write named float value.</summary>
         * <param name="fieldName">Field name.</param>
         * <param name="val">Float value.</param>
         */
        void WriteFloat(string fieldName, float val);

        /**
         * <summary>Write named float array.</summary>
         * <param name="fieldName">Field name.</param>
         * <param name="val">Float array.</param>
         */
        void WriteFloatArray(string fieldName, float[] val);

        /**
         * <summary>Write named double value.</summary>
         * <param name="fieldName">Field name.</param>
         * <param name="val">Double value.</param>
         */
        void WriteDouble(string fieldName, double val);

        /**
         * <summary>Write named double array.</summary>
         * <param name="fieldName">Field name.</param>
         * <param name="val">Double array.</param>
         */
        void WriteDoubleArray(string fieldName, double[] val);

        /**
         * <summary>Write named string value.</summary>
         * <param name="fieldName">Field name.</param>
         * <param name="val">String value.</param>
         */
        void WriteString(string fieldName, string val);

        /**
         * <summary>Write named string array.</summary>
         * <param name="fieldName">Field name.</param>
         * <param name="val">String array.</param>
         */
        void WriteStringArray(string fieldName, string[] val);

        /**
         * <summary>Write named GUID value.</summary>
         * <param name="fieldName">Field name.</param>
         * <param name="val">GUID value.</param>
         */
        void WriteGuid(string fieldName, Guid? val);

        /**
         * <summary>Write named GUID array.</summary>
         * <param name="fieldName">Field name.</param>
         * <param name="val">GUID array.</param>
         */
        void WriteGuidArray(string fieldName, Guid?[] val);

        /**
         * <summary>Write named object value.</summary>
         * <param name="fieldName">Field name.</param>
         * <param name="val">Object value.</param>
         */
        void WriteObject<T>(string fieldName, T val);

        /**
         * <summary>Write named object array.</summary>
         * <param name="fieldName">Field name.</param>
         * <param name="val">Object array.</param>
         */
        void WriteObjectArray<T>(string fieldName, T[] val);

        /**
         * <summary>Write named collection.</summary>
         * <param name="fieldName">Field name.</param>
         * <param name="val">Collection.</param>
         */
        void WriteCollection(string fieldName, ICollection val);

        /**
         * <summary>Write named generic collection.</summary>
         * <param name="fieldName">Field name.</param>
         * <param name="val">Collection.</param>
         */
        void WriteGenericCollection<T>(string fieldName, ICollection<T> val);

        /**
         * <summary>Write named dictionary.</summary>
         * <param name="fieldName">Field name.</param>
         * <param name="val">Dictionary.</param>
         */
        void WriteDictionary(string fieldName, IDictionary val);

        /**
         * <summary>Write named generic dictionary.</summary>
         * <param name="fieldName">Field name.</param>
         * <param name="val">Dictionary.</param>
         */
        void WriteGenericDictionary<K, V>(string fieldName, IDictionary<K, V> val);
        
        /**
         * <summary>Get raw writer.</summary>
         * <returns>Raw writer.</returns>
         */
        IGridClientPortableRawWriter RawWriter();
    }
}