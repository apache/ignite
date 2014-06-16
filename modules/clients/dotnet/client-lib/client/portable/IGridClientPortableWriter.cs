/* @csharp.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Client.Portable {
    using System;
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
        void WriteByte(string fieldName, sbyte val);

        /**
         * <summary>Write byte value.</summary>
         * <param name="val">Byte value.</param>
         */
        void WriteByte(sbyte val);

        /**
         * <summary>Write named byte array.</summary>
         * <param name="fieldName">Field name.</param>
         * <param name="val">Byte array.</param>
         */
        void WriteByteArray(string fieldName, byte[] val);

        /**
         * <summary>Write byte array.</summary>
         * <param name="val">Byte array.</param>
         */
        void WriteByteArray(byte[] val);

        /**
         * <summary>Write named char value.</summary>
         * <param name="fieldName">Field name.</param>
         * <param name="val">Char value.</param>
         */
        void WriteChar(string fieldName, char val);

        /**
         * <summary>Write char value.</summary>
         * <param name="val">Char value.</param>
         */
        void WriteChar(char val);

        /**
         * <summary>Write named char array.</summary>
         * <param name="fieldName">Field name.</param>
         * <param name="val">Char array.</param>
         */
        void WriteCharArray(string fieldName, char[] val);

        /**
         * <summary>Write char array.</summary>
         * <param name="val">Char array.</param>
         */
        void WriteCharArray(char[] val);

        /**
         * <summary>Write named short value.</summary>
         * <param name="fieldName">Field name.</param>
         * <param name="val">Short value.</param>
         */
        void WriteShort(string fieldName, short val);

        /**
         * <summary>Write short value.</summary>
         * <param name="val">Short value.</param>
         */
        void WriteShort(short val);

        /**
         * <summary>Write named short array.</summary>
         * <param name="fieldName">Field name.</param>
         * <param name="val">Short array.</param>
         */
        void WriteShortArray(string fieldName, short[] val);

        /**
         * <summary>Write short array.</summary>
         * <param name="val">Short array.</param>
         */
        void WriteShortArray(short[] val);

        /**
         * <summary>Write named int value.</summary>
         * <param name="fieldName">Field name.</param>
         * <param name="val">Int value.</param>
         */
        void WriteInt(string fieldName, int val);

        /**
         * <summary>Write int value.</summary>
         * <param name="val">Int value.</param>
         */
        void WriteInt(int val);

        /**
         * <summary>Write named int array.</summary>
         * <param name="fieldName">Field name.</param>
         * <param name="val">Int array.</param>
         */
        void WriteIntArray(string fieldName, int[] val);

        /**
         * <summary>Write int array.</summary>
         * <param name="val">Int array.</param>
         */
        void WriteIntArray(int[] val);

        /**
         * <summary>Write named long value.</summary>
         * <param name="fieldName">Field name.</param>
         * <param name="val">Long value.</param>
         */
        void WriteLong(string fieldName, long val);

        /**
         * <summary>Write long value.</summary>
         * <param name="val">Long value.</param>
         */
        void WriteLong(long val);

        /**
         * <summary>Write named long array.</summary>
         * <param name="fieldName">Field name.</param>
         * <param name="val">Long array.</param>
         */
        void WriteLongArray(string fieldName, long[] val);

        /**
         * <summary>Write long array.</summary>
         * <param name="val">Long array.</param>
         */
        void WriteLongArray(long[] val);

        /**
         * <summary>Write named boolean value.</summary>
         * <param name="fieldName">Field name.</param>
         * <param name="val">Boolean value.</param>
         */
        void WriteBoolean(string fieldName, bool val);

        /**
         * <summary>Write boolean value.</summary>
         * <param name="val">Boolean value.</param>
         */
        void WriteBoolean(bool val);

        /**
         * <summary>Write named boolean array.</summary>
         * <param name="fieldName">Field name.</param>
         * <param name="val">Boolean array.</param>
         */
        void WriteBooleanArray(string fieldName, bool[] val);

        /**
         * <summary>Write boolean array.</summary>
         * <param name="val">Boolean array.</param>
         */
        void WriteBooleanArray(bool[] val);

        /**
         * <summary>Write named float value.</summary>
         * <param name="fieldName">Field name.</param>
         * <param name="val">Float value.</param>
         */
        void WriteFloat(string fieldName, float val);

        /**
         * <summary>Write float value.</summary>
         * <param name="val">Float value.</param>
         */
        void WriteFloat(float val);

        /**
         * <summary>Write named float array.</summary>
         * <param name="fieldName">Field name.</param>
         * <param name="val">Float array.</param>
         */
        void WriteFloatArray(string fieldName, float[] val);

        /**
         * <summary>Write float array.</summary>
         * <param name="val">Float array.</param>
         */
        void WriteFloatArray(float[] val);

        /**
         * <summary>Write named double value.</summary>
         * <param name="fieldName">Field name.</param>
         * <param name="val">Double value.</param>
         */
        void WriteDouble(string fieldName, double val);

        /**
         * <summary>Write double value.</summary>
         * <param name="val">Double value.</param>
         */
        void WriteDouble(double val);

        /**
         * <summary>Write named double array.</summary>
         * <param name="fieldName">Field name.</param>
         * <param name="val">Double array.</param>
         */
        void WriteDoubleArray(string fieldName, double[] val);

        /**
         * <summary>Write double array.</summary>
         * <param name="val">Double array.</param>
         */
        void WriteDoubleArray(double[] val);

        /**
         * <summary>Write named string value.</summary>
         * <param name="fieldName">Field name.</param>
         * <param name="val">String value.</param>
         */
        void WriteString(string fieldName, string val);

        /**
         * <summary>Write string value.</summary>
         * <param name="val">String value.</param>
         */
        void WriteString(string val);

        /**
         * <summary>Write named string array.</summary>
         * <param name="fieldName">Field name.</param>
         * <param name="val">String array.</param>
         */
        void WriteStringArray(string fieldName, string[] val);

        /**
         * <summary>Write string array.</summary>
         * <param name="val">String array.</param>
         */
        void WriteStringArray(string[] val);

        /**
         * <summary>Write named GUID value.</summary>
         * <param name="fieldName">Field name.</param>
         * <param name="val">GUID value.</param>
         */
        void WriteGuid(string fieldName, Guid val);

        /**
         * <summary>Write GUID value.</summary>
         * <param name="val">GUID value.</param>
         */
        void WriteGuid(Guid val);

        /**
         * <summary>Write named GUID array.</summary>
         * <param name="fieldName">Field name.</param>
         * <param name="val">GUID array.</param>
         */
        void WriteGuidArray(string fieldName, Guid[] val);

        /**
         * <summary>Write GUID array.</summary>
         * <param name="val">GUID array.</param>
         */
        void WriteGuidArray(Guid[] val);

        /**
         * <summary>Write named object value.</summary>
         * <param name="fieldName">Field name.</param>
         * <param name="val">Object value.</param>
         */
        void WriteObject<T>(string fieldName, T val);

        /**
         * <summary>Write object value.</summary>
         * <param name="val">Object value.</param>
         */
        void WriteObject<T>(T val);

        /**
         * <summary>Write named object array.</summary>
         * <param name="fieldName">Field name.</param>
         * <param name="val">Object array.</param>
         */
        void WriteObjectArray<T>(string fieldName, T[] val);

        /**
         * <summary>Write object array.</summary>
         * <param name="val">Object array.</param>
         */
        void WriteObjectArray<T>(T[] val);

        /**
         * <summary>Write named collection.</summary>
         * <param name="fieldName">Field name.</param>
         * <param name="val">Collection.</param>
         */
        void WriteCollection<T>(string fieldName, ICollection<T> val);

        /**
         * <summary>Write collection.</summary>
         * <param name="val">Collection.</param>
         */
        void WriteCollection<T>(ICollection<T> val);

        /**
         * <summary>Write named map.</summary>
         * <param name="fieldName">Field name.</param>
         * <param name="val">Map.</param>
         */
        void WriteMap<K, V>(string fieldName, IDictionary<K, V> val);

        /**
         * <summary>Write map.</summary>
         * <param name="val">Map.</param>
         */
        void WriteMap<K, V>(IDictionary<K, V> val);
    }
}