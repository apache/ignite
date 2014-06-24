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
    using System.Collections;
    using System.Collections.Generic;

    /**
     * <summary>Raw writer for portable objects.</summary>
     */
    public interface IGridClientPortableRawWriter
    {
        /**
         * <summary>Write byte value.</summary>
         * <param name="val">Byte value.</param>
         */
        void WriteByte(byte val);

        /**
         * <summary>Write byte array.</summary>
         * <param name="val">Byte array.</param>
         */
        void WriteByteArray(byte[] val);

        /**
         * <summary>Write char value.</summary>
         * <param name="val">Char value.</param>
         */
        void WriteChar(char val);

        /**
         * <summary>Write char array.</summary>
         * <param name="val">Char array.</param>
         */
        void WriteCharArray(char[] val);

        /**
         * <summary>Write short value.</summary>
         * <param name="val">Short value.</param>
         */
        void WriteShort(short val);

        /**
         * <summary>Write short array.</summary>
         * <param name="val">Short array.</param>
         */
        void WriteShortArray(short[] val);

        /**
         * <summary>Write int value.</summary>
         * <param name="val">Int value.</param>
         */
        void WriteInt(int val);

        /**
         * <summary>Write int array.</summary>
         * <param name="val">Int array.</param>
         */
        void WriteIntArray(int[] val);

        /**
         * <summary>Write long value.</summary>
         * <param name="val">Long value.</param>
         */
        void WriteLong(long val);

        /**
         * <summary>Write long array.</summary>
         * <param name="val">Long array.</param>
         */
        void WriteLongArray(long[] val);

        /**
         * <summary>Write boolean value.</summary>
         * <param name="val">Boolean value.</param>
         */
        void WriteBoolean(bool val);

        /**
         * <summary>Write boolean array.</summary>
         * <param name="val">Boolean array.</param>
         */
        void WriteBooleanArray(bool[] val);

        /**
         * <summary>Write float value.</summary>
         * <param name="val">Float value.</param>
         */
        void WriteFloat(float val);

        /**
         * <summary>Write float array.</summary>
         * <param name="val">Float array.</param>
         */
        void WriteFloatArray(float[] val);

        /**
         * <summary>Write double value.</summary>
         * <param name="val">Double value.</param>
         */
        void WriteDouble(double val);

        /**
         * <summary>Write double array.</summary>
         * <param name="val">Double array.</param>
         */
        void WriteDoubleArray(double[] val);

        /**
         * <summary>Write string value.</summary>
         * <param name="val">String value.</param>
         */
        void WriteString(string val);

        /**
         * <summary>Write string array.</summary>
         * <param name="val">String array.</param>
         */
        void WriteStringArray(string[] val);

        /**
         * <summary>Write GUID value.</summary>
         * <param name="val">GUID value.</param>
         */
        void WriteGuid(Guid? val);

        /**
         * <summary>Write GUID array.</summary>
         * <param name="val">GUID array.</param>
         */
        void WriteGuidArray(Guid?[] val);
        
        /**
         * <summary>Write object value.</summary>
         * <param name="val">Object value.</param>
         */
        void WriteObject<T>(T val);

        /**
         * <summary>Write object array.</summary>
         * <param name="val">Object array.</param>
         */
        void WriteObjectArray<T>(T[] val);

        /**
         * <summary>Write collection.</summary>
         * <param name="val">Collection.</param>
         */
        void WriteCollection(ICollection val);

        /**
         * <summary>Write generic collection.</summary>
         * <param name="val">Collection.</param>
         */
        void WriteGenericCollection<T>(ICollection<T> val);

        /**
         * <summary>Write dictionary.</summary>
         * <param name="val">Dictionary.</param>
         */
        void WriteDictionary(IDictionary val);

        /**
         * <summary>Write generic dictionary.</summary>
         * <param name="val">Dictionary.</param>
         */
        void WriteGenericCollection<K, V>(IDictionary<K, V> val);
    }
}
