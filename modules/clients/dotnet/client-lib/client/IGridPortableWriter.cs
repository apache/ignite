/* @csharp.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

using System.Collections.Generic;

namespace GridGain.Client {
    using System;

    /// <summary>
    /// 
    /// </summary>
    public interface IGridPortableWriter {
        /// <summary>
        /// 
        /// </summary>
        /// <param name="fieldName"></param>
        /// <param name="val"></param>
        void WriteBoolean(string fieldName, bool val);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="fieldName"></param>
        /// <param name="val"></param>
        void WriteBooleanArray(string fieldName, bool[] val);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="fieldName"></param>
        /// <param name="val"></param>
        void WriteByte(string fieldName, byte val);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="fieldName"></param>
        /// <param name="val"></param>
        void WriteByteArray(string fieldName, byte[] val);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="fieldName"></param>
        /// <param name="val"></param>
        void WriteShort(string fieldName, short val);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="fieldName"></param>
        /// <param name="val"></param>
        void WriteShortArray(string fieldName, short[] val);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="fieldName"></param>
        /// <param name="val"></param>
        void WriteChar(string fieldName, char val);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="fieldName"></param>
        /// <param name="val"></param>
        void writeCharArray(string fieldName, char[] val);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="fieldName"></param>
        /// <param name="val"></param>
        void WriteInt(string fieldName, int val);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="fieldName"></param>
        /// <param name="val"></param>
        void WriteIntArray(string fieldName, int[] val);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="fieldName"></param>
        /// <param name="val"></param>
        void WriteLong(string fieldName, long val);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="fieldName"></param>
        /// <param name="val"></param>
        void WriteLongArray(string fieldName, long[] val);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="fieldName"></param>
        /// <param name="val"></param>
        void WriteFloat(string fieldName, float val);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="fieldName"></param>
        /// <param name="val"></param>
        void WriteFloatArray(string fieldName, float[] val);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="fieldName"></param>
        /// <param name="val"></param>
        void WriteDouble(string fieldName, double val);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="fieldName"></param>
        /// <param name="val"></param>
        void WriteDoubleArray(string fieldName, double[] val);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="fieldName"></param>
        /// <param name="val"></param>
        void WriteString(string fieldName, string val);
    
        /// <summary>
        /// 
        /// </summary>
        /// <param name="fieldName"></param>
        /// <param name="val"></param>
        void WriteObject<T>(string fieldName, T val) where T : IGridPortable;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="fieldName"></param>
        /// <param name="val"></param>
        void WriteMap<K, V>(string fieldName, IDictionary<K, V> val);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="fieldName"></param>
        /// <param name="val"></param>
        void WriteCollection<T>(string fieldName, ICollection<T> val);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="fieldName"></param>
        /// <param name="val"></param>
        void WriteGuid(string fieldName, Guid val);
    }
}