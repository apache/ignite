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

    /**
     * Byte array output.
     */ 
    class GridClientPortableByteArrayMarshallerOutput : IGridClientPortableMarshallerOutput
    {
        /** Data. */
        private byte[] data;

        /** Position. */
        private int pos;

        /**
         * <summary>Initializes output.</summary>
         * <param name="len">Data length.</param>
         */
        public void initialize(int len)
        {
            data = new byte[len];
        }

        /** <inheritdoc /> */
        public void writeByte(byte val)
        {
            data[pos++] = val;
        }

        /** <inheritdoc /> */
        public void writeBytes(byte[] val)
        {
            Array.Copy(val, 0, data, pos, val.Length);

            pos += val.Length;
        }

        /** <inheritdoc /> */
        public void close()
        {
            // No-op.
        }

        /**
         * <summary>Get underlying data.</summary>
         * <returns>Data.</returns>
         */ 
        public byte[] Data()
        {
            return data;
        }
    }
}
