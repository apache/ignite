/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Impl.Portable.IO
{
    using System;
    using System.IO;
    using System.Text;

    /// <summary>
    /// Portable onheap stream.
    /// </summary>
    [CLSCompliant(false)]
    public unsafe class PortableHeapStream : PortableAbstractStream
    {
        /** Data array. */
        protected byte[] data;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="cap">Initial capacity.</param>
        public PortableHeapStream(int cap)
        {
            data = new byte[cap];
        }

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="data">Data array.</param>
        public PortableHeapStream(byte[] data)
        {
            this.data = data;
        }

        /** <inheritdoc /> */
        public override void WriteByte(byte val)
        {
            int pos0 = EnsureWriteCapacityAndShift(1);

            data[pos0] = val;
        }

        /** <inheritdoc /> */
        public override byte ReadByte()
        {
            int pos0 = EnsureReadCapacityAndShift(1);

            return data[pos0];
        }

        /** <inheritdoc /> */
        public override void WriteByteArray(byte[] val)
        {
            int pos0 = EnsureWriteCapacityAndShift(val.Length);

            fixed (byte* data0 = data)
            {
                WriteByteArray0(val, data0 + pos0);
            }
        }

        /** <inheritdoc /> */
        public override byte[] ReadByteArray(int cnt)
        {
            int pos0 = EnsureReadCapacityAndShift(cnt);

            fixed (byte* data0 = data)
            {
                return ReadByteArray0(cnt, data0 + pos0);
            }
        }

        /** <inheritdoc /> */
        public override void WriteBoolArray(bool[] val)
        {
            int pos0 = EnsureWriteCapacityAndShift(val.Length);

            fixed (byte* data0 = data)
            {
                WriteBoolArray0(val, data0 + pos0);
            }
        }

        /** <inheritdoc /> */
        public override bool[] ReadBoolArray(int cnt)
        {
            int pos0 = EnsureReadCapacityAndShift(cnt);

            fixed (byte* data0 = data)
            {
                return ReadBoolArray0(cnt, data0 + pos0);
            }
        }

        /** <inheritdoc /> */
        public override void WriteShort(short val)
        {
            int pos0 = EnsureWriteCapacityAndShift(2);

            fixed (byte* data0 = data)
            {
                WriteShort0(val, data0 + pos0);
            }
        }

        /** <inheritdoc /> */
        public override short ReadShort()
        {
            int pos0 = EnsureReadCapacityAndShift(2);

            fixed (byte* data0 = data)
            {
                return ReadShort0(data0 + pos0);
            }
        }

        /** <inheritdoc /> */
        public override void WriteShortArray(short[] val)
        {
            int cnt = val.Length << 1;

            int pos0 = EnsureWriteCapacityAndShift(cnt);

            fixed (byte* data0 = data)
            {
                WriteShortArray0(val, data0 + pos0, cnt);
            }
        }

        /** <inheritdoc /> */
        public override short[] ReadShortArray(int cnt)
        {
            int cnt0 = cnt << 1;

            int pos0 = EnsureReadCapacityAndShift(cnt0);

            fixed (byte* data0 = data)
            {
                return ReadShortArray0(cnt, data0 + pos0, cnt0);
            }
        }

        /** <inheritdoc /> */
        public override void WriteCharArray(char[] val)
        {
            int cnt = val.Length << 1;

            int pos0 = EnsureWriteCapacityAndShift(cnt);

            fixed (byte* data0 = data)
            {
                WriteCharArray0(val, data0 + pos0, cnt);
            }
        }

        /** <inheritdoc /> */
        public override char[] ReadCharArray(int cnt)
        {
            int cnt0 = cnt << 1;

            int pos0 = EnsureReadCapacityAndShift(cnt0);

            fixed (byte* data0 = data)
            {
                return ReadCharArray0(cnt, data0 + pos0, cnt0);
            }
        }

        /** <inheritdoc /> */
        public override void WriteInt(int val)
        {
            int pos0 = EnsureWriteCapacityAndShift(4);

            fixed (byte* data0 = data)
            {
                WriteInt0(val, data0 + pos0);
            }
        }

        /** <inheritdoc /> */
        public override void WriteInt(int writePos, int val)
        {
            EnsureWriteCapacity(writePos + 4);

            fixed (byte* data0 = data)
            {
                WriteInt0(val, data0 + writePos);
            }
        }

        /** <inheritdoc /> */
        public override int ReadInt()
        {
            int pos0 = EnsureReadCapacityAndShift(4);

            fixed (byte* data0 = data)
            {
                return ReadInt0(data0 + pos0);
            }
        }

        /** <inheritdoc /> */
        public override void WriteIntArray(int[] val)
        {
            int cnt = val.Length << 2;

            int pos0 = EnsureWriteCapacityAndShift(cnt);

            fixed (byte* data0 = data)
            {
                WriteIntArray0(val, data0 + pos0, cnt);
            }
        }

        /** <inheritdoc /> */
        public override int[] ReadIntArray(int cnt)
        {
            int cnt0 = cnt << 2;

            int pos0 = EnsureReadCapacityAndShift(cnt0);

            fixed (byte* data0 = data)
            {
                return ReadIntArray0(cnt, data0 + pos0, cnt0);
            }
        }

        /** <inheritdoc /> */
        public override void WriteFloatArray(float[] val)
        {
            int cnt = val.Length << 2;

            int pos0 = EnsureWriteCapacityAndShift(cnt);

            fixed (byte* data0 = data)
            {
                WriteFloatArray0(val, data0 + pos0, cnt);
            }
        }

        /** <inheritdoc /> */
        public override float[] ReadFloatArray(int cnt)
        {
            int cnt0 = cnt << 2;

            int pos0 = EnsureReadCapacityAndShift(cnt0);

            fixed (byte* data0 = data)
            {
                return ReadFloatArray0(cnt, data0 + pos0, cnt0);
            }
        }

        /** <inheritdoc /> */
        public override void WriteLong(long val)
        {
            int pos0 = EnsureWriteCapacityAndShift(8);

            fixed (byte* data0 = data)
            {
                WriteLong0(val, data0 + pos0);
            }
        }

        /** <inheritdoc /> */
        public override long ReadLong()
        {
            int pos0 = EnsureReadCapacityAndShift(8);

            fixed (byte* data0 = data)
            {
                return ReadLong0(data0 + pos0);
            }
        }

        /** <inheritdoc /> */
        public override void WriteLongArray(long[] val)
        {
            int cnt = val.Length << 3;

            int pos0 = EnsureWriteCapacityAndShift(cnt);

            fixed (byte* data0 = data)
            {
                WriteLongArray0(val, data0 + pos0, cnt);
            }
        }

        /** <inheritdoc /> */
        public override long[] ReadLongArray(int cnt)
        {
            int cnt0 = cnt << 3;

            int pos0 = EnsureReadCapacityAndShift(cnt0);

            fixed (byte* data0 = data)
            {
                return ReadLongArray0(cnt, data0 + pos0, cnt0);
            }
        }

        /** <inheritdoc /> */
        public override void WriteDoubleArray(double[] val)
        {
            int cnt = val.Length << 3;

            int pos0 = EnsureWriteCapacityAndShift(cnt);

            fixed (byte* data0 = data)
            {
                WriteDoubleArray0(val, data0 + pos0, cnt);
            }
        }

        /** <inheritdoc /> */
        public override double[] ReadDoubleArray(int cnt)
        {
            int cnt0 = cnt << 3;

            int pos0 = EnsureReadCapacityAndShift(cnt0);

            fixed (byte* data0 = data)
            {
                return ReadDoubleArray0(cnt, data0 + pos0, cnt0);
            }
        }

        /** <inheritdoc /> */
        public override int WriteString(char* chars, int charCnt, int byteCnt, Encoding encoding)
        {
            int pos0 = EnsureWriteCapacityAndShift(byteCnt);

            int written;

            fixed (byte* data0 = data)
            {
                written = WriteString0(chars, charCnt, byteCnt, encoding, data0 + pos0);
            }

            return written;
        }

        /** <inheritdoc /> */
        public override void Write(byte* src, int cnt)
        {
            EnsureWriteCapacity(pos + cnt);

            fixed (byte* data0 = data)
            {
                WriteInternal(src, cnt, data0);
            }

            ShiftWrite(cnt);
        }

        /** <inheritdoc /> */
        public override void Read(byte* dest, int cnt)
        {
            fixed (byte* data0 = data)
            {
                ReadInternal(dest, cnt, data0);
            }
        }

        /** <inheritdoc /> */
        public override int Remaining()
        {
            return data.Length - pos;
        }

        /** <inheritdoc /> */
        public override byte[] Array()
        {
            return data;
        }

        /** <inheritdoc /> */
        public override byte[] ArrayCopy()
        {
            byte[] copy = new byte[pos];

            Buffer.BlockCopy(data, 0, copy, 0, pos);

            return copy;
        }

        /** <inheritdoc /> */
        public override bool IsSameArray(byte[] arr)
        {
            return data == arr;
        }

        /** <inheritdoc /> */
        protected override void Dispose(bool disposing)
        {
            // No-op.
        }

        /// <summary>
        /// Internal array.
        /// </summary>
        internal byte[] InternalArray
        {
            get { return data; }
        }

        /** <inheritdoc /> */
        protected override void EnsureWriteCapacity(int cnt)
        {
            if (cnt > data.Length)
            {
                int newCap = Capacity(data.Length, cnt);

                byte[] data0 = new byte[newCap];

                // Copy the whole initial array length here because it can be changed
                // from Java without position adjusting.
                Buffer.BlockCopy(data, 0, data0, 0, data.Length);

                data = data0;
            }
        }

        /** <inheritdoc /> */
        protected override void EnsureReadCapacity(int cnt)
        {
            if (data.Length - pos < cnt)
                throw new EndOfStreamException("Not enough data in stream [expected=" + cnt +
                    ", remaining=" + (data.Length - pos) + ']');
        }
    }
}
