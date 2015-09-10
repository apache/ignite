/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

namespace Apache.Ignite.Core.Impl.Portable.IO
{
    using System;
    using System.IO;
    using System.Text;

    /// <summary>
    /// Portable onheap stream.
    /// </summary>
    internal unsafe class PortableHeapStream : PortableAbstractStream
    {
        /** Data array. */
        protected byte[] Data;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="cap">Initial capacity.</param>
        public PortableHeapStream(int cap)
        {
            Data = new byte[cap];
        }

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="data">Data array.</param>
        public PortableHeapStream(byte[] data)
        {
            Data = data;
        }

        /** <inheritdoc /> */
        public override void WriteByte(byte val)
        {
            int pos0 = EnsureWriteCapacityAndShift(1);

            Data[pos0] = val;
        }

        /** <inheritdoc /> */
        public override byte ReadByte()
        {
            int pos0 = EnsureReadCapacityAndShift(1);

            return Data[pos0];
        }

        /** <inheritdoc /> */
        public override void WriteByteArray(byte[] val)
        {
            int pos0 = EnsureWriteCapacityAndShift(val.Length);

            fixed (byte* data0 = Data)
            {
                WriteByteArray0(val, data0 + pos0);
            }
        }

        /** <inheritdoc /> */
        public override byte[] ReadByteArray(int cnt)
        {
            int pos0 = EnsureReadCapacityAndShift(cnt);

            fixed (byte* data0 = Data)
            {
                return ReadByteArray0(cnt, data0 + pos0);
            }
        }

        /** <inheritdoc /> */
        public override void WriteBoolArray(bool[] val)
        {
            int pos0 = EnsureWriteCapacityAndShift(val.Length);

            fixed (byte* data0 = Data)
            {
                WriteBoolArray0(val, data0 + pos0);
            }
        }

        /** <inheritdoc /> */
        public override bool[] ReadBoolArray(int cnt)
        {
            int pos0 = EnsureReadCapacityAndShift(cnt);

            fixed (byte* data0 = Data)
            {
                return ReadBoolArray0(cnt, data0 + pos0);
            }
        }

        /** <inheritdoc /> */
        public override void WriteShort(short val)
        {
            int pos0 = EnsureWriteCapacityAndShift(2);

            fixed (byte* data0 = Data)
            {
                WriteShort0(val, data0 + pos0);
            }
        }

        /** <inheritdoc /> */
        public override short ReadShort()
        {
            int pos0 = EnsureReadCapacityAndShift(2);

            fixed (byte* data0 = Data)
            {
                return ReadShort0(data0 + pos0);
            }
        }

        /** <inheritdoc /> */
        public override void WriteShortArray(short[] val)
        {
            int cnt = val.Length << 1;

            int pos0 = EnsureWriteCapacityAndShift(cnt);

            fixed (byte* data0 = Data)
            {
                WriteShortArray0(val, data0 + pos0, cnt);
            }
        }

        /** <inheritdoc /> */
        public override short[] ReadShortArray(int cnt)
        {
            int cnt0 = cnt << 1;

            int pos0 = EnsureReadCapacityAndShift(cnt0);

            fixed (byte* data0 = Data)
            {
                return ReadShortArray0(cnt, data0 + pos0, cnt0);
            }
        }

        /** <inheritdoc /> */
        public override void WriteCharArray(char[] val)
        {
            int cnt = val.Length << 1;

            int pos0 = EnsureWriteCapacityAndShift(cnt);

            fixed (byte* data0 = Data)
            {
                WriteCharArray0(val, data0 + pos0, cnt);
            }
        }

        /** <inheritdoc /> */
        public override char[] ReadCharArray(int cnt)
        {
            int cnt0 = cnt << 1;

            int pos0 = EnsureReadCapacityAndShift(cnt0);

            fixed (byte* data0 = Data)
            {
                return ReadCharArray0(cnt, data0 + pos0, cnt0);
            }
        }

        /** <inheritdoc /> */
        public override void WriteInt(int val)
        {
            int pos0 = EnsureWriteCapacityAndShift(4);

            fixed (byte* data0 = Data)
            {
                WriteInt0(val, data0 + pos0);
            }
        }

        /** <inheritdoc /> */
        public override void WriteInt(int writePos, int val)
        {
            EnsureWriteCapacity(writePos + 4);

            fixed (byte* data0 = Data)
            {
                WriteInt0(val, data0 + writePos);
            }
        }

        /** <inheritdoc /> */
        public override int ReadInt()
        {
            int pos0 = EnsureReadCapacityAndShift(4);

            fixed (byte* data0 = Data)
            {
                return ReadInt0(data0 + pos0);
            }
        }

        /** <inheritdoc /> */
        public override void WriteIntArray(int[] val)
        {
            int cnt = val.Length << 2;

            int pos0 = EnsureWriteCapacityAndShift(cnt);

            fixed (byte* data0 = Data)
            {
                WriteIntArray0(val, data0 + pos0, cnt);
            }
        }

        /** <inheritdoc /> */
        public override int[] ReadIntArray(int cnt)
        {
            int cnt0 = cnt << 2;

            int pos0 = EnsureReadCapacityAndShift(cnt0);

            fixed (byte* data0 = Data)
            {
                return ReadIntArray0(cnt, data0 + pos0, cnt0);
            }
        }

        /** <inheritdoc /> */
        public override void WriteFloatArray(float[] val)
        {
            int cnt = val.Length << 2;

            int pos0 = EnsureWriteCapacityAndShift(cnt);

            fixed (byte* data0 = Data)
            {
                WriteFloatArray0(val, data0 + pos0, cnt);
            }
        }

        /** <inheritdoc /> */
        public override float[] ReadFloatArray(int cnt)
        {
            int cnt0 = cnt << 2;

            int pos0 = EnsureReadCapacityAndShift(cnt0);

            fixed (byte* data0 = Data)
            {
                return ReadFloatArray0(cnt, data0 + pos0, cnt0);
            }
        }

        /** <inheritdoc /> */
        public override void WriteLong(long val)
        {
            int pos0 = EnsureWriteCapacityAndShift(8);

            fixed (byte* data0 = Data)
            {
                WriteLong0(val, data0 + pos0);
            }
        }

        /** <inheritdoc /> */
        public override long ReadLong()
        {
            int pos0 = EnsureReadCapacityAndShift(8);

            fixed (byte* data0 = Data)
            {
                return ReadLong0(data0 + pos0);
            }
        }

        /** <inheritdoc /> */
        public override void WriteLongArray(long[] val)
        {
            int cnt = val.Length << 3;

            int pos0 = EnsureWriteCapacityAndShift(cnt);

            fixed (byte* data0 = Data)
            {
                WriteLongArray0(val, data0 + pos0, cnt);
            }
        }

        /** <inheritdoc /> */
        public override long[] ReadLongArray(int cnt)
        {
            int cnt0 = cnt << 3;

            int pos0 = EnsureReadCapacityAndShift(cnt0);

            fixed (byte* data0 = Data)
            {
                return ReadLongArray0(cnt, data0 + pos0, cnt0);
            }
        }

        /** <inheritdoc /> */
        public override void WriteDoubleArray(double[] val)
        {
            int cnt = val.Length << 3;

            int pos0 = EnsureWriteCapacityAndShift(cnt);

            fixed (byte* data0 = Data)
            {
                WriteDoubleArray0(val, data0 + pos0, cnt);
            }
        }

        /** <inheritdoc /> */
        public override double[] ReadDoubleArray(int cnt)
        {
            int cnt0 = cnt << 3;

            int pos0 = EnsureReadCapacityAndShift(cnt0);

            fixed (byte* data0 = Data)
            {
                return ReadDoubleArray0(cnt, data0 + pos0, cnt0);
            }
        }

        /** <inheritdoc /> */
        public override int WriteString(char* chars, int charCnt, int byteCnt, Encoding encoding)
        {
            int pos0 = EnsureWriteCapacityAndShift(byteCnt);

            int written;

            fixed (byte* data0 = Data)
            {
                written = WriteString0(chars, charCnt, byteCnt, encoding, data0 + pos0);
            }

            return written;
        }

        /** <inheritdoc /> */
        public override void Write(byte* src, int cnt)
        {
            EnsureWriteCapacity(Pos + cnt);

            fixed (byte* data0 = Data)
            {
                WriteInternal(src, cnt, data0);
            }

            ShiftWrite(cnt);
        }

        /** <inheritdoc /> */
        public override void Read(byte* dest, int cnt)
        {
            fixed (byte* data0 = Data)
            {
                ReadInternal(dest, cnt, data0);
            }
        }

        /** <inheritdoc /> */
        public override int Remaining()
        {
            return Data.Length - Pos;
        }

        /** <inheritdoc /> */
        public override byte[] Array()
        {
            return Data;
        }

        /** <inheritdoc /> */
        public override byte[] ArrayCopy()
        {
            byte[] copy = new byte[Pos];

            Buffer.BlockCopy(Data, 0, copy, 0, Pos);

            return copy;
        }

        /** <inheritdoc /> */
        public override bool IsSameArray(byte[] arr)
        {
            return Data == arr;
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
            get { return Data; }
        }

        /** <inheritdoc /> */
        protected override void EnsureWriteCapacity(int cnt)
        {
            if (cnt > Data.Length)
            {
                int newCap = Capacity(Data.Length, cnt);

                byte[] data0 = new byte[newCap];

                // Copy the whole initial array length here because it can be changed
                // from Java without position adjusting.
                Buffer.BlockCopy(Data, 0, data0, 0, Data.Length);

                Data = data0;
            }
        }

        /** <inheritdoc /> */
        protected override void EnsureReadCapacity(int cnt)
        {
            if (Data.Length - Pos < cnt)
                throw new EndOfStreamException("Not enough data in stream [expected=" + cnt +
                    ", remaining=" + (Data.Length - Pos) + ']');
        }
    }
}
