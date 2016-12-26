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

namespace Apache.Ignite.Core.Impl.Binary.IO
{
    using System;
    using System.Diagnostics;
    using System.Diagnostics.CodeAnalysis;
    using System.IO;
    using System.Text;

    /// <summary>
    /// Binary onheap stream.
    /// </summary>
    internal unsafe class BinaryHeapStream : BinaryStreamBase
    {
        /** Data array. */
        private byte[] _data;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="cap">Initial capacity.</param>
        public BinaryHeapStream(int cap)
        {
            Debug.Assert(cap >= 0);

            _data = new byte[cap];
        }

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="data">Data array.</param>
        public BinaryHeapStream(byte[] data)
        {
            Debug.Assert(data != null);

            _data = data;
        }

        /** <inheritdoc /> */
        public override void WriteByte(byte val)
        {
            int pos0 = EnsureWriteCapacityAndShift(1);

            _data[pos0] = val;
        }

        /** <inheritdoc /> */
        public override byte ReadByte()
        {
            int pos0 = EnsureReadCapacityAndShift(1);

            return _data[pos0];
        }

        /** <inheritdoc /> */
        [SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods")]
        public override void WriteByteArray(byte[] val)
        {
            int pos0 = EnsureWriteCapacityAndShift(val.Length);

            fixed (byte* data0 = _data)
            {
                WriteByteArray0(val, data0 + pos0);
            }
        }

        /** <inheritdoc /> */
        public override byte[] ReadByteArray(int cnt)
        {
            int pos0 = EnsureReadCapacityAndShift(cnt);

            fixed (byte* data0 = _data)
            {
                return ReadByteArray0(cnt, data0 + pos0);
            }
        }

        /** <inheritdoc /> */
        [SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods")]
        public override void WriteBoolArray(bool[] val)
        {
            int pos0 = EnsureWriteCapacityAndShift(val.Length);

            fixed (byte* data0 = _data)
            {
                WriteBoolArray0(val, data0 + pos0);
            }
        }

        /** <inheritdoc /> */
        public override bool[] ReadBoolArray(int cnt)
        {
            int pos0 = EnsureReadCapacityAndShift(cnt);

            fixed (byte* data0 = _data)
            {
                return ReadBoolArray0(cnt, data0 + pos0);
            }
        }

        /** <inheritdoc /> */
        public override void WriteShort(short val)
        {
            int pos0 = EnsureWriteCapacityAndShift(2);

            fixed (byte* data0 = _data)
            {
                WriteShort0(val, data0 + pos0);
            }
        }

        /** <inheritdoc /> */
        public override short ReadShort()
        {
            int pos0 = EnsureReadCapacityAndShift(2);

            fixed (byte* data0 = _data)
            {
                return ReadShort0(data0 + pos0);
            }
        }

        /** <inheritdoc /> */
        [SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods")]
        public override void WriteShortArray(short[] val)
        {
            int cnt = val.Length << 1;

            int pos0 = EnsureWriteCapacityAndShift(cnt);

            fixed (byte* data0 = _data)
            {
                WriteShortArray0(val, data0 + pos0, cnt);
            }
        }

        /** <inheritdoc /> */
        public override short[] ReadShortArray(int cnt)
        {
            int cnt0 = cnt << 1;

            int pos0 = EnsureReadCapacityAndShift(cnt0);

            fixed (byte* data0 = _data)
            {
                return ReadShortArray0(cnt, data0 + pos0, cnt0);
            }
        }

        /** <inheritdoc /> */
        [SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods")]
        public override void WriteCharArray(char[] val)
        {
            int cnt = val.Length << 1;

            int pos0 = EnsureWriteCapacityAndShift(cnt);

            fixed (byte* data0 = _data)
            {
                WriteCharArray0(val, data0 + pos0, cnt);
            }
        }

        /** <inheritdoc /> */
        public override char[] ReadCharArray(int cnt)
        {
            int cnt0 = cnt << 1;

            int pos0 = EnsureReadCapacityAndShift(cnt0);

            fixed (byte* data0 = _data)
            {
                return ReadCharArray0(cnt, data0 + pos0, cnt0);
            }
        }

        /** <inheritdoc /> */
        public override void WriteInt(int val)
        {
            int pos0 = EnsureWriteCapacityAndShift(4);

            fixed (byte* data0 = _data)
            {
                WriteInt0(val, data0 + pos0);
            }
        }

        /** <inheritdoc /> */
        public override void WriteInt(int writePos, int val)
        {
            EnsureWriteCapacity(writePos + 4);

            fixed (byte* data0 = _data)
            {
                WriteInt0(val, data0 + writePos);
            }
        }

        /** <inheritdoc /> */
        public override int ReadInt()
        {
            int pos0 = EnsureReadCapacityAndShift(4);

            fixed (byte* data0 = _data)
            {
                return ReadInt0(data0 + pos0);
            }
        }

        /** <inheritdoc /> */
        [SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods")]
        public override void WriteIntArray(int[] val)
        {
            int cnt = val.Length << 2;

            int pos0 = EnsureWriteCapacityAndShift(cnt);

            fixed (byte* data0 = _data)
            {
                WriteIntArray0(val, data0 + pos0, cnt);
            }
        }

        /** <inheritdoc /> */
        public override int[] ReadIntArray(int cnt)
        {
            int cnt0 = cnt << 2;

            int pos0 = EnsureReadCapacityAndShift(cnt0);

            fixed (byte* data0 = _data)
            {
                return ReadIntArray0(cnt, data0 + pos0, cnt0);
            }
        }

        /** <inheritdoc /> */
        [SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods")]
        public override void WriteFloatArray(float[] val)
        {
            int cnt = val.Length << 2;

            int pos0 = EnsureWriteCapacityAndShift(cnt);

            fixed (byte* data0 = _data)
            {
                WriteFloatArray0(val, data0 + pos0, cnt);
            }
        }

        /** <inheritdoc /> */
        public override float[] ReadFloatArray(int cnt)
        {
            int cnt0 = cnt << 2;

            int pos0 = EnsureReadCapacityAndShift(cnt0);

            fixed (byte* data0 = _data)
            {
                return ReadFloatArray0(cnt, data0 + pos0, cnt0);
            }
        }

        /** <inheritdoc /> */
        public override void WriteLong(long val)
        {
            int pos0 = EnsureWriteCapacityAndShift(8);

            fixed (byte* data0 = _data)
            {
                WriteLong0(val, data0 + pos0);
            }
        }

        /** <inheritdoc /> */
        public override long ReadLong()
        {
            int pos0 = EnsureReadCapacityAndShift(8);

            fixed (byte* data0 = _data)
            {
                return ReadLong0(data0 + pos0);
            }
        }

        /** <inheritdoc /> */
        [SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods")]
        public override void WriteLongArray(long[] val)
        {
            int cnt = val.Length << 3;

            int pos0 = EnsureWriteCapacityAndShift(cnt);

            fixed (byte* data0 = _data)
            {
                WriteLongArray0(val, data0 + pos0, cnt);
            }
        }

        /** <inheritdoc /> */
        public override long[] ReadLongArray(int cnt)
        {
            int cnt0 = cnt << 3;

            int pos0 = EnsureReadCapacityAndShift(cnt0);

            fixed (byte* data0 = _data)
            {
                return ReadLongArray0(cnt, data0 + pos0, cnt0);
            }
        }

        /** <inheritdoc /> */
        [SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods")]
        public override void WriteDoubleArray(double[] val)
        {
            int cnt = val.Length << 3;

            int pos0 = EnsureWriteCapacityAndShift(cnt);

            fixed (byte* data0 = _data)
            {
                WriteDoubleArray0(val, data0 + pos0, cnt);
            }
        }

        /** <inheritdoc /> */
        public override double[] ReadDoubleArray(int cnt)
        {
            int cnt0 = cnt << 3;

            int pos0 = EnsureReadCapacityAndShift(cnt0);

            fixed (byte* data0 = _data)
            {
                return ReadDoubleArray0(cnt, data0 + pos0, cnt0);
            }
        }

        /** <inheritdoc /> */
        public override int WriteString(char* chars, int charCnt, int byteCnt, Encoding encoding)
        {
            int pos0 = EnsureWriteCapacityAndShift(byteCnt);

            int written;

            fixed (byte* data0 = _data)
            {
                written = BinaryUtils.StringToUtf8Bytes(chars, charCnt, byteCnt, encoding, data0 + pos0);
            }

            return written;
        }

        /** <inheritdoc /> */
        public override void Write(byte* src, int cnt)
        {
            EnsureWriteCapacity(Pos + cnt);

            fixed (byte* data0 = _data)
            {
                WriteInternal(src, cnt, data0);
            }

            ShiftWrite(cnt);
        }

        /** <inheritdoc /> */
        public override void Read(byte* dest, int cnt)
        {
            fixed (byte* data0 = _data)
            {
                ReadInternal(data0, dest, cnt);
            }
        }

        /** <inheritdoc /> */
        public override int Remaining
        {
            get { return _data.Length - Pos; }
        }

        /** <inheritdoc /> */
        public override byte[] GetArray()
        {
            return _data;
        }

        /** <inheritdoc /> */
        public override byte[] GetArrayCopy()
        {
            byte[] copy = new byte[Pos];

            Buffer.BlockCopy(_data, 0, copy, 0, Pos);

            return copy;
        }

        /** <inheritdoc /> */
        public override bool IsSameArray(byte[] arr)
        {
            return _data == arr;
        }

        /** <inheritdoc /> */
        public override T Apply<TArg, T>(IBinaryStreamProcessor<TArg, T> proc, TArg arg)
        {
            fixed (byte* data0 = _data)
            {
                return proc.Invoke(data0, arg);
            }
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
            get { return _data; }
        }

        /** <inheritdoc /> */
        protected override void EnsureWriteCapacity(int cnt)
        {
            if (cnt > _data.Length)
            {
                int newCap = Capacity(_data.Length, cnt);

                byte[] data0 = new byte[newCap];

                // Copy the whole initial array length here because it can be changed
                // from Java without position adjusting.
                Buffer.BlockCopy(_data, 0, data0, 0, _data.Length);

                _data = data0;
            }
        }

        /** <inheritdoc /> */
        protected override void EnsureReadCapacity(int cnt)
        {
            if (_data.Length - Pos < cnt)
                throw new EndOfStreamException("Not enough data in stream [expected=" + cnt +
                    ", remaining=" + (_data.Length - Pos) + ']');
        }
    }
}
