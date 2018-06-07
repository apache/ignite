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
    using Apache.Ignite.Core.Impl.Memory;

    /// <summary>
    /// Binary onheap stream.
    /// </summary>
    internal unsafe class BinaryHeapStream : IBinaryStream
    {
        /** Byte: zero. */
        private const byte ByteZero = 0;

        /** Byte: one. */
        private const byte ByteOne = 1;

        /** LITTLE_ENDIAN flag. */
        private static readonly bool LittleEndian = BitConverter.IsLittleEndian;

        /** Position. */
        private int _pos;

        /** Disposed flag. */
        private bool _disposed;

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

        /// <summary>
        /// Internal routine to write byte array.
        /// </summary>
        /// <param name="val">Byte array.</param>
        /// <param name="data">Data pointer.</param>
        private static void WriteByteArray0(byte[] val, byte* data)
        {
            fixed (byte* val0 = val)
            {
                CopyMemory(val0, data, val.Length);
            }
        }

        /// <summary>
        /// Internal routine to read byte array.
        /// </summary>
        /// <param name="len">Array length.</param>
        /// <param name="data">Data pointer.</param>
        /// <returns>Byte array</returns>
        private static byte[] ReadByteArray0(int len, byte* data)
        {
            byte[] res = new byte[len];

            fixed (byte* res0 = res)
            {
                CopyMemory(data, res0, len);
            }

            return res;
        }

        /** <inheritdoc /> */
        public void WriteBool(bool val)
        {
            WriteByte(val ? ByteOne : ByteZero);
        }

        /** <inheritdoc /> */
        public bool ReadBool()
        {
            return ReadByte() == ByteOne;
        }

        /// <summary>
        /// Internal routine to write bool array.
        /// </summary>
        /// <param name="val">Bool array.</param>
        /// <param name="data">Data pointer.</param>
        private static void WriteBoolArray0(bool[] val, byte* data)
        {
            fixed (bool* val0 = val)
            {
                CopyMemory((byte*)val0, data, val.Length);
            }
        }

        /// <summary>
        /// Internal routine to read bool array.
        /// </summary>
        /// <param name="len">Array length.</param>
        /// <param name="data">Data pointer.</param>
        /// <returns>Bool array</returns>
        private static bool[] ReadBoolArray0(int len, byte* data)
        {
            bool[] res = new bool[len];

            fixed (bool* res0 = res)
            {
                CopyMemory(data, (byte*)res0, len);
            }

            return res;
        }

        /// <summary>
        /// Internal routine to write short value.
        /// </summary>
        /// <param name="val">Short value.</param>
        /// <param name="data">Data pointer.</param>
        private static void WriteShort0(short val, byte* data)
        {
            if (LittleEndian)
                *((short*)data) = val;
            else
            {
                byte* valPtr = (byte*)&val;

                data[0] = valPtr[1];
                data[1] = valPtr[0];
            }
        }

        /// <summary>
        /// Internal routine to read short value.
        /// </summary>
        /// <param name="data">Data pointer.</param>
        /// <returns>Short value</returns>
        private static short ReadShort0(byte* data)
        {
            short val;

            if (LittleEndian)
                val = *((short*)data);
            else
            {
                byte* valPtr = (byte*)&val;

                valPtr[0] = data[1];
                valPtr[1] = data[0];
            }

            return val;
        }

        /// <summary>
        /// Internal routine to write short array.
        /// </summary>
        /// <param name="val">Short array.</param>
        /// <param name="data">Data pointer.</param>
        /// <param name="cnt">Bytes count.</param>
        private static void WriteShortArray0(short[] val, byte* data, int cnt)
        {
            if (LittleEndian)
            {
                fixed (short* val0 = val)
                {
                    CopyMemory((byte*)val0, data, cnt);
                }
            }
            else
            {
                byte* curPos = data;

                for (int i = 0; i < val.Length; i++)
                {
                    short val0 = val[i];

                    byte* valPtr = (byte*)&(val0);
                    
                    *curPos++ = valPtr[1];
                    *curPos++ = valPtr[0];
                }
            }
        }

        /// <summary>
        /// Internal routine to read short array.
        /// </summary>
        /// <param name="len">Array length.</param>
        /// <param name="data">Data pointer.</param>
        /// <param name="cnt">Bytes count.</param>
        /// <returns>Short array</returns>
        private static short[] ReadShortArray0(int len, byte* data, int cnt)
        {
            short[] res = new short[len];

            if (LittleEndian)
            {
                fixed (short* res0 = res)
                {
                    CopyMemory(data, (byte*)res0, cnt);
                }
            }
            else
            {
                for (int i = 0; i < len; i++)
                {
                    short val;

                    byte* valPtr = (byte*)&val;

                    valPtr[1] = *data++;
                    valPtr[0] = *data++;

                    res[i] = val;
                }
            }

            return res;
        }

        /** <inheritdoc /> */
        public void WriteChar(char val)
        {
            WriteShort(*(short*)(&val));
        }

        /** <inheritdoc /> */
        public char ReadChar()
        {
            short val = ReadShort();

            return *(char*)(&val);
        }

        /// <summary>
        /// Internal routine to write char array.
        /// </summary>
        /// <param name="val">Char array.</param>
        /// <param name="data">Data pointer.</param>
        /// <param name="cnt">Bytes count.</param>
        private static void WriteCharArray0(char[] val, byte* data, int cnt)
        {
            if (LittleEndian)
            {
                fixed (char* val0 = val)
                {
                    CopyMemory((byte*)val0, data, cnt);
                }
            }
            else
            {
                byte* curPos = data;

                for (int i = 0; i < val.Length; i++)
                {
                    char val0 = val[i];

                    byte* valPtr = (byte*)&(val0);

                    *curPos++ = valPtr[1];
                    *curPos++ = valPtr[0];
                }
            }
        }

        /// <summary>
        /// Internal routine to read char array.
        /// </summary>
        /// <param name="len">Count.</param>
        /// <param name="data">Data pointer.</param>
        /// <param name="cnt">Bytes count.</param>
        /// <returns>Char array</returns>
        private static char[] ReadCharArray0(int len, byte* data, int cnt)
        {
            char[] res = new char[len];

            if (LittleEndian)
            {
                fixed (char* res0 = res)
                {
                    CopyMemory(data, (byte*)res0, cnt);
                }
            }
            else
            {
                for (int i = 0; i < len; i++)
                {
                    char val;

                    byte* valPtr = (byte*)&val;

                    valPtr[1] = *data++;
                    valPtr[0] = *data++;

                    res[i] = val;
                }
            }

            return res;
        }

        /// <summary>
        /// Internal routine to write int value.
        /// </summary>
        /// <param name="val">Int value.</param>
        /// <param name="data">Data pointer.</param>
        private static void WriteInt0(int val, byte* data)
        {
            if (LittleEndian)
                *((int*)data) = val;
            else
            {
                byte* valPtr = (byte*)&val;

                data[0] = valPtr[3];
                data[1] = valPtr[2];
                data[2] = valPtr[1];
                data[3] = valPtr[0];
            }
        }

        /// <summary>
        /// Internal routine to read int value.
        /// </summary>
        /// <param name="data">Data pointer.</param>
        /// <returns>Int value</returns>
        public static int ReadInt0(byte* data) {
            int val;

            if (LittleEndian)
                val = *((int*)data);
            else
            {
                byte* valPtr = (byte*)&val;

                valPtr[0] = data[3];
                valPtr[1] = data[2];
                valPtr[2] = data[1];
                valPtr[3] = data[0];
            }
            
            return val;
        }

        /// <summary>
        /// Internal routine to write int array.
        /// </summary>
        /// <param name="val">Int array.</param>
        /// <param name="data">Data pointer.</param>
        /// <param name="cnt">Bytes count.</param>
        private static void WriteIntArray0(int[] val, byte* data, int cnt)
        {
            if (LittleEndian)
            {
                fixed (int* val0 = val)
                {
                    CopyMemory((byte*)val0, data, cnt);
                }
            }
            else
            {
                byte* curPos = data;

                for (int i = 0; i < val.Length; i++)
                {
                    int val0 = val[i];

                    byte* valPtr = (byte*)&(val0);

                    *curPos++ = valPtr[3];
                    *curPos++ = valPtr[2];
                    *curPos++ = valPtr[1];
                    *curPos++ = valPtr[0];
                }
            }
        }

        /// <summary>
        /// Internal routine to read int array.
        /// </summary>
        /// <param name="len">Count.</param>
        /// <param name="data">Data pointer.</param>
        /// <param name="cnt">Bytes count.</param>
        /// <returns>Int array</returns>
        private static int[] ReadIntArray0(int len, byte* data, int cnt)
        {
            int[] res = new int[len];

            if (LittleEndian)
            {
                fixed (int* res0 = res)
                {
                    CopyMemory(data, (byte*)res0, cnt);
                }
            }
            else
            {
                for (int i = 0; i < len; i++)
                {
                    int val;

                    byte* valPtr = (byte*)&val;

                    valPtr[3] = *data++;
                    valPtr[2] = *data++;
                    valPtr[1] = *data++;
                    valPtr[0] = *data++;

                    res[i] = val;
                }
            }

            return res;
        }

        /** <inheritdoc /> */
        public void WriteFloat(float val)
        {
            int val0 = *(int*)(&val);

            WriteInt(val0);
        }

        /** <inheritdoc /> */
        public float ReadFloat()
        {
            int val = ReadInt();

            return BinaryUtils.IntToFloatBits(val);
        }

        /// <summary>
        /// Internal routine to write float array.
        /// </summary>
        /// <param name="val">Int array.</param>
        /// <param name="data">Data pointer.</param>
        /// <param name="cnt">Bytes count.</param>
        private static void WriteFloatArray0(float[] val, byte* data, int cnt)
        {
            if (LittleEndian)
            {
                fixed (float* val0 = val)
                {
                    CopyMemory((byte*)val0, data, cnt);
                }
            }
            else
            {
                byte* curPos = data;

                for (int i = 0; i < val.Length; i++)
                {
                    float val0 = val[i];

                    byte* valPtr = (byte*)&(val0);

                    *curPos++ = valPtr[3];
                    *curPos++ = valPtr[2];
                    *curPos++ = valPtr[1];
                    *curPos++ = valPtr[0];
                }
            }
        }

        /// <summary>
        /// Internal routine to read float array.
        /// </summary>
        /// <param name="len">Count.</param>
        /// <param name="data">Data pointer.</param>
        /// <param name="cnt">Bytes count.</param>
        /// <returns>Float array</returns>
        private static float[] ReadFloatArray0(int len, byte* data, int cnt)
        {
            float[] res = new float[len];

            if (LittleEndian)
            {
                fixed (float* res0 = res)
                {
                    CopyMemory(data, (byte*)res0, cnt);
                }
            }
            else
            {
                for (int i = 0; i < len; i++)
                {
                    int val;

                    byte* valPtr = (byte*)&val;

                    valPtr[3] = *data++;
                    valPtr[2] = *data++;
                    valPtr[1] = *data++;
                    valPtr[0] = *data++;

                    res[i] = val;
                }
            }

            return res;
        }

        /// <summary>
        /// Internal routine to write long value.
        /// </summary>
        /// <param name="val">Long value.</param>
        /// <param name="data">Data pointer.</param>
        private static void WriteLong0(long val, byte* data)
        {
            if (LittleEndian)
                *((long*)data) = val;
            else
            {
                byte* valPtr = (byte*)&val;

                data[0] = valPtr[7];
                data[1] = valPtr[6];
                data[2] = valPtr[5];
                data[3] = valPtr[4];
                data[4] = valPtr[3];
                data[5] = valPtr[2];
                data[6] = valPtr[1];
                data[7] = valPtr[0];
            }
        }

        /// <summary>
        /// Internal routine to read long value.
        /// </summary>
        /// <param name="data">Data pointer.</param>
        /// <returns>Long value</returns>
        private static long ReadLong0(byte* data)
        {
            long val;

            if (LittleEndian)
                val = *((long*)data);
            else
            {
                byte* valPtr = (byte*)&val;

                valPtr[0] = data[7];
                valPtr[1] = data[6];
                valPtr[2] = data[5];
                valPtr[3] = data[4];
                valPtr[4] = data[3];
                valPtr[5] = data[2];
                valPtr[6] = data[1];
                valPtr[7] = data[0];
            }

            return val;
        }

        /// <summary>
        /// Internal routine to write long array.
        /// </summary>
        /// <param name="val">Long array.</param>
        /// <param name="data">Data pointer.</param>
        /// <param name="cnt">Bytes count.</param>
        private static void WriteLongArray0(long[] val, byte* data, int cnt)
        {
            if (LittleEndian)
            {
                fixed (long* val0 = val)
                {
                    CopyMemory((byte*)val0, data, cnt);
                }
            }
            else
            {
                byte* curPos = data;

                for (int i = 0; i < val.Length; i++)
                {
                    long val0 = val[i];

                    byte* valPtr = (byte*)&(val0);

                    *curPos++ = valPtr[7];
                    *curPos++ = valPtr[6];
                    *curPos++ = valPtr[5];
                    *curPos++ = valPtr[4];
                    *curPos++ = valPtr[3];
                    *curPos++ = valPtr[2];
                    *curPos++ = valPtr[1];
                    *curPos++ = valPtr[0];
                }
            }
        }

        /// <summary>
        /// Internal routine to read long array.
        /// </summary>
        /// <param name="len">Count.</param>
        /// <param name="data">Data pointer.</param>
        /// <param name="cnt">Bytes count.</param>
        /// <returns>Long array</returns>
        private static long[] ReadLongArray0(int len, byte* data, int cnt)
        {
            long[] res = new long[len];

            if (LittleEndian)
            {
                fixed (long* res0 = res)
                {
                    CopyMemory(data, (byte*)res0, cnt);
                }
            }
            else
            {
                for (int i = 0; i < len; i++)
                {
                    long val;

                    byte* valPtr = (byte*)&val;

                    valPtr[7] = *data++;
                    valPtr[6] = *data++;
                    valPtr[5] = *data++;
                    valPtr[4] = *data++;
                    valPtr[3] = *data++;
                    valPtr[2] = *data++;
                    valPtr[1] = *data++;
                    valPtr[0] = *data++;

                    res[i] = val;
                }
            }

            return res;
        }

        /** <inheritdoc /> */
        public void WriteDouble(double val)
        {
            long val0 = *(long*)(&val);

            WriteLong(val0);
        }

        /** <inheritdoc /> */
        public double ReadDouble()
        {
            long val = ReadLong();

            return BinaryUtils.LongToDoubleBits(val);
        }

        /// <summary>
        /// Internal routine to write double array.
        /// </summary>
        /// <param name="val">Double array.</param>
        /// <param name="data">Data pointer.</param>
        /// <param name="cnt">Bytes count.</param>
        private static void WriteDoubleArray0(double[] val, byte* data, int cnt)
        {
            if (LittleEndian)
            {
                fixed (double* val0 = val)
                {
                    CopyMemory((byte*)val0, data, cnt);
                }
            }
            else
            {
                byte* curPos = data;

                for (int i = 0; i < val.Length; i++)
                {
                    double val0 = val[i];

                    byte* valPtr = (byte*)&(val0);

                    *curPos++ = valPtr[7];
                    *curPos++ = valPtr[6];
                    *curPos++ = valPtr[5];
                    *curPos++ = valPtr[4];
                    *curPos++ = valPtr[3];
                    *curPos++ = valPtr[2];
                    *curPos++ = valPtr[1];
                    *curPos++ = valPtr[0];
                }
            }
        }

        /// <summary>
        /// Internal routine to read double array.
        /// </summary>
        /// <param name="len">Count.</param>
        /// <param name="data">Data pointer.</param>
        /// <param name="cnt">Bytes count.</param>
        /// <returns>Double array</returns>
        private static double[] ReadDoubleArray0(int len, byte* data, int cnt)
        {
            double[] res = new double[len];

            if (LittleEndian)
            {
                fixed (double* res0 = res)
                {
                    CopyMemory(data, (byte*)res0, cnt);
                }
            }
            else
            {
                for (int i = 0; i < len; i++)
                {
                    double val;

                    byte* valPtr = (byte*)&val;

                    valPtr[7] = *data++;
                    valPtr[6] = *data++;
                    valPtr[5] = *data++;
                    valPtr[4] = *data++;
                    valPtr[3] = *data++;
                    valPtr[2] = *data++;
                    valPtr[1] = *data++;
                    valPtr[0] = *data++;

                    res[i] = val;
                }
            }

            return res;
        }

        /** <inheritdoc /> */
        public void Write(byte[] src, int off, int cnt)
        {
            fixed (byte* src0 = src)
            {
                Write(src0 + off, cnt);
            }
        }

        /** <inheritdoc /> */
        public void Read(byte[] dest, int off, int cnt)
        {
            fixed (byte* dest0 = dest)
            {
                Read(dest0 + off, cnt);
            }
        }

        /// <summary>
        /// Internal write routine.
        /// </summary>
        /// <param name="src">Source.</param>
        /// <param name="cnt">Count.</param>
        /// <param name="data">Data (dsetination).</param>
        private void WriteInternal(byte* src, int cnt, byte* data)
        {
            CopyMemory(src, data + _pos, cnt);
        }

        /// <summary>
        /// Internal read routine.
        /// </summary>
        /// <param name="src">Source</param>
        /// <param name="dest">Destination.</param>
        /// <param name="cnt">Count.</param>
        /// <returns>Amount of bytes written.</returns>
        private void ReadInternal(byte* src, byte* dest, int cnt)
        {
            int cnt0 = Math.Min(Remaining, cnt);

            CopyMemory(src + _pos, dest, cnt0);

            ShiftRead(cnt0);
        }

        /** <inheritdoc /> */
        public int Position
        {
            get { return _pos; }
        }

        /** <inheritdoc /> */
        public int Remaining
        {
            get { return _data.Length - _pos; }
        }

        /// <summary>
        /// Internal array.
        /// </summary>
        internal byte[] InternalArray
        {
            get { return _data; }
        }

        /// <inheritdoc />
        /// <exception cref="T:System.ArgumentException">
        /// Unsupported seek origin:  + origin
        /// or
        /// Seek before origin:  + newPos
        /// </exception>
        public int Seek(int offset, SeekOrigin origin)
        {
            int newPos;

            switch (origin)
            {
                case SeekOrigin.Begin:
                    {
                        newPos = offset;

                        break;
                    }

                case SeekOrigin.Current:
                    {
                        newPos = _pos + offset;

                        break;
                    }

                default:
                    throw new ArgumentException("Unsupported seek origin: " + origin);
            }

            if (newPos < 0)
                throw new ArgumentException("Seek before origin: " + newPos);

            EnsureWriteCapacity(newPos);

            _pos = newPos;

            return _pos;
        }

        /** <inheritdoc /> */
        public void Flush()
        {
            // No-op.
        }

        /** <inheritdoc /> */
        public void Dispose()
        {
            if (_disposed)
                return;

            GC.SuppressFinalize(this);

            _disposed = true;
        }

        /// <summary>
        /// Ensure capacity for write and shift position.
        /// </summary>
        /// <param name="cnt">Bytes count.</param>
        /// <returns>Position before shift.</returns>
        private int EnsureWriteCapacityAndShift(int cnt)
        {
            int pos0 = _pos;

            EnsureWriteCapacity(_pos + cnt);

            ShiftWrite(cnt);

            return pos0;
        }

        /// <summary>
        /// Ensure capacity for read and shift position.
        /// </summary>
        /// <param name="cnt">Bytes count.</param>
        /// <returns>Position before shift.</returns>
        private int EnsureReadCapacityAndShift(int cnt)
        {
            int pos0 = _pos;

            EnsureReadCapacity(cnt);

            ShiftRead(cnt);

            return pos0;
        }

        /// <summary>
        /// Shift position due to write
        /// </summary>
        /// <param name="cnt">Bytes count.</param>
        private void ShiftWrite(int cnt)
        {
            _pos += cnt;
        }

        /// <summary>
        /// Shift position due to read.
        /// </summary>
        /// <param name="cnt">Bytes count.</param>
        private void ShiftRead(int cnt)
        {
            _pos += cnt;
        }

        /// <summary>
        /// Calculate new capacity.
        /// </summary>
        /// <param name="curCap">Current capacity.</param>
        /// <param name="reqCap">Required capacity.</param>
        /// <returns>New capacity.</returns>
        private static int Capacity(int curCap, int reqCap)
        {
            int newCap;

            if (reqCap < 256)
                newCap = 256;
            else
            {
                newCap = curCap << 1;

                if (newCap < reqCap)
                    newCap = reqCap;
            }

            return newCap;
        }

        /// <summary>
        /// Unsafe memory copy routine.
        /// </summary>
        /// <param name="src">Source.</param>
        /// <param name="dest">Destination.</param>
        /// <param name="len">Length.</param>
        private static void CopyMemory(byte* src, byte* dest, int len)
        {
            PlatformMemoryUtils.CopyMemory(src, dest, len);
        }

        /** <inheritdoc /> */
        public void WriteByte(byte val)
        {
            int pos0 = EnsureWriteCapacityAndShift(1);

            _data[pos0] = val;
        }

        /** <inheritdoc /> */
        public byte ReadByte()
        {
            int pos0 = EnsureReadCapacityAndShift(1);

            return _data[pos0];
        }

        /** <inheritdoc /> */
        [SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods")]
        public void WriteByteArray(byte[] val)
        {
            int pos0 = EnsureWriteCapacityAndShift(val.Length);

            fixed (byte* data0 = _data)
            {
                WriteByteArray0(val, data0 + pos0);
            }
        }

        /** <inheritdoc /> */
        public byte[] ReadByteArray(int cnt)
        {
            int pos0 = EnsureReadCapacityAndShift(cnt);

            fixed (byte* data0 = _data)
            {
                return ReadByteArray0(cnt, data0 + pos0);
            }
        }

        /** <inheritdoc /> */
        [SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods")]
        public void WriteBoolArray(bool[] val)
        {
            int pos0 = EnsureWriteCapacityAndShift(val.Length);

            fixed (byte* data0 = _data)
            {
                WriteBoolArray0(val, data0 + pos0);
            }
        }

        /** <inheritdoc /> */
        public bool[] ReadBoolArray(int cnt)
        {
            int pos0 = EnsureReadCapacityAndShift(cnt);

            fixed (byte* data0 = _data)
            {
                return ReadBoolArray0(cnt, data0 + pos0);
            }
        }

        /** <inheritdoc /> */
        public void WriteShort(short val)
        {
            int pos0 = EnsureWriteCapacityAndShift(2);

            fixed (byte* data0 = _data)
            {
                WriteShort0(val, data0 + pos0);
            }
        }

        /** <inheritdoc /> */
        public short ReadShort()
        {
            int pos0 = EnsureReadCapacityAndShift(2);

            fixed (byte* data0 = _data)
            {
                return ReadShort0(data0 + pos0);
            }
        }

        /** <inheritdoc /> */
        [SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods")]
        public void WriteShortArray(short[] val)
        {
            int cnt = val.Length << 1;

            int pos0 = EnsureWriteCapacityAndShift(cnt);

            fixed (byte* data0 = _data)
            {
                WriteShortArray0(val, data0 + pos0, cnt);
            }
        }

        /** <inheritdoc /> */
        public short[] ReadShortArray(int cnt)
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
        public void WriteCharArray(char[] val)
        {
            int cnt = val.Length << 1;

            int pos0 = EnsureWriteCapacityAndShift(cnt);

            fixed (byte* data0 = _data)
            {
                WriteCharArray0(val, data0 + pos0, cnt);
            }
        }

        /** <inheritdoc /> */
        public char[] ReadCharArray(int cnt)
        {
            int cnt0 = cnt << 1;

            int pos0 = EnsureReadCapacityAndShift(cnt0);

            fixed (byte* data0 = _data)
            {
                return ReadCharArray0(cnt, data0 + pos0, cnt0);
            }
        }

        /** <inheritdoc /> */
        public void WriteInt(int val)
        {
            int pos0 = EnsureWriteCapacityAndShift(4);

            fixed (byte* data0 = _data)
            {
                WriteInt0(val, data0 + pos0);
            }
        }

        /** <inheritdoc /> */
        public void WriteInt(int writePos, int val)
        {
            EnsureWriteCapacity(writePos + 4);

            fixed (byte* data0 = _data)
            {
                WriteInt0(val, data0 + writePos);
            }
        }

        /** <inheritdoc /> */
        public int ReadInt()
        {
            int pos0 = EnsureReadCapacityAndShift(4);

            fixed (byte* data0 = _data)
            {
                return ReadInt0(data0 + pos0);
            }
        }

        /** <inheritdoc /> */
        [SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods")]
        public void WriteIntArray(int[] val)
        {
            int cnt = val.Length << 2;

            int pos0 = EnsureWriteCapacityAndShift(cnt);

            fixed (byte* data0 = _data)
            {
                WriteIntArray0(val, data0 + pos0, cnt);
            }
        }

        /** <inheritdoc /> */
        public int[] ReadIntArray(int cnt)
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
        public void WriteFloatArray(float[] val)
        {
            int cnt = val.Length << 2;

            int pos0 = EnsureWriteCapacityAndShift(cnt);

            fixed (byte* data0 = _data)
            {
                WriteFloatArray0(val, data0 + pos0, cnt);
            }
        }

        /** <inheritdoc /> */
        public float[] ReadFloatArray(int cnt)
        {
            int cnt0 = cnt << 2;

            int pos0 = EnsureReadCapacityAndShift(cnt0);

            fixed (byte* data0 = _data)
            {
                return ReadFloatArray0(cnt, data0 + pos0, cnt0);
            }
        }

        /** <inheritdoc /> */
        public void WriteLong(long val)
        {
            int pos0 = EnsureWriteCapacityAndShift(8);

            fixed (byte* data0 = _data)
            {
                WriteLong0(val, data0 + pos0);
            }
        }

        /** <inheritdoc /> */
        public long ReadLong()
        {
            int pos0 = EnsureReadCapacityAndShift(8);

            fixed (byte* data0 = _data)
            {
                return ReadLong0(data0 + pos0);
            }
        }

        /** <inheritdoc /> */
        [SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods")]
        public void WriteLongArray(long[] val)
        {
            int cnt = val.Length << 3;

            int pos0 = EnsureWriteCapacityAndShift(cnt);

            fixed (byte* data0 = _data)
            {
                WriteLongArray0(val, data0 + pos0, cnt);
            }
        }

        /** <inheritdoc /> */
        public long[] ReadLongArray(int cnt)
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
        public void WriteDoubleArray(double[] val)
        {
            int cnt = val.Length << 3;

            int pos0 = EnsureWriteCapacityAndShift(cnt);

            fixed (byte* data0 = _data)
            {
                WriteDoubleArray0(val, data0 + pos0, cnt);
            }
        }

        /** <inheritdoc /> */
        public double[] ReadDoubleArray(int cnt)
        {
            int cnt0 = cnt << 3;

            int pos0 = EnsureReadCapacityAndShift(cnt0);

            fixed (byte* data0 = _data)
            {
                return ReadDoubleArray0(cnt, data0 + pos0, cnt0);
            }
        }

        /** <inheritdoc /> */
        public int WriteString(char* chars, int charCnt, int byteCnt, Encoding encoding)
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
        public void Write(byte* src, int cnt)
        {
            EnsureWriteCapacity(_pos + cnt);

            fixed (byte* data0 = _data)
            {
                WriteInternal(src, cnt, data0);
            }

            ShiftWrite(cnt);
        }

        /** <inheritdoc /> */
        public void Read(byte* dest, int cnt)
        {
            fixed (byte* data0 = _data)
            {
                ReadInternal(data0, dest, cnt);
            }
        }

        /** <inheritdoc /> */
        public byte[] GetArray()
        {
            return _data;
        }

        /** <inheritdoc /> */
        public bool CanGetArray
        {
            get { return true; }
        }

        /** <inheritdoc /> */
        public byte[] GetArrayCopy()
        {
            byte[] copy = new byte[_pos];

            Buffer.BlockCopy(_data, 0, copy, 0, _pos);

            return copy;
        }

        /** <inheritdoc /> */
        public bool IsSameArray(byte[] arr)
        {
            return _data == arr;
        }

        /** <inheritdoc /> */
        [SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods")]
        public T Apply<TArg, T>(IBinaryStreamProcessor<TArg, T> proc, TArg arg)
        {
            Debug.Assert(proc != null);

            fixed (byte* data0 = _data)
            {
                return proc.Invoke(data0, arg);
            }
        }

        /// <summary>
        /// Ensure capacity for write.
        /// </summary>
        /// <param name="cnt">Bytes count.</param>
        private void EnsureWriteCapacity(int cnt)
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

        /// <summary>
        /// Ensure capacity for write and shift position.
        /// </summary>
        /// <param name="cnt">Bytes count.</param>
        /// <returns>Position before shift.</returns>
        private void EnsureReadCapacity(int cnt)
        {
            if (_data.Length - _pos < cnt)
                throw new EndOfStreamException("Not enough data in stream [expected=" + cnt +
                                               ", remaining=" + (_data.Length - _pos) + ']');
        }
    }
}
