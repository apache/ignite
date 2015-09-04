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

namespace Apache.Ignite.Core.Impl.Memory
{
    using System;
    using System.IO;
    using System.Text;
    using Apache.Ignite.Core.Impl.Portable.IO;

    /// <summary>
    /// Platform memory stream.
    /// </summary>
    public unsafe class PlatformMemoryStream : IPortableStream
    {
        /** Length: 1 byte. */
        protected const int Len1 = 1;

        /** Length: 2 bytes. */
        protected const int Len2 = 2;

        /** Length: 4 bytes. */
        protected const int Len4 = 4;

        /** Length: 8 bytes. */
        protected const int Len8 = 8;

        /** Shift: 2 bytes. */
        protected const int Shift2 = 1;

        /** Shift: 4 bytes. */
        protected const int Shift4 = 2;

        /** Shift: 8 bytes. */
        protected const int Shift8 = 3;
        
        /** Underlying memory. */
        private readonly IPlatformMemory _mem;

        /** Actual data. */
        protected byte* Data;

        /** CalculateCapacity. */
        private int _cap;

        /** Position. */
        private int _pos;

        /** Length. */
        private int _len;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="mem">Memory.</param>
        public PlatformMemoryStream(IPlatformMemory mem)
        {
            _mem = mem;

            Data = (byte*)mem.Data;
            _cap = mem.Capacity;
            _len = mem.Length;
        }

        #region WRITE

        /** <inheritdoc /> */
        public void WriteByte(byte val)
        {
            int curPos = EnsureWriteCapacityAndShift(Len1);

            *(Data + curPos) = val;
        }

        /** <inheritdoc /> */
        public void WriteByteArray(byte[] val)
        {
            fixed (byte* val0 = val)
            {
                CopyFromAndShift(val0, val.Length);
            }
        }

        /** <inheritdoc /> */
        public void WriteBool(bool val)
        {
            WriteByte(val ? (byte)1 : (byte)0);
        }
        
        /** <inheritdoc /> */
        public void WriteBoolArray(bool[] val)
        {
            fixed (bool* val0 = val)
            {
                CopyFromAndShift((byte*)val0, val.Length);
            }
        }

        /** <inheritdoc /> */
        public virtual void WriteShort(short val)
        {
            int curPos = EnsureWriteCapacityAndShift(Len2);

            *((short*)(Data + curPos)) = val;
        }

        /** <inheritdoc /> */
        public virtual void WriteShortArray(short[] val)
        {
            fixed (short* val0 = val)
            {
                CopyFromAndShift((byte*)val0, val.Length << Shift2);
            }
        }

        /** <inheritdoc /> */
        public virtual void WriteChar(char val)
        {
            int curPos = EnsureWriteCapacityAndShift(Len2);

            *((char*)(Data + curPos)) = val;
        }

        /** <inheritdoc /> */
        public virtual void WriteCharArray(char[] val)
        {
            fixed (char* val0 = val)
            {
                CopyFromAndShift((byte*)val0, val.Length << Shift2);
            }
        }

        /** <inheritdoc /> */
        public virtual void WriteInt(int val)
        {
            int curPos = EnsureWriteCapacityAndShift(Len4);

            *((int*)(Data + curPos)) = val;
        }

        /** <inheritdoc /> */
        public virtual void WriteInt(int writePos, int val)
        {
            EnsureWriteCapacity(writePos + 4);

            *((int*)(Data + writePos)) = val;
        }

        /** <inheritdoc /> */
        public virtual void WriteIntArray(int[] val)
        {
            fixed (int* val0 = val)
            {
                CopyFromAndShift((byte*)val0, val.Length << Shift4);
            }
        }

        /** <inheritdoc /> */
        public virtual void WriteLong(long val)
        {
            int curPos = EnsureWriteCapacityAndShift(Len8);

            *((long*)(Data + curPos)) = val;
        }

        /** <inheritdoc /> */
        public virtual void WriteLongArray(long[] val)
        {
            fixed (long* val0 = val)
            {
                CopyFromAndShift((byte*)val0, val.Length << Shift8);
            }
        }

        /** <inheritdoc /> */
        public virtual void WriteFloat(float val)
        {
            int curPos = EnsureWriteCapacityAndShift(Len4);

            *((float*)(Data + curPos)) = val;
        }

        /** <inheritdoc /> */
        public virtual void WriteFloatArray(float[] val)
        {
            fixed (float* val0 = val)
            {
                CopyFromAndShift((byte*)val0, val.Length << Shift4);
            }
        }

        /** <inheritdoc /> */
        public virtual void WriteDouble(double val)
        {
            int curPos = EnsureWriteCapacityAndShift(Len8);

            *((double*)(Data + curPos)) = val;
        }

        /** <inheritdoc /> */
        public virtual void WriteDoubleArray(double[] val)
        {
            fixed (double* val0 = val)
            {
                CopyFromAndShift((byte*)val0, val.Length << Shift8);
            }
        }

        /** <inheritdoc /> */
        public int WriteString(char* chars, int charCnt, int byteCnt, Encoding enc)
        {
            int curPos = EnsureWriteCapacityAndShift(byteCnt);

            return enc.GetBytes(chars, charCnt, Data + curPos, byteCnt);
        }

        /** <inheritdoc /> */
        public void Write(byte[] src, int off, int cnt)
        {
            fixed (byte* src0 = src)
            {
                CopyFromAndShift(src0 + off, cnt);    
            }
        }

        /** <inheritdoc /> */
        public void Write(byte* src, int cnt)
        {
            CopyFromAndShift(src, cnt);
        }
        
        #endregion WRITE
        
        #region READ

        /** <inheritdoc /> */
        public byte ReadByte()
        {
            int curPos = EnsureReadCapacityAndShift(Len1);

            return *(Data + curPos);
        }

        /** <inheritdoc /> */

        public byte[] ReadByteArray(int cnt)
        {
            int curPos = EnsureReadCapacityAndShift(cnt);

            byte[] res = new byte[cnt];

            fixed (byte* res0 = res)
            {
                PlatformMemoryUtils.CopyMemory(Data + curPos, res0, cnt);
            }

            return res;
        }
        
        /** <inheritdoc /> */
        public bool ReadBool()
        {
            return ReadByte() == 1;
        }

        /** <inheritdoc /> */
        public bool[] ReadBoolArray(int cnt)
        {
            bool[] res = new bool[cnt];

            fixed (bool* res0 = res)
            {
                CopyToAndShift((byte*)res0, cnt);
            }

            return res;
        }

        /** <inheritdoc /> */
        public virtual short ReadShort()
        {
            int curPos = EnsureReadCapacityAndShift(Len2);

            return *((short*)(Data + curPos));
        }

        /** <inheritdoc /> */
        public virtual short[] ReadShortArray(int cnt)
        {
            short[] res = new short[cnt];

            fixed (short* res0 = res)
            {
                CopyToAndShift((byte*)res0, cnt << Shift2);
            }

            return res;
        }

        /** <inheritdoc /> */
        public virtual char ReadChar()
        {
            int curPos = EnsureReadCapacityAndShift(Len2);

            return *((char*)(Data + curPos));
        }

        /** <inheritdoc /> */
        public virtual char[] ReadCharArray(int cnt)
        {
            char[] res = new char[cnt];

            fixed (char* res0 = res)
            {
                CopyToAndShift((byte*)res0, cnt << Shift2);
            }

            return res;
        }

        /** <inheritdoc /> */
        public virtual int ReadInt()
        {
            int curPos = EnsureReadCapacityAndShift(Len4);

            return *((int*)(Data + curPos));
        }
        
        /** <inheritdoc /> */
        public virtual int[] ReadIntArray(int cnt)
        {
            int[] res = new int[cnt];

            fixed (int* res0 = res)
            {
                CopyToAndShift((byte*)res0, cnt << Shift4);
            }

            return res;
        }

        /** <inheritdoc /> */
        public virtual long ReadLong()
        {
            int curPos = EnsureReadCapacityAndShift(Len8);

            return *((long*)(Data + curPos));
        }
        
        /** <inheritdoc /> */
        public virtual long[] ReadLongArray(int cnt)
        {
            long[] res = new long[cnt];

            fixed (long* res0 = res)
            {
                CopyToAndShift((byte*)res0, cnt << Shift8);
            }

            return res;
        }

        /** <inheritdoc /> */
        public virtual float ReadFloat()
        {
            int curPos = EnsureReadCapacityAndShift(Len4);

            return *((float*)(Data + curPos));
        }

        /** <inheritdoc /> */
        public virtual float[] ReadFloatArray(int cnt)
        {
            float[] res = new float[cnt];

            fixed (float* res0 = res)
            {
                CopyToAndShift((byte*)res0, cnt << Shift4);
            }

            return res;
        }

        /** <inheritdoc /> */
        public virtual double ReadDouble()
        {
            int curPos = EnsureReadCapacityAndShift(Len8);

            return *((double*)(Data + curPos));
        }

        /** <inheritdoc /> */
        public virtual double[] ReadDoubleArray(int cnt)
        {
            double[] res = new double[cnt];

            fixed (double* res0 = res)
            {
                CopyToAndShift((byte*)res0, cnt << Shift8);
            }

            return res;
        }

        /** <inheritdoc /> */
        public void Read(byte[] dest, int off, int cnt)
        {
            fixed (byte* dest0 = dest)
            {
                Read(dest0 + off, cnt);
            }
        }

        /** <inheritdoc /> */
        public void Read(byte* dest, int cnt)
        {
            CopyToAndShift(dest, cnt);
        }

        #endregion 

        #region MISC

        /// <summary>
        /// Get cross-platform memory pointer for the stream.
        /// </summary>
        public long MemoryPointer
        {
            get { return _mem.Pointer; }
        }

        /// <summary>
        /// Synchronize stream write opeartions with underlying memory and return current memory pointer.
        /// <returns>Memory pointer.</returns>
        /// </summary>
        public long SynchronizeOutput()
        {
            if (_pos > _len)
                _len = _pos;

            _mem.Length = _len;

            return MemoryPointer;
        }

        /// <summary>
        /// Synchronized stream read operations from underlying memory. This is required when stream was passed 
        /// to Java and something might have been written there.
        /// </summary>
        public void SynchronizeInput()
        {
            Data = (byte*)_mem.Data;
            _cap = _mem.Capacity;
            _len = _mem.Length;
        }

        /// <summary>
        /// Reset stream state. Sets both position and length to 0.
        /// </summary>
        public void Reset()
        {
            _pos = 0;
        }

        /// <summary>
        /// Reset stream state as if it was just created.
        /// </summary>
        public void Reuse()
        {
            Data = (byte*)_mem.Data;
            _cap = _mem.Capacity;
            _len = _mem.Length;
            _pos = 0;
        }

        /** <inheritdoc /> */
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

        /// <summary>
        /// Ensure capacity for write and shift position.
        /// </summary>
        /// <param name="cnt">Bytes count.</param>
        /// <returns>Position before shift.</returns>
        protected int EnsureWriteCapacityAndShift(int cnt)
        {
            int curPos = _pos;

            int newPos = _pos + cnt;

            EnsureWriteCapacity(newPos);

            _pos = newPos;

            return curPos;
        }

        /// <summary>
        /// Ensure write capacity.
        /// </summary>
        /// <param name="reqCap">Required capacity.</param>
        protected void EnsureWriteCapacity(int reqCap)
        {
            if (reqCap > _cap)
            {
                reqCap = CalculateCapacity(_cap, reqCap);

                _mem.Reallocate(reqCap);

                Data = (byte*)_mem.Data;
                _cap = _mem.Capacity;
            }
        }

        /// <summary>
        /// Ensure capacity for read and shift position.
        /// </summary>
        /// <param name="cnt">Bytes count.</param>
        /// <returns>Position before shift.</returns>
        protected int EnsureReadCapacityAndShift(int cnt)
        {
            int curPos = _pos;

            if (_len - _pos < cnt)
                throw new EndOfStreamException("Not enough data in stream [expected=" + cnt +
                    ", remaining=" + (_len - _pos) + ']');

            _pos += cnt;

            return curPos;
        }

        /// <summary>
        /// Copy (read) some data into destination and shift the stream forward.
        /// </summary>
        /// <param name="dest">Destination.</param>
        /// <param name="cnt">Bytes count.</param>
        private void CopyToAndShift(byte* dest, int cnt)
        {
            int curPos = EnsureReadCapacityAndShift(cnt);

            PlatformMemoryUtils.CopyMemory(Data + curPos, dest, cnt);
        }

        /// <summary>
        /// Copy (write) some data from source and shift the stream forward.
        /// </summary>
        /// <param name="src">Source.</param>
        /// <param name="cnt">Bytes count.</param>
        private void CopyFromAndShift(byte* src, int cnt)
        {
            int curPos = EnsureWriteCapacityAndShift(cnt);

            PlatformMemoryUtils.CopyMemory(src, Data + curPos, cnt);
        }

        /// <summary>
        /// Calculate new capacity.
        /// </summary>
        /// <param name="curCap">Current capacity.</param>
        /// <param name="reqCap">Required capacity.</param>
        /// <returns>New capacity.</returns>
        private static int CalculateCapacity(int curCap, int reqCap)
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

        /** <inheritdoc /> */
        public int Position
        {
            get { return _pos; }
        }

        /** <inheritdoc /> */
        public int Remaining()
        {
            return _len - _pos;
        }

        /** <inheritdoc /> */
        public void Dispose()
        {
            SynchronizeOutput();

            _mem.Release();
        }
        
        #endregion

        #region ARRAYS

        /** <inheritdoc /> */
        public byte[] Array()
        {
            return ArrayCopy();
        }

        /** <inheritdoc /> */
        public byte[] ArrayCopy()
        {
            byte[] res = new byte[_mem.Length];

            fixed (byte* res0 = res)
            {
                PlatformMemoryUtils.CopyMemory(Data, res0, res.Length);
            }

            return res;
        }

        /** <inheritdoc /> */
        public bool IsSameArray(byte[] arr)
        {
            return false;
        }

        #endregion
    }
}
