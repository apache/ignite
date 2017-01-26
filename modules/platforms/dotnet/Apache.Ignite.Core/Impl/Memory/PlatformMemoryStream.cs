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
    using System.Diagnostics.CodeAnalysis;
    using System.IO;
    using System.Text;
    using Apache.Ignite.Core.Impl.Binary;
    using Apache.Ignite.Core.Impl.Binary.IO;
    using Apache.Ignite.Core.Impl.Common;

    /// <summary>
    /// Platform memory stream.
    /// </summary>
    [CLSCompliant(false)]
    [SuppressMessage("Microsoft.Naming", "CA1711:IdentifiersShouldNotHaveIncorrectSuffix")]
    public unsafe class PlatformMemoryStream : IBinaryStream
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
        private byte* _data;

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
        [SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods")]
        public PlatformMemoryStream(IPlatformMemory mem)
        {
            _mem = mem;

            _data = (byte*)mem.Data;
            _cap = mem.Capacity;
            _len = mem.Length;
        }

        #region WRITE

        /// <summary>
        /// Write byte.
        /// </summary>
        /// <param name="val">Byte value.</param>
        public void WriteByte(byte val)
        {
            int curPos = EnsureWriteCapacityAndShift(Len1);

            *(_data + curPos) = val;
        }

        /// <summary>
        /// Write byte array.
        /// </summary>
        /// <param name="val">Byte array.</param>
        public void WriteByteArray(byte[] val)
        {
            IgniteArgumentCheck.NotNull(val, "val");

            fixed (byte* val0 = val)
            {
                CopyFromAndShift(val0, val.Length);
            }
        }

        /// <summary>
        /// Write bool.
        /// </summary>
        /// <param name="val">Bool value.</param>
        public void WriteBool(bool val)
        {
            WriteByte(val ? (byte)1 : (byte)0);
        }

        /// <summary>
        /// Write bool array.
        /// </summary>
        /// <param name="val">Bool array.</param>
        public void WriteBoolArray(bool[] val)
        {
            IgniteArgumentCheck.NotNull(val, "val");

            fixed (bool* val0 = val)
            {
                CopyFromAndShift((byte*)val0, val.Length);
            }
        }

        /// <summary>
        /// Write short.
        /// </summary>
        /// <param name="val">Short value.</param>
        public virtual void WriteShort(short val)
        {
            int curPos = EnsureWriteCapacityAndShift(Len2);

            *((short*)(_data + curPos)) = val;
        }

        /// <summary>
        /// Write short array.
        /// </summary>
        /// <param name="val">Short array.</param>
        public virtual void WriteShortArray(short[] val)
        {
            IgniteArgumentCheck.NotNull(val, "val");

            fixed (short* val0 = val)
            {
                CopyFromAndShift((byte*)val0, val.Length << Shift2);
            }
        }

        /// <summary>
        /// Write char.
        /// </summary>
        /// <param name="val">Char value.</param>
        public virtual void WriteChar(char val)
        {
            int curPos = EnsureWriteCapacityAndShift(Len2);

            *((char*)(_data + curPos)) = val;
        }

        /// <summary>
        /// Write char array.
        /// </summary>
        /// <param name="val">Char array.</param>
        public virtual void WriteCharArray(char[] val)
        {
            IgniteArgumentCheck.NotNull(val, "val");

            fixed (char* val0 = val)
            {
                CopyFromAndShift((byte*)val0, val.Length << Shift2);
            }
        }

        /// <summary>
        /// Write int.
        /// </summary>
        /// <param name="val">Int value.</param>
        public virtual void WriteInt(int val)
        {
            int curPos = EnsureWriteCapacityAndShift(Len4);

            *((int*)(_data + curPos)) = val;
        }

        /// <summary>
        /// Write int to specific position.
        /// </summary>
        /// <param name="writePos">Position.</param>
        /// <param name="val">Value.</param>
        [SuppressMessage("Microsoft.Usage", "CA2233:OperationsShouldNotOverflow", MessageId = "writePos+4")]
        public virtual void WriteInt(int writePos, int val)
        {
            EnsureWriteCapacity(writePos + 4);

            *((int*)(_data + writePos)) = val;
        }

        /// <summary>
        /// Write int array.
        /// </summary>
        /// <param name="val">Int array.</param>
        public virtual void WriteIntArray(int[] val)
        {
            IgniteArgumentCheck.NotNull(val, "val");

            fixed (int* val0 = val)
            {
                CopyFromAndShift((byte*)val0, val.Length << Shift4);
            }
        }

        /// <summary>
        /// Write long.
        /// </summary>
        /// <param name="val">Long value.</param>
        public virtual void WriteLong(long val)
        {
            int curPos = EnsureWriteCapacityAndShift(Len8);

            *((long*)(_data + curPos)) = val;
        }

        /// <summary>
        /// Write long array.
        /// </summary>
        /// <param name="val">Long array.</param>
        public virtual void WriteLongArray(long[] val)
        {
            IgniteArgumentCheck.NotNull(val, "val");

            fixed (long* val0 = val)
            {
                CopyFromAndShift((byte*)val0, val.Length << Shift8);
            }
        }

        /// <summary>
        /// Write float.
        /// </summary>
        /// <param name="val">Float value.</param>
        public virtual void WriteFloat(float val)
        {
            int curPos = EnsureWriteCapacityAndShift(Len4);

            *((float*)(_data + curPos)) = val;
        }

        /// <summary>
        /// Write float array.
        /// </summary>
        /// <param name="val">Float array.</param>
        public virtual void WriteFloatArray(float[] val)
        {
            IgniteArgumentCheck.NotNull(val, "val");

            fixed (float* val0 = val)
            {
                CopyFromAndShift((byte*)val0, val.Length << Shift4);
            }
        }

        /// <summary>
        /// Write double.
        /// </summary>
        /// <param name="val">Double value.</param>
        public virtual void WriteDouble(double val)
        {
            int curPos = EnsureWriteCapacityAndShift(Len8);

            *((double*)(_data + curPos)) = val;
        }

        /// <summary>
        /// Write double array.
        /// </summary>
        /// <param name="val">Double array.</param>
        public virtual void WriteDoubleArray(double[] val)
        {
            IgniteArgumentCheck.NotNull(val, "val");

            fixed (double* val0 = val)
            {
                CopyFromAndShift((byte*)val0, val.Length << Shift8);
            }
        }

        /// <summary>
        /// Write string.
        /// </summary>
        /// <param name="chars">Characters.</param>
        /// <param name="charCnt">Char count.</param>
        /// <param name="byteCnt">Byte count.</param>
        /// <param name="encoding">Encoding.</param>
        /// <returns>
        /// Amounts of bytes written.
        /// </returns>
        [SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods")]
        public int WriteString(char* chars, int charCnt, int byteCnt, Encoding encoding)
        {
            IgniteArgumentCheck.NotNull(charCnt, "charCnt");
            IgniteArgumentCheck.NotNull(byteCnt, "byteCnt");
            
            int curPos = EnsureWriteCapacityAndShift(byteCnt);

            return BinaryUtils.StringToUtf8Bytes(chars, charCnt, byteCnt, encoding, _data + curPos);
        }

        /// <summary>
        /// Write arbitrary data.
        /// </summary>
        /// <param name="src">Source array.</param>
        /// <param name="off">Offset</param>
        /// <param name="cnt">Count.</param>
        public void Write(byte[] src, int off, int cnt)
        {
            IgniteArgumentCheck.NotNull(src, "src");

            fixed (byte* src0 = src)
            {
                CopyFromAndShift(src0 + off, cnt);    
            }
        }

        /// <summary>
        /// Write arbitrary data.
        /// </summary>
        /// <param name="src">Source.</param>
        /// <param name="cnt">Count.</param>
        public void Write(byte* src, int cnt)
        {
            CopyFromAndShift(src, cnt);
        }

        #endregion WRITE

        #region READ

        /// <summary>
        /// Read byte.
        /// </summary>
        /// <returns>
        /// Byte value.
        /// </returns>
        public byte ReadByte()
        {
            int curPos = EnsureReadCapacityAndShift(Len1);

            return *(_data + curPos);
        }
        
        /// <summary>
        /// Read byte array.
        /// </summary>
        /// <param name="cnt">Count.</param>
        /// <returns>
        /// Byte array.
        /// </returns>
        public byte[] ReadByteArray(int cnt)
        {
            int curPos = EnsureReadCapacityAndShift(cnt);

            byte[] res = new byte[cnt];

            fixed (byte* res0 = res)
            {
                PlatformMemoryUtils.CopyMemory(_data + curPos, res0, cnt);
            }

            return res;
        }

        /// <summary>
        /// Read bool.
        /// </summary>
        /// <returns>
        /// Bool value.
        /// </returns>
        public bool ReadBool()
        {
            return ReadByte() == 1;
        }

        /// <summary>
        /// Read bool array.
        /// </summary>
        /// <param name="cnt">Count.</param>
        /// <returns>
        /// Bool array.
        /// </returns>
        public bool[] ReadBoolArray(int cnt)
        {
            bool[] res = new bool[cnt];

            fixed (bool* res0 = res)
            {
                CopyToAndShift((byte*)res0, cnt);
            }

            return res;
        }

        /// <summary>
        /// Read short.
        /// </summary>
        /// <returns>
        /// Short value.
        /// </returns>
        public virtual short ReadShort()
        {
            int curPos = EnsureReadCapacityAndShift(Len2);

            return *((short*)(_data + curPos));
        }

        /// <summary>
        /// Read short array.
        /// </summary>
        /// <param name="cnt">Count.</param>
        /// <returns>
        /// Short array.
        /// </returns>
        public virtual short[] ReadShortArray(int cnt)
        {
            short[] res = new short[cnt];

            fixed (short* res0 = res)
            {
                CopyToAndShift((byte*)res0, cnt << Shift2);
            }

            return res;
        }

        /// <summary>
        /// Read char.
        /// </summary>
        /// <returns>
        /// Char value.
        /// </returns>
        public virtual char ReadChar()
        {
            int curPos = EnsureReadCapacityAndShift(Len2);

            return *((char*)(_data + curPos));
        }

        /// <summary>
        /// Read char array.
        /// </summary>
        /// <param name="cnt">Count.</param>
        /// <returns>
        /// Char array.
        /// </returns>
        public virtual char[] ReadCharArray(int cnt)
        {
            char[] res = new char[cnt];

            fixed (char* res0 = res)
            {
                CopyToAndShift((byte*)res0, cnt << Shift2);
            }

            return res;
        }

        /// <summary>
        /// Read int.
        /// </summary>
        /// <returns>
        /// Int value.
        /// </returns>
        public virtual int ReadInt()
        {
            int curPos = EnsureReadCapacityAndShift(Len4);

            return *((int*)(_data + curPos));
        }

        /// <summary>
        /// Read int array.
        /// </summary>
        /// <param name="cnt">Count.</param>
        /// <returns>
        /// Int array.
        /// </returns>
        public virtual int[] ReadIntArray(int cnt)
        {
            int[] res = new int[cnt];

            fixed (int* res0 = res)
            {
                CopyToAndShift((byte*)res0, cnt << Shift4);
            }

            return res;
        }

        /// <summary>
        /// Read long.
        /// </summary>
        /// <returns>
        /// Long value.
        /// </returns>
        public virtual long ReadLong()
        {
            int curPos = EnsureReadCapacityAndShift(Len8);

            return *((long*)(_data + curPos));
        }

        /// <summary>
        /// Read long array.
        /// </summary>
        /// <param name="cnt">Count.</param>
        /// <returns>
        /// Long array.
        /// </returns>
        public virtual long[] ReadLongArray(int cnt)
        {
            long[] res = new long[cnt];

            fixed (long* res0 = res)
            {
                CopyToAndShift((byte*)res0, cnt << Shift8);
            }

            return res;
        }

        /// <summary>
        /// Read float.
        /// </summary>
        /// <returns>
        /// Float value.
        /// </returns>
        public virtual float ReadFloat()
        {
            int curPos = EnsureReadCapacityAndShift(Len4);

            return *((float*)(_data + curPos));
        }

        /// <summary>
        /// Read float array.
        /// </summary>
        /// <param name="cnt">Count.</param>
        /// <returns>
        /// Float array.
        /// </returns>
        public virtual float[] ReadFloatArray(int cnt)
        {
            float[] res = new float[cnt];

            fixed (float* res0 = res)
            {
                CopyToAndShift((byte*)res0, cnt << Shift4);
            }

            return res;
        }

        /// <summary>
        /// Read double.
        /// </summary>
        /// <returns>
        /// Double value.
        /// </returns>
        public virtual double ReadDouble()
        {
            int curPos = EnsureReadCapacityAndShift(Len8);

            return *((double*)(_data + curPos));
        }

        /// <summary>
        /// Read double array.
        /// </summary>
        /// <param name="cnt">Count.</param>
        /// <returns>
        /// Double array.
        /// </returns>
        public virtual double[] ReadDoubleArray(int cnt)
        {
            double[] res = new double[cnt];

            fixed (double* res0 = res)
            {
                CopyToAndShift((byte*)res0, cnt << Shift8);
            }

            return res;
        }

        /// <summary>
        /// Read arbitrary data.
        /// </summary>
        /// <param name="dest">Destination array.</param>
        /// <param name="off">Offset.</param>
        /// <param name="cnt">Count.</param>
        public void Read(byte[] dest, int off, int cnt)
        {
            IgniteArgumentCheck.NotNull(dest, "dest");

            fixed (byte* dest0 = dest)
            {
                Read(dest0 + off, cnt);
            }
        }

        /// <summary>
        /// Read arbitrary data.
        /// </summary>
        /// <param name="dest">Destination.</param>
        /// <param name="cnt">Count.</param>
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
            _data = (byte*)_mem.Data;
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
            _data = (byte*)_mem.Data;
            _cap = _mem.Capacity;
            _len = _mem.Length;
            _pos = 0;
        }

        /// <summary>
        /// Seek to the given positoin.
        /// </summary>
        /// <param name="offset">Offset.</param>
        /// <param name="origin">Seek origin.</param>
        /// <returns>
        /// Position.
        /// </returns>
        /// <exception cref="System.ArgumentException">
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

        /// <summary>
        /// Returns a hash code for the specified byte range.
        /// </summary>
        public T Apply<TArg, T>(IBinaryStreamProcessor<TArg, T> proc, TArg arg)
        {
            return proc.Invoke(_data, arg);
        }

        /// <summary>
        /// Flushes the data to underlying storage.
        /// </summary>
        public void Flush()
        {
            SynchronizeOutput();
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

                _data = (byte*)_mem.Data;
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

            PlatformMemoryUtils.CopyMemory(_data + curPos, dest, cnt);
        }

        /// <summary>
        /// Copy (write) some data from source and shift the stream forward.
        /// </summary>
        /// <param name="src">Source.</param>
        /// <param name="cnt">Bytes count.</param>
        private void CopyFromAndShift(byte* src, int cnt)
        {
            int curPos = EnsureWriteCapacityAndShift(cnt);

            PlatformMemoryUtils.CopyMemory(src, _data + curPos, cnt);
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


        /// <summary>
        /// Position.
        /// </summary>
        public int Position
        {
            get { return _pos; }
        }


        /// <summary>
        /// Gets remaining bytes in the stream.
        /// </summary>
        /// <value>
        /// Remaining bytes.
        /// </value>
        public int Remaining
        {
            get { return _len - _pos; }
        }


        /// <summary>
        /// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }


        /// <summary>
        /// Finalizes an instance of the <see cref="PlatformMemoryStream"/> class.
        /// </summary>
        ~PlatformMemoryStream()
        {
            Dispose(false);
        }

        /// <summary>
        /// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
        /// </summary>
        protected virtual void Dispose(bool disposing)
        {
            if (disposing)
                SynchronizeOutput();

            _mem.Release();
        }

        /// <summary>
        /// Gets the data.
        /// </summary>
        protected byte* Data
        {
            get { return _data; }
        }

        #endregion

        #region ARRAYS
        
        /// <summary>
        /// Gets underlying array, avoiding copying if possible.
        /// </summary>
        /// <returns>
        /// Underlying array.
        /// </returns>
        public byte[] GetArray()
        {
            return GetArrayCopy();
        }
        
        /// <summary>
        /// Gets underlying data in a new array.
        /// </summary>
        /// <returns>
        /// New array with data.
        /// </returns>
        public byte[] GetArrayCopy()
        {
            byte[] res = new byte[_mem.Length];

            fixed (byte* res0 = res)
            {
                PlatformMemoryUtils.CopyMemory(_data, res0, res.Length);
            }

            return res;
        }

        /// <summary>
        /// Check whether array passed as argument is the same as the stream hosts.
        /// </summary>
        /// <param name="arr">Array.</param>
        /// <returns>
        ///   <c>True</c> if they are same.
        /// </returns>
        public bool IsSameArray(byte[] arr)
        {
            return false;
        }

        #endregion
    }
}
