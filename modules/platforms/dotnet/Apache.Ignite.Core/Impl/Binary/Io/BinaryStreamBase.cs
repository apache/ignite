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
    using System.IO;
    using System.Text;
    using Apache.Ignite.Core.Impl.Memory;

    /// <summary>
    /// Base class for managed and unmanaged data streams.
    /// </summary>
    internal abstract unsafe class BinaryStreamBase : IBinaryStream
    {
        /** Byte: zero. */
        private const byte ByteZero = 0;

        /** Byte: one. */
        private const byte ByteOne = 1;

        /** LITTLE_ENDIAN flag. */
        private static readonly bool LittleEndian = BitConverter.IsLittleEndian;

        /** Position. */
        protected int Pos;

        /** Disposed flag. */
        private bool _disposed;

        /// <summary>
        /// Write byte.
        /// </summary>
        /// <param name="val">Byte value.</param>
        public abstract void WriteByte(byte val);

        /// <summary>
        /// Read byte.
        /// </summary>
        /// <returns>
        /// Byte value.
        /// </returns>
        public abstract byte ReadByte();

        /// <summary>
        /// Write byte array.
        /// </summary>
        /// <param name="val">Byte array.</param>
        public abstract void WriteByteArray(byte[] val);

        /// <summary>
        /// Internal routine to write byte array.
        /// </summary>
        /// <param name="val">Byte array.</param>
        /// <param name="data">Data pointer.</param>
        protected static void WriteByteArray0(byte[] val, byte* data)
        {
            fixed (byte* val0 = val)
            {
                CopyMemory(val0, data, val.Length);
            }
        }

        /// <summary>
        /// Read byte array.
        /// </summary>
        /// <param name="cnt">Count.</param>
        /// <returns>
        /// Byte array.
        /// </returns>
        public abstract byte[] ReadByteArray(int cnt);

        /// <summary>
        /// Internal routine to read byte array.
        /// </summary>
        /// <param name="len">Array length.</param>
        /// <param name="data">Data pointer.</param>
        /// <returns>Byte array</returns>
        protected static byte[] ReadByteArray0(int len, byte* data)
        {
            byte[] res = new byte[len];

            fixed (byte* res0 = res)
            {
                CopyMemory(data, res0, len);
            }

            return res;
        }

        /// <summary>
        /// Write bool.
        /// </summary>
        /// <param name="val">Bool value.</param>
        public void WriteBool(bool val)
        {
            WriteByte(val ? ByteOne : ByteZero);
        }

        /// <summary>
        /// Read bool.
        /// </summary>
        /// <returns>
        /// Bool value.
        /// </returns>
        public bool ReadBool()
        {
            return ReadByte() == ByteOne;
        }

        /// <summary>
        /// Write bool array.
        /// </summary>
        /// <param name="val">Bool array.</param>
        public abstract void WriteBoolArray(bool[] val);

        /// <summary>
        /// Internal routine to write bool array.
        /// </summary>
        /// <param name="val">Bool array.</param>
        /// <param name="data">Data pointer.</param>
        protected static void WriteBoolArray0(bool[] val, byte* data)
        {
            fixed (bool* val0 = val)
            {
                CopyMemory((byte*)val0, data, val.Length);
            }
        }

        /// <summary>
        /// Read bool array.
        /// </summary>
        /// <param name="cnt">Count.</param>
        /// <returns>
        /// Bool array.
        /// </returns>
        public abstract bool[] ReadBoolArray(int cnt);

        /// <summary>
        /// Internal routine to read bool array.
        /// </summary>
        /// <param name="len">Array length.</param>
        /// <param name="data">Data pointer.</param>
        /// <returns>Bool array</returns>
        protected static bool[] ReadBoolArray0(int len, byte* data)
        {
            bool[] res = new bool[len];

            fixed (bool* res0 = res)
            {
                CopyMemory(data, (byte*)res0, len);
            }

            return res;
        }

        /// <summary>
        /// Write short.
        /// </summary>
        /// <param name="val">Short value.</param>
        public abstract void WriteShort(short val);

        /// <summary>
        /// Internal routine to write short value.
        /// </summary>
        /// <param name="val">Short value.</param>
        /// <param name="data">Data pointer.</param>
        protected static void WriteShort0(short val, byte* data)
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
        /// Read short.
        /// </summary>
        /// <returns>
        /// Short value.
        /// </returns>
        public abstract short ReadShort();

        /// <summary>
        /// Internal routine to read short value.
        /// </summary>
        /// <param name="data">Data pointer.</param>
        /// <returns>Short value</returns>
        protected static short ReadShort0(byte* data)
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
        /// Write short array.
        /// </summary>
        /// <param name="val">Short array.</param>
        public abstract void WriteShortArray(short[] val);

        /// <summary>
        /// Internal routine to write short array.
        /// </summary>
        /// <param name="val">Short array.</param>
        /// <param name="data">Data pointer.</param>
        /// <param name="cnt">Bytes count.</param>
        protected static void WriteShortArray0(short[] val, byte* data, int cnt)
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
        /// Read short array.
        /// </summary>
        /// <param name="cnt">Count.</param>
        /// <returns>
        /// Short array.
        /// </returns>
        public abstract short[] ReadShortArray(int cnt);

        /// <summary>
        /// Internal routine to read short array.
        /// </summary>
        /// <param name="len">Array length.</param>
        /// <param name="data">Data pointer.</param>
        /// <param name="cnt">Bytes count.</param>
        /// <returns>Short array</returns>
        protected static short[] ReadShortArray0(int len, byte* data, int cnt)
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

        /// <summary>
        /// Write char.
        /// </summary>
        /// <param name="val">Char value.</param>
        public void WriteChar(char val)
        {
            WriteShort(*(short*)(&val));
        }

        /// <summary>
        /// Read char.
        /// </summary>
        /// <returns>
        /// Char value.
        /// </returns>
        public char ReadChar()
        {
            short val = ReadShort();

            return *(char*)(&val);
        }

        /// <summary>
        /// Write char array.
        /// </summary>
        /// <param name="val">Char array.</param>
        public abstract void WriteCharArray(char[] val);

        /// <summary>
        /// Internal routine to write char array.
        /// </summary>
        /// <param name="val">Char array.</param>
        /// <param name="data">Data pointer.</param>
        /// <param name="cnt">Bytes count.</param>
        protected static void WriteCharArray0(char[] val, byte* data, int cnt)
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
        /// Read char array.
        /// </summary>
        /// <param name="cnt">Count.</param>
        /// <returns>
        /// Char array.
        /// </returns>
        public abstract char[] ReadCharArray(int cnt);

        /// <summary>
        /// Internal routine to read char array.
        /// </summary>
        /// <param name="len">Count.</param>
        /// <param name="data">Data pointer.</param>
        /// <param name="cnt">Bytes count.</param>
        /// <returns>Char array</returns>
        protected static char[] ReadCharArray0(int len, byte* data, int cnt)
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
        /// Write int.
        /// </summary>
        /// <param name="val">Int value.</param>
        public abstract void WriteInt(int val);

        /// <summary>
        /// Write int to specific position.
        /// </summary>
        /// <param name="writePos">Position.</param>
        /// <param name="val">Value.</param>
        public abstract void WriteInt(int writePos, int val);

        /// <summary>
        /// Internal routine to write int value.
        /// </summary>
        /// <param name="val">Int value.</param>
        /// <param name="data">Data pointer.</param>
        protected static void WriteInt0(int val, byte* data)
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
        /// Read int.
        /// </summary>
        /// <returns>
        /// Int value.
        /// </returns>
        public abstract int ReadInt();

        /// <summary>
        /// Internal routine to read int value.
        /// </summary>
        /// <param name="data">Data pointer.</param>
        /// <returns>Int value</returns>
        protected static int ReadInt0(byte* data) {
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
        /// Write int array.
        /// </summary>
        /// <param name="val">Int array.</param>
        public abstract void WriteIntArray(int[] val);

        /// <summary>
        /// Internal routine to write int array.
        /// </summary>
        /// <param name="val">Int array.</param>
        /// <param name="data">Data pointer.</param>
        /// <param name="cnt">Bytes count.</param>
        protected static void WriteIntArray0(int[] val, byte* data, int cnt)
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
        /// Read int array.
        /// </summary>
        /// <param name="cnt">Count.</param>
        /// <returns>
        /// Int array.
        /// </returns>
        public abstract int[] ReadIntArray(int cnt);

        /// <summary>
        /// Internal routine to read int array.
        /// </summary>
        /// <param name="len">Count.</param>
        /// <param name="data">Data pointer.</param>
        /// <param name="cnt">Bytes count.</param>
        /// <returns>Int array</returns>
        protected static int[] ReadIntArray0(int len, byte* data, int cnt)
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

        /// <summary>
        /// Write float.
        /// </summary>
        /// <param name="val">Float value.</param>
        public void WriteFloat(float val)
        {
            int val0 = *(int*)(&val);

            WriteInt(val0);
        }

        /// <summary>
        /// Read float.
        /// </summary>
        /// <returns>
        /// Float value.
        /// </returns>
        public float ReadFloat()
        {
            int val = ReadInt();

            return BinaryUtils.IntToFloatBits(val);
        }

        /// <summary>
        /// Write float array.
        /// </summary>
        /// <param name="val">Float array.</param>
        public abstract void WriteFloatArray(float[] val);

        /// <summary>
        /// Internal routine to write float array.
        /// </summary>
        /// <param name="val">Int array.</param>
        /// <param name="data">Data pointer.</param>
        /// <param name="cnt">Bytes count.</param>
        protected static void WriteFloatArray0(float[] val, byte* data, int cnt)
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
        /// Read float array.
        /// </summary>
        /// <param name="cnt">Count.</param>
        /// <returns>
        /// Float array.
        /// </returns>
        public abstract float[] ReadFloatArray(int cnt);

        /// <summary>
        /// Internal routine to read float array.
        /// </summary>
        /// <param name="len">Count.</param>
        /// <param name="data">Data pointer.</param>
        /// <param name="cnt">Bytes count.</param>
        /// <returns>Float array</returns>
        protected static float[] ReadFloatArray0(int len, byte* data, int cnt)
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
        /// Write long.
        /// </summary>
        /// <param name="val">Long value.</param>
        public abstract void WriteLong(long val);

        /// <summary>
        /// Internal routine to write long value.
        /// </summary>
        /// <param name="val">Long value.</param>
        /// <param name="data">Data pointer.</param>
        protected static void WriteLong0(long val, byte* data)
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
        /// Read long.
        /// </summary>
        /// <returns>
        /// Long value.
        /// </returns>
        public abstract long ReadLong();

        /// <summary>
        /// Internal routine to read long value.
        /// </summary>
        /// <param name="data">Data pointer.</param>
        /// <returns>Long value</returns>
        protected static long ReadLong0(byte* data)
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
        /// Write long array.
        /// </summary>
        /// <param name="val">Long array.</param>
        public abstract void WriteLongArray(long[] val);

        /// <summary>
        /// Internal routine to write long array.
        /// </summary>
        /// <param name="val">Long array.</param>
        /// <param name="data">Data pointer.</param>
        /// <param name="cnt">Bytes count.</param>
        protected static void WriteLongArray0(long[] val, byte* data, int cnt)
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
        /// Read long array.
        /// </summary>
        /// <param name="cnt">Count.</param>
        /// <returns>
        /// Long array.
        /// </returns>
        public abstract long[] ReadLongArray(int cnt);

        /// <summary>
        /// Internal routine to read long array.
        /// </summary>
        /// <param name="len">Count.</param>
        /// <param name="data">Data pointer.</param>
        /// <param name="cnt">Bytes count.</param>
        /// <returns>Long array</returns>
        protected static long[] ReadLongArray0(int len, byte* data, int cnt)
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

        /// <summary>
        /// Write double.
        /// </summary>
        /// <param name="val">Double value.</param>
        public void WriteDouble(double val)
        {
            long val0 = *(long*)(&val);

            WriteLong(val0);
        }

        /// <summary>
        /// Read double.
        /// </summary>
        /// <returns>
        /// Double value.
        /// </returns>
        public double ReadDouble()
        {
            long val = ReadLong();

            return BinaryUtils.LongToDoubleBits(val);
        }

        /// <summary>
        /// Write double array.
        /// </summary>
        /// <param name="val">Double array.</param>
        public abstract void WriteDoubleArray(double[] val);

        /// <summary>
        /// Internal routine to write double array.
        /// </summary>
        /// <param name="val">Double array.</param>
        /// <param name="data">Data pointer.</param>
        /// <param name="cnt">Bytes count.</param>
        protected static void WriteDoubleArray0(double[] val, byte* data, int cnt)
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
        /// Read double array.
        /// </summary>
        /// <param name="cnt">Count.</param>
        /// <returns>
        /// Double array.
        /// </returns>
        public abstract double[] ReadDoubleArray(int cnt);

        /// <summary>
        /// Internal routine to read double array.
        /// </summary>
        /// <param name="len">Count.</param>
        /// <param name="data">Data pointer.</param>
        /// <param name="cnt">Bytes count.</param>
        /// <returns>Double array</returns>
        protected static double[] ReadDoubleArray0(int len, byte* data, int cnt)
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
        public abstract int WriteString(char* chars, int charCnt, int byteCnt, Encoding encoding);

        /// <summary>
        /// Write arbitrary data.
        /// </summary>
        /// <param name="src">Source array.</param>
        /// <param name="off">Offset</param>
        /// <param name="cnt">Count.</param>
        public void Write(byte[] src, int off, int cnt)
        {
            fixed (byte* src0 = src)
            {
                Write(src0 + off, cnt);
            }
        }

        /// <summary>
        /// Read arbitrary data.
        /// </summary>
        /// <param name="dest">Destination array.</param>
        /// <param name="off">Offset.</param>
        /// <param name="cnt">Count.</param>
        /// <returns>
        /// Amount of bytes read.
        /// </returns>
        public void Read(byte[] dest, int off, int cnt)
        {
            fixed (byte* dest0 = dest)
            {
                Read(dest0 + off, cnt);
            }
        }

        /// <summary>
        /// Write arbitrary data.
        /// </summary>
        /// <param name="src">Source.</param>
        /// <param name="cnt">Count.</param>
        public abstract void Write(byte* src, int cnt);

        /// <summary>
        /// Internal write routine.
        /// </summary>
        /// <param name="src">Source.</param>
        /// <param name="cnt">Count.</param>
        /// <param name="data">Data (dsetination).</param>
        protected void WriteInternal(byte* src, int cnt, byte* data)
        {
            CopyMemory(src, data + Pos, cnt);
        }

        /// <summary>
        /// Read arbitrary data.
        /// </summary>
        /// <param name="dest">Destination.</param>
        /// <param name="cnt">Count.</param>
        /// <returns></returns>
        public abstract void Read(byte* dest, int cnt);

        /// <summary>
        /// Internal read routine.
        /// </summary>
        /// <param name="src">Source</param>
        /// <param name="dest">Destination.</param>
        /// <param name="cnt">Count.</param>
        /// <returns>Amount of bytes written.</returns>
        protected void ReadInternal(byte* src, byte* dest, int cnt)
        {
            int cnt0 = Math.Min(Remaining, cnt);

            CopyMemory(src + Pos, dest, cnt0);

            ShiftRead(cnt0);
        }

        /// <summary>
        /// Position.
        /// </summary>
        public int Position
        {
            get { return Pos; }
        }

        /// <summary>
        /// Gets remaining bytes in the stream.
        /// </summary>
        /// <value>
        ///     Remaining bytes.
        /// </value>
        public abstract int Remaining { get; }

        /// <summary>
        /// Gets underlying array, avoiding copying if possible.
        /// </summary>
        /// <returns>
        /// Underlying array.
        /// </returns>
        public abstract byte[] GetArray();

        /// <summary>
        /// Gets underlying data in a new array.
        /// </summary>
        /// <returns>
        /// New array with data.
        /// </returns>
        public abstract byte[] GetArrayCopy();

        /// <summary>
        /// Check whether array passed as argument is the same as the stream hosts.
        /// </summary>
        /// <param name="arr">Array.</param>
        /// <returns>
        ///   <c>True</c> if they are same.
        /// </returns>
        public abstract bool IsSameArray(byte[] arr);
        
        /// <summary>
        /// Seek to the given position.
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
                        newPos = Pos + offset;

                        break;
                    }

                default:
                    throw new ArgumentException("Unsupported seek origin: " + origin);
            }

            if (newPos < 0)
                throw new ArgumentException("Seek before origin: " + newPos);

            EnsureWriteCapacity(newPos);

            Pos = newPos;

            return Pos;
        }

        /// <summary>
        /// Returns a hash code for the specified byte range.
        /// </summary>
        public abstract T Apply<TArg, T>(IBinaryStreamProcessor<TArg, T> proc, TArg arg);

        /// <summary>
        /// Flushes the data to underlying storage.
        /// </summary>
        public void Flush()
        {
            // No-op.
        }

        /** <inheritdoc /> */
        public void Dispose()
        {
            if (_disposed)
                return;

            Dispose(true);

            GC.SuppressFinalize(this);

            _disposed = true;
        }

        /// <summary>
        /// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
        /// </summary>
        protected abstract void Dispose(bool disposing);

        /// <summary>
        /// Ensure capacity for write.
        /// </summary>
        /// <param name="cnt">Bytes count.</param>
        protected abstract void EnsureWriteCapacity(int cnt);

        /// <summary>
        /// Ensure capacity for write and shift position.
        /// </summary>
        /// <param name="cnt">Bytes count.</param>
        /// <returns>Position before shift.</returns>
        protected int EnsureWriteCapacityAndShift(int cnt)
        {
            int pos0 = Pos;

            EnsureWriteCapacity(Pos + cnt);

            ShiftWrite(cnt);

            return pos0;
        }

        /// <summary>
        /// Ensure capacity for read.
        /// </summary>
        /// <param name="cnt">Bytes count.</param>
        protected abstract void EnsureReadCapacity(int cnt);

        /// <summary>
        /// Ensure capacity for read and shift position.
        /// </summary>
        /// <param name="cnt">Bytes count.</param>
        /// <returns>Position before shift.</returns>
        protected int EnsureReadCapacityAndShift(int cnt)
        {
            int pos0 = Pos;

            EnsureReadCapacity(cnt);

            ShiftRead(cnt);

            return pos0;
        }

        /// <summary>
        /// Shift position due to write
        /// </summary>
        /// <param name="cnt">Bytes count.</param>
        protected void ShiftWrite(int cnt)
        {
            Pos += cnt;
        }

        /// <summary>
        /// Shift position due to read.
        /// </summary>
        /// <param name="cnt">Bytes count.</param>
        private void ShiftRead(int cnt)
        {
            Pos += cnt;
        }

        /// <summary>
        /// Calculate new capacity.
        /// </summary>
        /// <param name="curCap">Current capacity.</param>
        /// <param name="reqCap">Required capacity.</param>
        /// <returns>New capacity.</returns>
        protected static int Capacity(int curCap, int reqCap)
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
    }
}
