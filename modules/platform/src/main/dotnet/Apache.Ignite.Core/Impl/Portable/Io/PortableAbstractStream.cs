/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace Apache.Ignite.Core.Impl.Portable.IO
{
    using System;
    using System.Diagnostics.CodeAnalysis;
    using System.IO;
    using System.Reflection;
    using System.Text;

    /// <summary>
    /// Base class for managed and unmanaged data streams.
    /// </summary>
    [CLSCompliant(false)]
    public unsafe abstract class PortableAbstractStream : IPortableStream
    {
        /// <summary>
        /// Array copy delegate.
        /// </summary>
        delegate void MemCopy(byte* a1, byte* a2, int len);

        /** memcpy function handle. */
        private static readonly MemCopy MEMCPY;

        /** Whether src and dest arguments are inverted. */
        private static readonly bool MEMCPY_INVERTED;

        /** Byte: zero. */
        protected const byte BYTE_ZERO = 0;

        /** Byte: one. */
        protected const byte BYTE_ONE = 1;

        /** LITTLE_ENDIAN flag. */
        protected static readonly bool LITTLE_ENDIAN = BitConverter.IsLittleEndian;

        /** Position. */
        protected int pos;

        /** Disposed flag. */
        private bool disposed;

        /// <summary>
        /// Static initializer.
        /// </summary>
        [SuppressMessage("Microsoft.Design", "CA1065:DoNotRaiseExceptionsInUnexpectedLocations")]
        static PortableAbstractStream()
        {
            Type type = typeof(Buffer);

            const BindingFlags flags = BindingFlags.Static | BindingFlags.NonPublic;
            Type[] paramTypes = { typeof(byte*), typeof(byte*), typeof(int) };

            // Assume .Net 4.5.
            MethodInfo mthd = type.GetMethod("Memcpy", flags, null, paramTypes, null);

            MEMCPY_INVERTED = true;

            if (mthd == null)
            {
                // Assume .Net 4.0.
                mthd = type.GetMethod("memcpyimpl", flags, null, paramTypes, null);

                MEMCPY_INVERTED = false;

                if (mthd == null)
                    throw new InvalidOperationException("Unable to get memory copy function delegate.");
            }

            MEMCPY = (MemCopy)Delegate.CreateDelegate(typeof(MemCopy), mthd);
        }

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
        protected void WriteByteArray0(byte[] val, byte* data)
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
        protected byte[] ReadByteArray0(int len, byte* data)
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
            WriteByte(val ? BYTE_ONE : BYTE_ZERO);
        }

        /// <summary>
        /// Read bool.
        /// </summary>
        /// <returns>
        /// Bool value.
        /// </returns>
        public bool ReadBool()
        {
            return ReadByte() == BYTE_ONE;
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
        protected void WriteBoolArray0(bool[] val, byte* data)
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
        protected bool[] ReadBoolArray0(int len, byte* data)
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
        protected void WriteShort0(short val, byte* data)
        {
            if (LITTLE_ENDIAN)
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
        protected short ReadShort0(byte* data)
        {
            short val;

            if (LITTLE_ENDIAN)
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
        protected void WriteShortArray0(short[] val, byte* data, int cnt)
        {
            if (LITTLE_ENDIAN)
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
        protected short[] ReadShortArray0(int len, byte* data, int cnt)
        {
            short[] res = new short[len];

            if (LITTLE_ENDIAN)
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
        protected void WriteCharArray0(char[] val, byte* data, int cnt)
        {
            if (LITTLE_ENDIAN)
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
        protected char[] ReadCharArray0(int len, byte* data, int cnt)
        {
            char[] res = new char[len];

            if (LITTLE_ENDIAN)
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
        protected void WriteInt0(int val, byte* data)
        {
            if (LITTLE_ENDIAN)
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
        protected int ReadInt0(byte* data) {
            int val;

            if (LITTLE_ENDIAN)
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
        protected void WriteIntArray0(int[] val, byte* data, int cnt)
        {
            if (LITTLE_ENDIAN)
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
        protected int[] ReadIntArray0(int len, byte* data, int cnt)
        {
            int[] res = new int[len];

            if (LITTLE_ENDIAN)
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

            return *(float*)(&val);
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
        protected void WriteFloatArray0(float[] val, byte* data, int cnt)
        {
            if (LITTLE_ENDIAN)
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
        protected float[] ReadFloatArray0(int len, byte* data, int cnt)
        {
            float[] res = new float[len];

            if (LITTLE_ENDIAN)
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
        protected void WriteLong0(long val, byte* data)
        {
            if (LITTLE_ENDIAN)
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
        protected long ReadLong0(byte* data)
        {
            long val;

            if (LITTLE_ENDIAN)
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
        protected void WriteLongArray0(long[] val, byte* data, int cnt)
        {
            if (LITTLE_ENDIAN)
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
        protected long[] ReadLongArray0(int len, byte* data, int cnt)
        {
            long[] res = new long[len];

            if (LITTLE_ENDIAN)
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

            return *(double*)(&val);
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
        protected void WriteDoubleArray0(double[] val, byte* data, int cnt)
        {
            if (LITTLE_ENDIAN)
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
        protected double[] ReadDoubleArray0(int len, byte* data, int cnt)
        {
            double[] res = new double[len];

            if (LITTLE_ENDIAN)
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
        /// Internal string write routine.
        /// </summary>
        /// <param name="chars">Chars.</param>
        /// <param name="charCnt">Chars count.</param>
        /// <param name="byteCnt">Bytes count.</param>
        /// <param name="enc">Encoding.</param>
        /// <param name="data">Data.</param>
        /// <returns>Amount of bytes written.</returns>
        protected int WriteString0(char* chars, int charCnt, int byteCnt, Encoding enc, byte* data)
        {
            return enc.GetBytes(chars, charCnt, data, byteCnt);
        }

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
            CopyMemory(src, data + pos, cnt);
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
        /// <param name="dest">Destination.</param>
        /// <param name="cnt">Count.</param>
        /// <param name="data">Data (source).</param>
        /// <returns>Amount of bytes written.</returns>
        protected void ReadInternal(byte* dest, int cnt, byte* data)
        {
            int cnt0 = Math.Min(Remaining(), cnt);

            CopyMemory(data + pos, dest, cnt0);

            ShiftRead(cnt0);
        }

        /// <summary>
        /// Position.
        /// </summary>
        public int Position
        {
            get { return pos; }
        }

        /// <summary>
        /// Gets remaining bytes in the stream.
        /// </summary>
        /// <returns>
        /// Remaining bytes.
        /// </returns>
        public abstract int Remaining();

        /// <summary>
        /// Gets underlying array, avoiding copying if possible.
        /// </summary>
        /// <returns>
        /// Underlying array.
        /// </returns>
        public abstract byte[] Array();

        /// <summary>
        /// Gets underlying data in a new array.
        /// </summary>
        /// <returns>
        /// New array with data.
        /// </returns>
        public abstract byte[] ArrayCopy();

        /// <summary>
        /// Check whether array passed as argument is the same as the stream hosts.
        /// </summary>
        /// <param name="arr">Array.</param>
        /// <returns>
        ///   <c>True</c> if they are same.
        /// </returns>
        public virtual bool IsSameArray(byte[] arr)
        {
            return false;
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
                        newPos = pos + offset;

                        break;
                    }

                default:
                    throw new ArgumentException("Unsupported seek origin: " + origin);
            }

            if (newPos < 0)
                throw new ArgumentException("Seek before origin: " + newPos);

            EnsureWriteCapacity(newPos);

            pos = newPos;

            return pos;
        }

        /** <inheritdoc /> */
        public void Dispose()
        {
            if (disposed)
                return;

            Dispose(true);

            GC.SuppressFinalize(this);

            disposed = true;
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
            int pos0 = pos;

            EnsureWriteCapacity(pos + cnt);

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
            int pos0 = pos;

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
            pos += cnt;
        }

        /// <summary>
        /// Shift position due to read.
        /// </summary>
        /// <param name="cnt">Bytes count.</param>
        protected void ShiftRead(int cnt)
        {
            pos += cnt;
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
        public static void CopyMemory(byte* src, byte* dest, int len)
        {
            if (MEMCPY_INVERTED)
                MEMCPY.Invoke(dest, src, len);
            else
                MEMCPY.Invoke(src, dest, len);
        }
    }
}
