﻿/*
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
    /// <summary>
    /// Platform memory stream for big endian platforms.
    /// </summary>
    internal class PlatformBigEndianMemoryStream : PlatformMemoryStream
    {
        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="mem"></param>
        public PlatformBigEndianMemoryStream(IPlatformMemory mem) : base(mem)
        {
            // No-op.
        }

        #region WRITE

        /** <inheritDoc /> */
        public override unsafe void WriteShort(short val)
        {
            byte* curPos = Data + EnsureWriteCapacityAndShift(Len2);

            byte* valPtr = (byte*)&val;

            curPos[0] = valPtr[1];
            curPos[1] = valPtr[0];
        }

        /** <inheritDoc /> */
        public override unsafe void WriteShortArray(short[] val)
        {
            byte* curPos = Data + EnsureWriteCapacityAndShift(val.Length << Shift2);

            for (int i = 0; i < val.Length; i++)
            {
                short val0 = val[i];

                byte* valPtr = (byte*)&(val0);

                *curPos++ = valPtr[1];
                *curPos++ = valPtr[0];
            }
        }

        /** <inheritDoc /> */
        public override unsafe void WriteChar(char val)
        {
            WriteShort(*(short*)(&val));
        }

        /** <inheritDoc /> */
        public override unsafe void WriteCharArray(char[] val)
        {
            byte* curPos = Data + EnsureWriteCapacityAndShift(val.Length << Shift2);

            for (int i = 0; i < val.Length; i++)
            {
                char val0 = val[i];

                byte* valPtr = (byte*)&(val0);

                *curPos++ = valPtr[1];
                *curPos++ = valPtr[0];
            }
        }

        /** <inheritDoc /> */
        public override unsafe void WriteInt(int val)
        {
            byte* curPos = Data + EnsureWriteCapacityAndShift(Len4);

            byte* valPtr = (byte*)&val;

            curPos[0] = valPtr[3];
            curPos[1] = valPtr[2];
            curPos[2] = valPtr[1];
            curPos[3] = valPtr[0];
        }

        /** <inheritDoc /> */
        public override unsafe void WriteInt(int writePos, int val)
        {
            EnsureWriteCapacity(writePos + 4);

            byte* curPos = Data + writePos;

            byte* valPtr = (byte*)&val;

            curPos[0] = valPtr[3];
            curPos[1] = valPtr[2];
            curPos[2] = valPtr[1];
            curPos[3] = valPtr[0];
        }

        /** <inheritDoc /> */
        public override unsafe void WriteIntArray(int[] val)
        {
            byte* curPos = Data + EnsureWriteCapacityAndShift(val.Length << Shift4);

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

        /** <inheritDoc /> */
        public override unsafe void WriteLong(long val)
        {
            byte* curPos = Data + EnsureWriteCapacityAndShift(Len8);

            byte* valPtr = (byte*)&val;

            curPos[0] = valPtr[7];
            curPos[1] = valPtr[6];
            curPos[2] = valPtr[5];
            curPos[3] = valPtr[4];
            curPos[4] = valPtr[3];
            curPos[5] = valPtr[2];
            curPos[6] = valPtr[1];
            curPos[7] = valPtr[0];
        }

        /** <inheritDoc /> */
        public override unsafe void WriteLongArray(long[] val)
        {
            byte* curPos = Data + EnsureWriteCapacityAndShift(val.Length << Shift8);

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

        /** <inheritDoc /> */
        public override unsafe void WriteFloat(float val)
        {
            WriteInt(*(int*)(&val));
        }

        /** <inheritDoc /> */
        public override unsafe void WriteFloatArray(float[] val)
        {
            byte* curPos = Data + EnsureWriteCapacityAndShift(val.Length << Shift4);

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

        /** <inheritDoc /> */
        public override unsafe void WriteDouble(double val)
        {
            WriteLong(*(long*)(&val));
        }

        /** <inheritDoc /> */
        public override unsafe void WriteDoubleArray(double[] val)
        {
            byte* curPos = Data + EnsureWriteCapacityAndShift(val.Length << Shift8);

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

        #endregion

        #region READ

        /** <inheritDoc /> */
        public override unsafe short ReadShort()
        {
            int curPos = EnsureReadCapacityAndShift(Len2);

            short val;

            byte* valPtr = (byte*)&val;

            valPtr[1] = *(Data + curPos++);
            valPtr[0] = *(Data + curPos);

            return val;
        }

        /** <inheritDoc /> */
        public override unsafe short[] ReadShortArray(int len)
        {
            int curPos = EnsureReadCapacityAndShift(len << Shift2);

            short[] res = new short[len];

            for (int i = 0; i < len; i++)
            {
                short val;

                byte* valPtr = (byte*)&val;

                valPtr[1] = *(Data + curPos++);
                valPtr[0] = *(Data + curPos++);

                res[i] = val;
            }

            return res;
        }

        /** <inheritDoc /> */
        public override unsafe char ReadChar()
        {
            int curPos = EnsureReadCapacityAndShift(Len2);

            char val;

            byte* valPtr = (byte*)&val;

            valPtr[1] = *(Data + curPos++);
            valPtr[0] = *(Data + curPos);

            return val;
        }

        /** <inheritDoc /> */
        public override unsafe char[] ReadCharArray(int len)
        {
            int curPos = EnsureReadCapacityAndShift(len << Shift2);

            char[] res = new char[len];

            for (int i = 0; i < len; i++)
            {
                char val;

                byte* valPtr = (byte*)&val;

                valPtr[1] = *(Data + curPos++);
                valPtr[0] = *(Data + curPos++);

                res[i] = val;
            }

            return res;
        }

        /** <inheritDoc /> */
        public override unsafe int ReadInt()
        {
            int curPos = EnsureReadCapacityAndShift(Len4);

            int val;

            byte* valPtr = (byte*)&val;

            valPtr[3] = *(Data + curPos++);
            valPtr[2] = *(Data + curPos++);
            valPtr[1] = *(Data + curPos++);
            valPtr[0] = *(Data + curPos);

            return val;
        }

        /** <inheritDoc /> */
        public override unsafe int[] ReadIntArray(int len)
        {
            int curPos = EnsureReadCapacityAndShift(len << Shift4);

            int[] res = new int[len];

            for (int i = 0; i < len; i++)
            {
                int val;

                byte* valPtr = (byte*)&val;

                valPtr[3] = *(Data + curPos++);
                valPtr[2] = *(Data + curPos++);
                valPtr[1] = *(Data + curPos++);
                valPtr[0] = *(Data + curPos++);

                res[i] = val;
            }

            return res;
        }

        /** <inheritDoc /> */
        public override unsafe long ReadLong()
        {
            int curPos = EnsureReadCapacityAndShift(Len8);

            long val;

            byte* valPtr = (byte*)&val;

            valPtr[7] = *(Data + curPos++);
            valPtr[6] = *(Data + curPos++);
            valPtr[5] = *(Data + curPos++);
            valPtr[4] = *(Data + curPos++);
            valPtr[3] = *(Data + curPos++);
            valPtr[2] = *(Data + curPos++);
            valPtr[1] = *(Data + curPos++);
            valPtr[0] = *(Data + curPos);

            return val;
        }

        /** <inheritDoc /> */

        public override unsafe long[] ReadLongArray(int len)
        {
            int curPos = EnsureReadCapacityAndShift(len << Shift8);

            long[] res = new long[len];

            for (int i = 0; i < len; i++)
            {
                long val;

                byte* valPtr = (byte*) &val;

                valPtr[7] = *(Data + curPos++);
                valPtr[6] = *(Data + curPos++);
                valPtr[5] = *(Data + curPos++);
                valPtr[4] = *(Data + curPos++);
                valPtr[3] = *(Data + curPos++);
                valPtr[2] = *(Data + curPos++);
                valPtr[1] = *(Data + curPos++);
                valPtr[0] = *(Data + curPos++);

                res[i] = val;
            }

            return res;
        }

        /** <inheritDoc /> */
        public override unsafe float ReadFloat()
        {
            int curPos = EnsureReadCapacityAndShift(Len4);

            float val;

            byte* valPtr = (byte*)&val;

            valPtr[3] = *(Data + curPos++);
            valPtr[2] = *(Data + curPos++);
            valPtr[1] = *(Data + curPos++);
            valPtr[0] = *(Data + curPos);

            return val;
        }

        /** <inheritDoc /> */
        public override unsafe float[] ReadFloatArray(int len)
        {
            int curPos = EnsureReadCapacityAndShift(len << Shift4);

            float[] res = new float[len];

            for (int i = 0; i < len; i++)
            {
                float val;

                byte* valPtr = (byte*)&val;

                valPtr[3] = *(Data + curPos++);
                valPtr[2] = *(Data + curPos++);
                valPtr[1] = *(Data + curPos++);
                valPtr[0] = *(Data + curPos++);

                res[i] = val;
            }

            return res;
        }

        /** <inheritDoc /> */
        public override unsafe double ReadDouble()
        {
            int curPos = EnsureReadCapacityAndShift(Len8);

            double val;

            byte* valPtr = (byte*)&val;

            valPtr[7] = *(Data + curPos++);
            valPtr[6] = *(Data + curPos++);
            valPtr[5] = *(Data + curPos++);
            valPtr[4] = *(Data + curPos++);
            valPtr[3] = *(Data + curPos++);
            valPtr[2] = *(Data + curPos++);
            valPtr[1] = *(Data + curPos++);
            valPtr[0] = *(Data + curPos);

            return val;
        }

        /** <inheritDoc /> */
        public override unsafe double[] ReadDoubleArray(int len)
        {
            int curPos = EnsureReadCapacityAndShift(len << Shift8);

            double[] res = new double[len];

            for (int i = 0; i < len; i++)
            {
                double val;

                byte* valPtr = (byte*)&val;

                valPtr[7] = *(Data + curPos++);
                valPtr[6] = *(Data + curPos++);
                valPtr[5] = *(Data + curPos++);
                valPtr[4] = *(Data + curPos++);
                valPtr[3] = *(Data + curPos++);
                valPtr[2] = *(Data + curPos++);
                valPtr[1] = *(Data + curPos++);
                valPtr[0] = *(Data + curPos++);

                res[i] = val;
            }

            return res;
        }

        #endregion
    }
}
