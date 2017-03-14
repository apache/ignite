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
    using System.Diagnostics.CodeAnalysis;
    using System.IO;
    using System.Text;

    /// <summary>
    /// Stream capable of working with binary objects.
    /// </summary>
    [CLSCompliant(false)]
    [SuppressMessage("Microsoft.Naming", "CA1711:IdentifiersShouldNotHaveIncorrectSuffix")]
    public unsafe interface IBinaryStream : IDisposable
    {
        /// <summary>
        /// Write bool.
        /// </summary>
        /// <param name="val">Bool value.</param>
        void WriteBool(bool val);

        /// <summary>
        /// Read bool.
        /// </summary>
        /// <returns>Bool value.</returns>
        bool ReadBool();

        /// <summary>
        /// Write bool array.
        /// </summary>
        /// <param name="val">Bool array.</param>
        void WriteBoolArray(bool[] val);

        /// <summary>
        /// Read bool array.
        /// </summary>
        /// <param name="cnt">Count.</param>
        /// <returns>Bool array.</returns>
        bool[] ReadBoolArray(int cnt);

        /// <summary>
        /// Write byte.
        /// </summary>
        /// <param name="val">Byte value.</param>
        void WriteByte(byte val);

        /// <summary>
        /// Read byte.
        /// </summary>
        /// <returns>Byte value.</returns>
        byte ReadByte();

        /// <summary>
        /// Write byte array.
        /// </summary>
        /// <param name="val">Byte array.</param>
        void WriteByteArray(byte[] val);

        /// <summary>
        /// Read byte array.
        /// </summary>
        /// <param name="cnt">Count.</param>
        /// <returns>Byte array.</returns>
        byte[] ReadByteArray(int cnt);

        /// <summary>
        /// Write short.
        /// </summary>
        /// <param name="val">Short value.</param>
        void WriteShort(short val);

        /// <summary>
        /// Read short.
        /// </summary>
        /// <returns>Short value.</returns>
        short ReadShort();

        /// <summary>
        /// Write short array.
        /// </summary>
        /// <param name="val">Short array.</param>
        void WriteShortArray(short[] val);

        /// <summary>
        /// Read short array.
        /// </summary>
        /// <param name="cnt">Count.</param>
        /// <returns>Short array.</returns>
        short[] ReadShortArray(int cnt);

        /// <summary>
        /// Write char.
        /// </summary>
        /// <param name="val">Char value.</param>
        void WriteChar(char val);

        /// <summary>
        /// Read char.
        /// </summary>
        /// <returns>Char value.</returns>
        char ReadChar();

        /// <summary>
        /// Write char array.
        /// </summary>
        /// <param name="val">Char array.</param>
        void WriteCharArray(char[] val);

        /// <summary>
        /// Read char array.
        /// </summary>
        /// <param name="cnt">Count.</param>
        /// <returns>Char array.</returns>
        char[] ReadCharArray(int cnt);

        /// <summary>
        /// Write int.
        /// </summary>
        /// <param name="val">Int value.</param>
        void WriteInt(int val);

        /// <summary>
        /// Write int to specific position.
        /// </summary>
        /// <param name="writePos">Position.</param>
        /// <param name="val">Value.</param>
        void WriteInt(int writePos, int val);

        /// <summary>
        /// Read int.
        /// </summary>
        /// <returns>Int value.</returns>
        int ReadInt();

        /// <summary>
        /// Write int array.
        /// </summary>
        /// <param name="val">Int array.</param>
        void WriteIntArray(int[] val);

        /// <summary>
        /// Read int array.
        /// </summary>
        /// <param name="cnt">Count.</param>
        /// <returns>Int array.</returns>
        int[] ReadIntArray(int cnt);
        
        /// <summary>
        /// Write long.
        /// </summary>
        /// <param name="val">Long value.</param>
        void WriteLong(long val);

        /// <summary>
        /// Read long.
        /// </summary>
        /// <returns>Long value.</returns>
        long ReadLong();

        /// <summary>
        /// Write long array.
        /// </summary>
        /// <param name="val">Long array.</param>
        void WriteLongArray(long[] val);

        /// <summary>
        /// Read long array.
        /// </summary>
        /// <param name="cnt">Count.</param>
        /// <returns>Long array.</returns>
        long[] ReadLongArray(int cnt);

        /// <summary>
        /// Write float.
        /// </summary>
        /// <param name="val">Float value.</param>
        void WriteFloat(float val);

        /// <summary>
        /// Read float.
        /// </summary>
        /// <returns>Float value.</returns>
        float ReadFloat();

        /// <summary>
        /// Write float array.
        /// </summary>
        /// <param name="val">Float array.</param>
        void WriteFloatArray(float[] val);

        /// <summary>
        /// Read float array.
        /// </summary>
        /// <param name="cnt">Count.</param>
        /// <returns>Float array.</returns>
        float[] ReadFloatArray(int cnt);

        /// <summary>
        /// Write double.
        /// </summary>
        /// <param name="val">Double value.</param>
        void WriteDouble(double val);

        /// <summary>
        /// Read double.
        /// </summary>
        /// <returns>Double value.</returns>
        double ReadDouble();

        /// <summary>
        /// Write double array.
        /// </summary>
        /// <param name="val">Double array.</param>
        void WriteDoubleArray(double[] val);

        /// <summary>
        /// Read double array.
        /// </summary>
        /// <param name="cnt">Count.</param>
        /// <returns>Double array.</returns>
        double[] ReadDoubleArray(int cnt);

        /// <summary>
        /// Write string.
        /// </summary>
        /// <param name="chars">Characters.</param>
        /// <param name="charCnt">Char count.</param>
        /// <param name="byteCnt">Byte count.</param>
        /// <param name="encoding">Encoding.</param>
        /// <returns>Amounts of bytes written.</returns>
        int WriteString(char* chars, int charCnt, int byteCnt, Encoding encoding);

        /// <summary>
        /// Write arbitrary data.
        /// </summary>
        /// <param name="src">Source array.</param>
        /// <param name="off">Offset</param>
        /// <param name="cnt">Count.</param>
        void Write(byte[] src, int off, int cnt);

        /// <summary>
        /// Read arbitrary data.
        /// </summary>
        /// <param name="dest">Destination array.</param>
        /// <param name="off">Offset.</param>
        /// <param name="cnt">Count.</param>
        /// <returns>Amount of bytes read.</returns>
        void Read(byte[] dest, int off, int cnt);

        /// <summary>
        /// Write arbitrary data.
        /// </summary>
        /// <param name="src">Source.</param>
        /// <param name="cnt">Count.</param>
        void Write(byte* src, int cnt);

        /// <summary>
        /// Read arbitrary data.
        /// </summary>
        /// <param name="dest">Destination.</param>
        /// <param name="cnt">Count.</param>
        void Read(byte* dest, int cnt);
        
        /// <summary>
        /// Position.
        /// </summary>
        int Position
        {
            get;
        }

        /// <summary>
        /// Gets remaining bytes in the stream.
        /// </summary>
        /// <value>Remaining bytes.</value>
        int Remaining { get; }

        /// <summary>
        /// Gets underlying array, avoiding copying if possible.
        /// </summary>
        /// <returns>Underlying array.</returns>
        byte[] GetArray();

        /// <summary>
        /// Gets underlying data in a new array.
        /// </summary>
        /// <returns>New array with data.</returns>
        byte[] GetArrayCopy();
        
        /// <summary>
        /// Check whether array passed as argument is the same as the stream hosts.
        /// </summary>
        /// <param name="arr">Array.</param>
        /// <returns><c>True</c> if they are same.</returns>
        bool IsSameArray(byte[] arr);

        /// <summary>
        /// Seek to the given position.
        /// </summary>
        /// <param name="offset">Offset.</param>
        /// <param name="origin">Seek origin.</param>
        /// <returns>Position.</returns>
        int Seek(int offset, SeekOrigin origin);

        /// <summary>
        /// Applies specified processor to the raw stream data.
        /// </summary>
        T Apply<TArg, T>(IBinaryStreamProcessor<TArg, T> proc, TArg arg);

        /// <summary>
        /// Flushes the data to underlying storage.
        /// </summary>
        void Flush();
    }
}
