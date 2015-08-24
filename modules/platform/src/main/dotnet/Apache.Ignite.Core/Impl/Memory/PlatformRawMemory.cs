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

    /// <summary>
    /// Non-resizeable raw memory chunk without metadata header.
    /// </summary>
    public class PlatformRawMemory : IPlatformMemory
    {
        /** */
        private readonly long _memPtr;

        /** */
        private readonly int _size;

        /// <summary>
        /// Initializes a new instance of the <see cref="PlatformRawMemory"/> class.
        /// </summary>
        /// <param name="memPtr">Heap pointer.</param>
        /// <param name="size">Size.</param>
        public unsafe PlatformRawMemory(void* memPtr, int size)
        {
            _memPtr = (long) memPtr;
            _size = size;
        }

        /** <inheritdoc /> */
        public PlatformMemoryStream Stream()
        {
            return BitConverter.IsLittleEndian ? new PlatformMemoryStream(this) :
                new PlatformBigEndianMemoryStream(this);
        }

        /** <inheritdoc /> */
        public long Pointer
        {
            get { throw new NotSupportedException(); }
        }

        /** <inheritdoc /> */
        public long Data
        {
            get { return _memPtr; }
        }

        /** <inheritdoc /> */
        public int Capacity
        {
            get { return _size; }
        }

        /** <inheritdoc /> */
        public int Length
        {
            get { return _size; }
            set { throw new NotSupportedException(); }
        }

        /** <inheritdoc /> */
        public void Reallocate(int cap)
        {
            throw new NotSupportedException();
        }

        /** <inheritdoc /> */
        public void Release()
        {
            throw new NotSupportedException();
        }
    }
}