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
    /// Abstract memory chunk.
    /// </summary>
    [CLSCompliant(false)]
    public abstract class PlatformMemory : IPlatformMemory
    {
        /** Memory pointer. */
        protected readonly long MemPtr;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="memPtr">Memory pointer.</param>
        protected PlatformMemory(long memPtr)
        {
            MemPtr = memPtr;
        }

        /** <inheritdoc /> */
        public virtual PlatformMemoryStream Stream()
        {
            return BitConverter.IsLittleEndian ? new PlatformMemoryStream(this) : 
                new PlatformBigEndianMemoryStream(this);
        }

        /** <inheritdoc /> */
        public long Pointer
        {
            get { return MemPtr; }
        }

        /** <inheritdoc /> */
        public long Data
        {
            get { return PlatformMemoryUtils.Data(MemPtr); }
        }

        /** <inheritdoc /> */
        public int Capacity
        {
            get { return PlatformMemoryUtils.Capacity(MemPtr); }
        }

        /** <inheritdoc /> */
        public int Length
        {
            get { return PlatformMemoryUtils.Length(MemPtr); }
            set { PlatformMemoryUtils.Length(MemPtr, value); }
        }

        /** <inheritdoc /> */
        public abstract void Reallocate(int cap);

        /** <inheritdoc /> */
        public abstract void Release();
    }
}
