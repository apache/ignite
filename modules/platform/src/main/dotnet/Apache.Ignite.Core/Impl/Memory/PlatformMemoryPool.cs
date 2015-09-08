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
    using Microsoft.Win32.SafeHandles;

    /// <summary>
    /// Platform memory pool.
    /// </summary>
    [CLSCompliant(false)]
    public class PlatformMemoryPool : SafeHandleMinusOneIsInvalid
    {
        /** First pooled memory chunk. */
        private PlatformPooledMemory _mem1;

        /** Second pooled memory chunk. */
        private PlatformPooledMemory _mem2;

        /** Third pooled memory chunk. */
        private PlatformPooledMemory _mem3;

        /// <summary>
        /// Constructor.
        /// </summary>
        public PlatformMemoryPool() : base(true)
        {
            handle = (IntPtr)PlatformMemoryUtils.AllocatePool();
        }

        /// <summary>
        /// Allocate memory chunk, optionally pooling it.
        /// </summary>
        /// <param name="cap">Minimum capacity.</param>
        /// <returns>Memory chunk</returns>
        public PlatformMemory Allocate(int cap)
        {
            var memPtr = PlatformMemoryUtils.AllocatePooled(handle.ToInt64(), cap);

            // memPtr == 0 means that we failed to acquire thread-local memory chunk, so fallback to unpooled memory.
            return memPtr != 0 ? Get(memPtr) : new PlatformUnpooledMemory(PlatformMemoryUtils.AllocateUnpooled(cap));
        }

        /// <summary>
        /// Re-allocate existing pool memory chunk.
        /// </summary>
        /// <param name="memPtr">Memory pointer.</param>
        /// <param name="cap">Minimum capacity.</param>
        public void Reallocate(long memPtr, int cap)
        {
            PlatformMemoryUtils.ReallocatePooled(memPtr, cap);
        }

        /// <summary>
        /// Release pooled memory chunk.
        /// </summary>
        /// <param name="memPtr">Memory pointer.</param>
        public void Release(long memPtr)
        {
            PlatformMemoryUtils.ReleasePooled(memPtr);
        }

        /// <summary>
        /// Get pooled memory chunk.
        /// </summary>
        /// <param name="memPtr">Memory pointer.</param>
        /// <returns>Memory chunk.</returns>
        public PlatformMemory Get(long memPtr) 
        {
            long delta = memPtr - handle.ToInt64();

            if (delta == PlatformMemoryUtils.PoolHdrOffMem1) 
                return _mem1 ?? (_mem1 = new PlatformPooledMemory(this, memPtr));
            
            if (delta == PlatformMemoryUtils.PoolHdrOffMem2) 
                return _mem2 ?? (_mem2 = new PlatformPooledMemory(this, memPtr));

            return _mem3 ?? (_mem3 = new PlatformPooledMemory(this, memPtr));
        }

        /** <inheritdoc /> */
        protected override bool ReleaseHandle()
        {
            PlatformMemoryUtils.ReleasePool(handle.ToInt64());

            handle = new IntPtr(-1);

            return true;
        }
    }
}
