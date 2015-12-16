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
    using System.Diagnostics;
    using Microsoft.Win32.SafeHandles;

    /// <summary>
    /// Platform memory pool.
    /// </summary>
    [CLSCompliant(false)]
    public unsafe class PlatformMemoryPool : SafeHandleMinusOneIsInvalid
    {
        private readonly PlatformMemoryHeader* _hdr;

        /** Pooled memory chunks. */
        private readonly PlatformPooledMemory[] _mem = new PlatformPooledMemory[PlatformMemoryUtils.PoolSize];

        /// <summary>
        /// Constructor.
        /// </summary>
        public PlatformMemoryPool() : base(true)
        {
            _hdr = PlatformMemoryUtils.AllocatePool();
            handle = (IntPtr) _hdr;
        }

        /// <summary>
        /// Allocate memory chunk, optionally pooling it.
        /// </summary>
        /// <param name="cap">Minimum capacity.</param>
        /// <returns>Memory chunk</returns>
        public PlatformMemory Allocate(int cap)
        {
            var memPtr = PlatformMemoryUtils.AllocatePooled(_hdr, cap);

            // memPtr == 0 means that we failed to acquire thread-local memory chunk, so fallback to unpooled memory.
            return memPtr != (void*) 0
                ? Get(memPtr)
                : new PlatformUnpooledMemory(PlatformMemoryUtils.AllocateUnpooled(cap));
        }

        /// <summary>
        /// Re-allocate existing pool memory chunk.
        /// </summary>
        /// <param name="memPtr">Memory pointer.</param>
        /// <param name="cap">Minimum capacity.</param>
        public static void Reallocate(PlatformMemoryHeader* memPtr, int cap)
        {
            PlatformMemoryUtils.ReallocatePooled(memPtr, cap);
        }

        /// <summary>
        /// Release pooled memory chunk.
        /// </summary>
        /// <param name="memPtr">Memory pointer.</param>
        public static void Release(PlatformMemoryHeader* memPtr)
        {
            PlatformMemoryUtils.ReleasePooled(memPtr);
        }

        /// <summary>
        /// Get pooled memory chunk.
        /// </summary>
        /// <param name="memPtr">Memory pointer.</param>
        /// <returns>Memory chunk.</returns>
        public PlatformMemory Get(PlatformMemoryHeader* memPtr) 
        {
            for (var i = 0; i < _mem.Length; i++)
            {
                if (memPtr == _hdr + i)
                {
                    _mem[i] = _mem[i] ?? new PlatformPooledMemory(memPtr);

                    return _mem[i];
                }
            }

            Debug.Fail("Failed to find pooled memory chunk by a pointer.");
            
            return null;
        }

        /** <inheritdoc /> */
        protected override bool ReleaseHandle()
        {
            PlatformMemoryUtils.ReleasePool(_hdr);

            handle = new IntPtr(-1);

            return true;
        }
    }
}
