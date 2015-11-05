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
    using System.Diagnostics.CodeAnalysis;
    using System.Reflection;
    using System.Runtime.InteropServices;

    /// <summary>
    /// Utility methods for platform memory management.
    /// </summary>
    [CLSCompliant(false)]
    public static unsafe class PlatformMemoryUtils
    {
        #region CONSTANTS

        /** Memory chunk header length. */
        private const int MemHdrLen = 20;

        /** Pooled items count. */
        internal const int PoolSize = 3;

        /** Header length. */
        private const int PoolHdrLen = MemHdrLen * PoolSize;

        #endregion

        #region UNPOOLED MEMORY 

        /// <summary>
        /// Allocate unpooled memory chunk.
        /// </summary>
        /// <param name="cap">Minimum capacity.</param>
        /// <returns>New memory pointer.</returns>
        public static PlatformMemoryHeader* AllocateUnpooled(int cap)
        {
            var memPtr = (PlatformMemoryHeader*) Marshal.AllocHGlobal(MemHdrLen);
            long dataPtr = Marshal.AllocHGlobal(cap).ToInt64();

            *memPtr = new PlatformMemoryHeader(dataPtr, cap, 0, PlatformMemoryHeader.FlagExt);

            return memPtr;
        }


        /// <summary>
        /// Reallocate unpooled memory chunk.
        /// </summary>
        /// <param name="memPtr">Memory pointer.</param>
        /// <param name="cap">Minimum capacity.</param>
        /// <returns></returns>
        public static void ReallocateUnpooled(PlatformMemoryHeader* memPtr, int cap)
        {
            memPtr->Pointer = Marshal.ReAllocHGlobal((IntPtr)memPtr->Pointer, (IntPtr)cap).ToInt64();

            memPtr->Capacity = cap;
        }

        /// <summary>
        /// Release unpooled memory chunk.
        /// </summary>
        /// <param name="memPtr">Memory pointer.</param>
        public static void ReleaseUnpooled(PlatformMemoryHeader* memPtr) 
        {
            Marshal.FreeHGlobal((IntPtr) memPtr->Pointer);
            Marshal.FreeHGlobal((IntPtr) memPtr);
        }

        #endregion

        #region POOLED MEMORY

        /// <summary>
        /// Allocate pool memory.
        /// </summary>
        /// <returns>Pool pointer.</returns>
        public static PlatformMemoryHeader* AllocatePool()
        {
            // 1. Allocate memory.
            var poolPtr = (PlatformMemoryHeader*) Marshal.AllocHGlobal((IntPtr) PoolHdrLen).ToInt64();

            for (var i = 0; i < PoolSize; i++)
                *(poolPtr + i) = new PlatformMemoryHeader(0, 0, 0,
                    PlatformMemoryHeader.FlagExt | PlatformMemoryHeader.FlagPooled);

            return poolPtr;
        }

        /// <summary>
        /// Release pool memory.
        /// </summary>
        /// <param name="poolPtr">Pool pointer.</param>
        public static void ReleasePool(PlatformMemoryHeader* poolPtr)
        {
            for (var i = 0; i < PoolSize; i++)
            {
                var mem = (poolPtr + i)->Pointer;

                if (mem != 0)
                    Marshal.FreeHGlobal((IntPtr) mem);
            }

            // Clean pool chunk.
            Marshal.FreeHGlobal((IntPtr) poolPtr);
        }

        /// <summary>
        /// Allocate pooled memory chunk.
        /// </summary>
        /// <param name="poolPtr">Pool pointer.</param>
        /// <param name="cap">CalculateCapacity.</param>
        /// <returns>Memory pointer or <c>0</c> in case there are no free memory chunks in the pool.</returns>
        public static PlatformMemoryHeader* AllocatePooled(PlatformMemoryHeader* poolPtr, int cap)
        {
            Debug.Assert(poolPtr != (void*)0);

            for (var i = 0; i < PoolSize; i++)
            {
                var hdr = poolPtr + i;

                if (!hdr->IsAcquired)
                {
                    AllocatePooled0(hdr, cap);
                    return hdr;
                }
            }

            return (PlatformMemoryHeader*) 0;
        }

        /// <summary>
        /// Internal pooled memory chunk allocation routine.
        /// </summary>
        /// <param name="hdr">Memory header.</param>
        /// <param name="cap">Capacity.</param>
        private static void AllocatePooled0(PlatformMemoryHeader* hdr, int cap) 
        {
            Debug.Assert(hdr != (void*)0);

            if (hdr->Pointer == 0)
            {
                // First allocation of the chunk.
                hdr->Pointer = Marshal.AllocHGlobal(cap).ToInt64();
                hdr->Capacity = cap;
            }
            else if (cap > hdr->Capacity)
            {
                // Ensure that we have enough capacity.
                hdr->Pointer = Marshal.ReAllocHGlobal((IntPtr) hdr->Pointer, (IntPtr) cap).ToInt64();
                hdr->Capacity = cap;
            }

            hdr->Flags = PlatformMemoryHeader.FlagExt | PlatformMemoryHeader.FlagPooled |
                         PlatformMemoryHeader.FlagAcquired;
        }

        /// <summary>
        /// Reallocate pooled memory chunk.
        /// </summary>
        /// <param name="hdr">Memory header.</param>
        /// <param name="cap">Minimum capacity.</param>
        public static void ReallocatePooled(PlatformMemoryHeader* hdr, int cap) 
        {
            Debug.Assert(hdr != (void*) 0);

            if (cap > hdr->Capacity)
            {
                hdr->Pointer = Marshal.ReAllocHGlobal((IntPtr) hdr->Pointer, (IntPtr) cap).ToInt64();
                hdr->Capacity = cap;
            }
        }

        /// <summary>
        /// Release pooled memory chunk.
        /// </summary>
        /// <param name="hdr">Memory header.</param>
        public static void ReleasePooled(PlatformMemoryHeader* hdr)
        {
            hdr->Flags ^= PlatformMemoryHeader.FlagAcquired;
        }

        #endregion

        #region MEMCPY

        /** Array copy delegate. */
        private delegate void MemCopy(byte* a1, byte* a2, int len);

        /** memcpy function handle. */
        private static readonly MemCopy Memcpy;

        /** Whether src and dest arguments are inverted. */
        [SuppressMessage("Microsoft.Performance", "CA1802:UseLiteralsWhereAppropriate")]
        private static readonly bool MemcpyInverted;

        /// <summary>
        /// Static initializer.
        /// </summary>
        [SuppressMessage("Microsoft.Design", "CA1065:DoNotRaiseExceptionsInUnexpectedLocations")]
        [SuppressMessage("Microsoft.Performance", "CA1810:InitializeReferenceTypeStaticFieldsInline")]
        static PlatformMemoryUtils()
        {
            Type type = typeof(Buffer);

            const BindingFlags flags = BindingFlags.Static | BindingFlags.NonPublic;
            Type[] paramTypes = { typeof(byte*), typeof(byte*), typeof(int) };

            // Assume .Net 4.5.
            MethodInfo mthd = type.GetMethod("Memcpy", flags, null, paramTypes, null);

            MemcpyInverted = true;

            if (mthd == null)
            {
                // Assume .Net 4.0.
                mthd = type.GetMethod("memcpyimpl", flags, null, paramTypes, null);

                MemcpyInverted = false;

                if (mthd == null)
                    throw new InvalidOperationException("Unable to get memory copy function delegate.");
            }

            Memcpy = (MemCopy)Delegate.CreateDelegate(typeof(MemCopy), mthd);
        }

        /// <summary>
        /// Unsafe memory copy routine.
        /// </summary>
        /// <param name="src">Source.</param>
        /// <param name="dest">Destination.</param>
        /// <param name="len">Length.</param>
        public static void CopyMemory(void* src, void* dest, int len)
        {
            CopyMemory((byte*)src, (byte*)dest, len);
        }

        /// <summary>
        /// Unsafe memory copy routine.
        /// </summary>
        /// <param name="src">Source.</param>
        /// <param name="dest">Destination.</param>
        /// <param name="len">Length.</param>
        public static void CopyMemory(byte* src, byte* dest, int len)
        {
            if (MemcpyInverted)
                Memcpy.Invoke(dest, src, len);
            else
                Memcpy.Invoke(src, dest, len);
        }

        #endregion
    }
}
