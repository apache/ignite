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
    /// <summary>
    /// Interop unpooled memory chunk.
    /// </summary>
    internal class InteropUnpooledMemory : InteropMemory
    {
        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="memPtr">Memory pointer.</param>
        public InteropUnpooledMemory(long memPtr) : base(memPtr)
        {
            // No-op.
        }

        /** <inheritdoc /> */
        public override void Reallocate(int cap)
        {
            // Try doubling capacity to avoid excessive allocations.
            int doubledCap = ((InteropMemoryUtils.Capacity(MemPtr) + 16) << 1) - 16;

            if (doubledCap > cap)
                cap = doubledCap;

            InteropMemoryUtils.ReallocateUnpooled(MemPtr, cap);
        }

        /** <inheritdoc /> */
        public override void Release()
        {
            InteropMemoryUtils.ReleaseUnpooled(MemPtr);
        }
    }
}
