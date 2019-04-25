/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
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
    /// Platform unpooled memory chunk.
    /// </summary>
    internal class PlatformUnpooledMemory : PlatformMemory
    {
        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="memPtr">Memory pointer.</param>
        public PlatformUnpooledMemory(long memPtr) : base(memPtr)
        {
            // No-op.
        }

        /** <inheritdoc /> */
        public override void Reallocate(int cap)
        {
            // Try doubling capacity to avoid excessive allocations.
            int doubledCap = ((PlatformMemoryUtils.GetCapacity(Pointer) + 16) << 1) - 16;

            if (doubledCap > cap)
                cap = doubledCap;

            PlatformMemoryUtils.ReallocateUnpooled(Pointer, cap);
        }

        /** <inheritdoc /> */
        public override void Release()
        {
            PlatformMemoryUtils.ReleaseUnpooled(Pointer);
        }
    }
}
