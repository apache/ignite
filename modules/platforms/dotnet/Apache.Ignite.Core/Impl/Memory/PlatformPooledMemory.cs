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
    /// Platform pooled memory chunk.
    /// </summary>
    internal class PlatformPooledMemory : PlatformMemory
    {
        /** Cached stream. */
        private PlatformMemoryStream _stream;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="memPtr">Memory pointer.</param>
        public PlatformPooledMemory(long memPtr) : base(memPtr)
        {
            // No-op.
        }

        /** <inheritdoc /> */
        public override PlatformMemoryStream GetStream()
        {
            if (_stream == null)
                _stream = base.GetStream();
            else
                _stream.Reuse();

            return _stream;
        }

        /** <inheritdoc /> */
        public override void Reallocate(int cap)
        {
            // Try doubling capacity to avoid excessive allocations.
            int doubledCap = PlatformMemoryUtils.GetCapacity(Pointer) << 1;

            if (doubledCap > cap)
                cap = doubledCap;

            PlatformMemoryPool.Reallocate(Pointer, cap);
        }

        /** <inheritdoc /> */
        public override void Release()
        {
            PlatformMemoryPool.Release(Pointer); // Return to the pool.
        }
    }
}
