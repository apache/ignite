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
    /// Interop external memory chunk.
    /// </summary>
    internal class InteropExternalMemory : PlatformMemory
    {
        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="memPtr">Memory pointer.</param>
        public InteropExternalMemory(long memPtr) : base(memPtr)
        {
            // No-op.
        }

        /** <inheritdoc /> */
        public override void Reallocate(int cap)
        {
            InteropMemoryUtils.ReallocateExternal(Pointer, cap);
        }

        /** <inheritdoc /> */
        public override void Release()
        {
            // Memory can only be released by native platform.
        }
    }
}
