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
    using Apache.Ignite.Core.Impl.Unmanaged;

    /// <summary>
    /// Utility methods for interop memory management.
    /// </summary>
    internal static class InteropMemoryUtils
    {
        /// <summary>
        /// Re-allocate external memory chunk.
        /// </summary>
        /// <param name="memPtr">Memory pointer.</param>
        /// <param name="cap">CalculateCapacity.</param>
        /// <returns>New memory pointer.</returns>
        public static void ReallocateExternal(long memPtr, int cap)
        {
            UnmanagedUtils.Reallocate(memPtr, cap);
        }
    }
}
