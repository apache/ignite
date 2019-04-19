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
    /// Platform memory chunk.
    /// </summary>
    internal interface IPlatformMemory
    {
        /// <summary>
        /// Gets stream for read/write operations on the given memory chunk.
        /// </summary>
        /// <returns></returns>
        PlatformMemoryStream GetStream();

        /// <summary>
        /// Cross-platform pointer.
        /// </summary>
        long Pointer { get; }

        /// <summary>
        /// Data pointer.
        /// </summary>
        long Data { get; }

        /// <summary>
        /// CalculateCapacity.
        /// </summary>
        int Capacity { get; }

        /// <summary>
        /// Length.
        /// </summary>
        int Length { get; set; }

        /// <summary>
        /// Reallocates memory chunk.
        /// </summary>
        /// <param name="cap">Minimum capacity.</param>
        void Reallocate(int cap);

        /// <summary>
        /// Release memory.
        /// </summary>
        void Release();
    }
}
