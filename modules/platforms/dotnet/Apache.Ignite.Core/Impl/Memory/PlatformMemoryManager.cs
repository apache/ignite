/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

namespace Apache.Ignite.Core.Impl.Memory
{
    using System.Diagnostics.CodeAnalysis;
    using System.Threading;

    /// <summary>
    /// Memory manager implementation.
    /// </summary>
    [SuppressMessage("Microsoft.Design", "CA1001:TypesThatOwnDisposableFieldsShouldBeDisposable",
        Justification = "This class instance usually lives as long as the app runs.")]
    // ReSharper disable once ClassWithVirtualMembersNeverInherited.Global
    internal class PlatformMemoryManager
    {
        /** Default capacity. */
        private readonly int _dfltCap;

        /** Thread-local pool. */
        private readonly ThreadLocal<PlatformMemoryPool> _threadLocPool = new ThreadLocal<PlatformMemoryPool>();

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="dfltCap">Default capacity.</param>
        public PlatformMemoryManager(int dfltCap)
        {
            _dfltCap = dfltCap;
        }

        /// <summary>
        /// Allocate memory.
        /// </summary>
        /// <returns>Memory.</returns>
        public IPlatformMemory Allocate()
        {
            return Allocate(_dfltCap);
        }

        /// <summary>
        /// Allocate memory having at least the given capacity.
        /// </summary>
        /// <param name="cap">Minimum capacity.</param>
        /// <returns>Memory.</returns>
        public IPlatformMemory Allocate(int cap)
        {
            return Pool().Allocate(cap);
        }

        /// <summary>
        /// Gets memory from existing pointer.
        /// </summary>
        /// <param name="memPtr">Cross-platform memory pointer.</param>
        /// <returns>Memory.</returns>
        public IPlatformMemory Get(long memPtr)
        {
            int flags = PlatformMemoryUtils.GetFlags(memPtr);

            return PlatformMemoryUtils.IsExternal(flags) ? GetExternalMemory(memPtr)
                : PlatformMemoryUtils.IsPooled(flags) ? Pool().Get(memPtr) : new PlatformUnpooledMemory(memPtr);
        }

        /// <summary>
        /// Gets or creates thread-local memory pool.
        /// </summary>
        /// <returns>Memory pool.</returns>
        [SuppressMessage("Microsoft.Reliability", "CA2000:Dispose objects before losing scope")]
        public PlatformMemoryPool Pool()
        {
            PlatformMemoryPool pool = _threadLocPool.Value;

            if (pool == null)
            {
                pool = new PlatformMemoryPool();

                _threadLocPool.Value = pool;
            }

            return pool;
        }

        /// <summary>
        /// Gets the external memory.
        /// </summary>
        /// <param name="memPtr">Cross-platform memory pointer.</param>
        /// <returns>Memory.</returns>
        protected virtual IPlatformMemory GetExternalMemory(long memPtr)
        {
            return new InteropExternalMemory(memPtr);
        }
    }
}
