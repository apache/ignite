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
    using System.Diagnostics.CodeAnalysis;
    using System.Threading;
    
    /// <summary>
    /// Memory manager implementation.
    /// </summary>
    [SuppressMessage("Microsoft.Design", "CA1001:TypesThatOwnDisposableFieldsShouldBeDisposable",
        Justification = "This class instance usually lives as long as the app runs.")]
    internal class InteropMemoryManager
    {
        /** Default capacity. */
        private readonly int _dfltCap;

        /** Thread-local pool. */
        private readonly ThreadLocal<InteropMemoryPool> _threadLocPool = new ThreadLocal<InteropMemoryPool>();

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="dfltCap">Default capacity.</param>
        public InteropMemoryManager(int dfltCap)
        {
            _dfltCap = dfltCap;
        }

        /// <summary>
        /// Allocate memory.
        /// </summary>
        /// <returns>Memory.</returns>
        public IInteropMemory Allocate()
        {
            return Allocate(_dfltCap);
        }

        /// <summary>
        /// Allocate memory having at least the given capacity.
        /// </summary>
        /// <param name="cap">Minimum capacity.</param>
        /// <returns>Memory.</returns>
        public IInteropMemory Allocate(int cap)
        {
            return Pool().Allocate(cap);
        }

        /// <summary>
        /// Gets memory from existing pointer.
        /// </summary>
        /// <param name="memPtr">Cross-platform memory pointer.</param>
        /// <returns>Memory.</returns>
        public IInteropMemory Get(long memPtr)
        {
            int flags = InteropMemoryUtils.Flags(memPtr);

            return InteropMemoryUtils.IsExternal(flags) ? GetExternalMemory(memPtr)
                : InteropMemoryUtils.IsPooled(flags) ? Pool().Get(memPtr) : new InteropUnpooledMemory(memPtr);
        }

        /// <summary>
        /// Gets or creates thread-local memory pool.
        /// </summary>
        /// <returns>Memory pool.</returns>
        public InteropMemoryPool Pool()
        {
            InteropMemoryPool pool = _threadLocPool.Value;

            if (pool == null)
            {
                pool = new InteropMemoryPool();

                _threadLocPool.Value = pool;
            }

            return pool;
        }

        /// <summary>
        /// Gets the external memory.
        /// </summary>
        /// <param name="memPtr">Cross-platform memory pointer.</param>
        /// <returns>Memory.</returns>
        protected IInteropMemory GetExternalMemory(long memPtr)
        {
            throw new NotSupportedException("Not supported in Ignite yet");
        }
    }
}
