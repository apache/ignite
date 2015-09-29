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

namespace Apache.Ignite.Core.Impl.Unmanaged
{
    using System;

    /// <summary>
    /// Unmanaged target which does not require explicit release.
    /// </summary>
    internal unsafe class UnmanagedNonReleaseableTarget : IUnmanagedTarget
    {
        /** Context. */
        private readonly void* _ctx;

        /** Target. */
        private readonly void* _target;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="ctx">Context.</param>
        /// <param name="target">Target.</param>
        public UnmanagedNonReleaseableTarget(void* ctx, void* target)
        {
            _ctx = ctx;
            _target = target;
        }

        /** <inheritdoc /> */
        public void* Context
        {
            get { return _ctx; }
        }

        /** <inheritdoc /> */
        public void* Target
        {
            get { return _target; }
        }

        /** <inheritdoc /> */
        public IUnmanagedTarget ChangeTarget(void* target)
        {
            throw new NotSupportedException();
        }

        /** <inheritdoc /> */
        public void Dispose()
        {
            // No-op.
        }
    }
}
