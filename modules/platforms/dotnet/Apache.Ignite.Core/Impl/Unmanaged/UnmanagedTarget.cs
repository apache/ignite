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
    using System.Runtime.InteropServices;
    using UU = UnmanagedUtils;

    /// <summary>
    /// Base unmanaged target implementation.
    /// </summary>
    internal unsafe sealed class UnmanagedTarget : CriticalHandle, IUnmanagedTarget
    {
        /** Context. */
        private readonly UnmanagedContext _ctx;
        
        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="ctx">Context.</param>
        /// <param name="target">Target.</param>
        public UnmanagedTarget(UnmanagedContext ctx, void* target)
            : base(IntPtr.Zero)
        {
            _ctx = ctx;
            
            SetHandle(new IntPtr(target));
        }

        /** <inheritdoc /> */
        public void* Context
        {
            get { return _ctx.NativeContext; }
        }

        /** <inheritdoc /> */
        public void* Target
        {
            get { return handle.ToPointer(); }
        }

        /** <inheritdoc /> */
        public IUnmanagedTarget ChangeTarget(void* target)
        {
            return new UnmanagedTarget(_ctx, target);
        }

        /** <inheritdoc /> */
        protected override bool ReleaseHandle()
        {
            UU.Release(this);
            
            return true;
        }

        /** <inheritdoc /> */
        public override bool IsInvalid
        {
            get { return handle == IntPtr.Zero; }
        }
    }
}
