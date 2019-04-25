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

using System;

namespace Apache.Ignite.Core.Impl.Unmanaged.Jni
{
    using System.Diagnostics;

    /// <summary>
    /// JNI Global Reference.
    /// <para />
    /// We should always convert local reference to global and delete local one immediately,
    /// otherwise these local references may cause memory leaks (false GC roots).
    /// </summary>
    internal sealed class GlobalRef : IDisposable
    {
        /** Reference. */
        private readonly IntPtr _target;

        /** JVM. */
        private readonly Jvm _jvm;

        /// <summary>
        /// Initializes a new instance of the <see cref="GlobalRef"/> class.
        /// </summary>
        public GlobalRef(IntPtr target, Jvm jvm)
        {
            Debug.Assert(target != IntPtr.Zero);
            Debug.Assert(jvm != null);

            _target = target;
            _jvm = jvm;
        }

        /// <summary>
        /// Gets the target.
        /// </summary>
        public IntPtr Target
        {
            get { return _target; }
        }

        /// <summary>
        /// Gets the JVM.
        /// </summary>
        public Jvm Jvm
        {
            get { return _jvm; }
        }

        /// <summary>
        /// Releases the unmanaged resources.
        /// </summary>
        private void ReleaseUnmanagedResources()
        {
            _jvm.AttachCurrentThread().DeleteGlobalRef(_target);
        }

        /** <inheritdoc /> */
        public void Dispose()
        {
            ReleaseUnmanagedResources();
            GC.SuppressFinalize(this);
        }

        /** <inheritdoc /> */
        ~GlobalRef()
        {
            ReleaseUnmanagedResources();
        }
    }
}
