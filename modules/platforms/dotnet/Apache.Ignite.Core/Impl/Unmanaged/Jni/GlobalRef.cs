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
