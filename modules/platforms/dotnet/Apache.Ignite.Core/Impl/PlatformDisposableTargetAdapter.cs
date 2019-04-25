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

namespace Apache.Ignite.Core.Impl
{
    using System;

    /// <summary>
    /// PlatformTargetAdapter with IDisposable pattern.
    /// </summary>
    internal abstract class PlatformDisposableTargetAdapter : PlatformTargetAdapter, IDisposable
    {
        /** Disposed flag. */
        private volatile bool _disposed;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="target">Target.</param>
        protected PlatformDisposableTargetAdapter(IPlatformTargetInternal target) : base(target)
        {
            // No-op.
        }

        /** <inheritdoc /> */
        public void Dispose()
        {
            lock (this)
            {
                if (_disposed)
                    return;

                Dispose(true);

                GC.SuppressFinalize(this);

                _disposed = true;
            }
        }

        /// <summary>
        /// Releases unmanaged and - optionally - managed resources.
        /// </summary>
        /// <param name="disposing">
        /// <c>true</c> when called from Dispose;  <c>false</c> when called from finalizer.
        /// </param>
        protected virtual void Dispose(bool disposing)
        {
            Target.Dispose();
        }

        /// <summary>
        /// Throws <see cref="ObjectDisposedException"/> if this instance has been disposed.
        /// </summary>
        protected void ThrowIfDisposed()
        {
            if (_disposed)
                throw new ObjectDisposedException(GetType().Name, "Object has been disposed.");
        }
    }
}