/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
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
        private readonly void* ctx;

        /** Target. */
        private readonly void* target;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="ctx">Context.</param>
        /// <param name="target">Target.</param>
        public UnmanagedNonReleaseableTarget(void* ctx, void* target)
        {
            this.ctx = ctx;
            this.target = target;
        }

        /** <inheritdoc /> */
        public void* Context
        {
            get { return ctx; }
        }

        /** <inheritdoc /> */
        public void* Target
        {
            get { return target; }
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
