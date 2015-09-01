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
    /// Unmanaged target.
    /// </summary>
    internal unsafe interface IUnmanagedTarget : IDisposable
    {
        /// <summary>
        /// Context.
        /// </summary>
        void* Context { get; }

        /// <summary>
        /// Target.
        /// </summary>
        void* Target { get; }

        /// <summary>
        /// Creates new instance with same context and different target.
        /// </summary>
        IUnmanagedTarget ChangeTarget(void* target);
    }
}
