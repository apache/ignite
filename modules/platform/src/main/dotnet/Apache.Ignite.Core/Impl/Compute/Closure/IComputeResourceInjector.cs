/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace Apache.Ignite.Core.Impl.Compute.Closure
{
    /// <summary>
    /// Interface denoting entity which must perform custom resource injection.
    /// </summary>
    internal interface IComputeResourceInjector
    {
        /// <summary>
        /// Inject resources.
        /// </summary>
        /// <param name="grid">Grid.</param>
        void Inject(GridImpl grid);
    }
}
