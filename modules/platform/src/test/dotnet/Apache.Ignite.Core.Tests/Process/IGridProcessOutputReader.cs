/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace Apache.Ignite.Core.Tests.Process
{
    using System.Diagnostics;

    /// <summary>
    /// Process output reader.
    /// </summary>
    public interface IGridProcessOutputReader
    {
        /// <summary>
        /// Callback invoked when output data appear.
        /// </summary>
        /// <param name="proc">Process produced data.</param>
        /// <param name="data">Data.</param>
        /// <param name="err">Error flag.</param>
        void OnOutput(Process proc, string data, bool err);
    }
}
