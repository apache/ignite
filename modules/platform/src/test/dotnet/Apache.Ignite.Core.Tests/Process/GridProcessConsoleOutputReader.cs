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
    using System;
    using System.Diagnostics;

    /// <summary>
    /// Output reader pushing data to the console.
    /// </summary>
    public class GridProcessConsoleOutputReader : IGridProcessOutputReader
    {
        /** Out message format. */
        private static readonly string OUT_FORMAT = ">>> {0} OUT: {1}";

        /** Error message format. */
        private static readonly string ERR_FORMAT = ">>> {0} ERR: {1}";

        /** <inheritDoc /> */
        public void OnOutput(Process proc, string data, bool err)
        {
            Console.WriteLine(err ? ERR_FORMAT : OUT_FORMAT, proc.Id, data);
        }
    }
}
