/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Client.Compute
{
    /// <summary>
    /// Forked task adapter test.
    /// </summary>
    public class GridForkedTaskAdapterTest : GridTaskAdapterTest
    {
        /// <summary>
        /// Constructor.
        /// </summary>
        public GridForkedTaskAdapterTest() : base(true) { }
    }
}
