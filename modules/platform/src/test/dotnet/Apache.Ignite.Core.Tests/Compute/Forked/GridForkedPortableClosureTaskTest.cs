/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace Apache.Ignite.Core.Tests.Compute.Forked
{
    /// <summary>
    /// Forked closure execution tests for portable objects.
    /// </summary>
    public class GridForkedPortableClosureTaskTest : GridPortableClosureTaskTest
    {
        /// <summary>
        /// Constructor.
        /// </summary>
        public GridForkedPortableClosureTaskTest() : base(true) { }
    }
}
