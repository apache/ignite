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
    /// Forked closure execution tests for serializable objects.
    /// </summary>
    public class GridForkedSerializableClosureTaskTest : GridSerializableClosureTaskTest
    {
        /// <summary>
        /// Constructor.
        /// </summary>
        public GridForkedSerializableClosureTaskTest() : base(true) { }
    }
}
