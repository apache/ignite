/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Client
{
    using System;
    using System.IO;

    using GridGain.Impl;

    using NUnit.Framework;

    /// <summary>
    /// Tests GridManager class.
    /// </summary>
    public class GridManagerTest
    {
        /// <summary>
        /// Tests home dir resolver.
        /// </summary>
        [Test]
        public void TestGridGainHome()
        {
            var env = Environment.GetEnvironmentVariable(GridManager.ENV_GRIDGAIN_HOME);
            
            Environment.SetEnvironmentVariable(GridManager.ENV_GRIDGAIN_HOME, null);

            try
            {
                Assert.IsTrue(Directory.Exists(GridManager.GridGainHome(null)));
            }
            finally
            {
                // Restore
                Environment.SetEnvironmentVariable(GridManager.ENV_GRIDGAIN_HOME, env);
            }
        }
    }
}