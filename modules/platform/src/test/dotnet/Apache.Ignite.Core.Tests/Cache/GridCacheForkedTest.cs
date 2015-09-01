﻿/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Cache
{
    using System.IO;
    using GridGain.Client;
    using GridGain.Client.Process;
    using NUnit.Framework;

    /// <summary>
    /// Tests cache with a standalone process.
    /// </summary>
    public class GridCacheForkedTest
    {
        /** */
        private IIgnite grid;

        /// <summary>
        /// Set up.
        /// </summary>
        [TestFixtureSetUp]
        public void SetUp()
        {
            const string springConfigUrl = "config\\compute\\compute-grid1.xml";
            
            // ReSharper disable once UnusedVariable
            var proc = new GridProcess(
                "-jvmClasspath=" + GridTestUtils.CreateTestClasspath(),
                "-springConfigUrl=" + Path.GetFullPath(springConfigUrl),
                "-J-ea",
                "-J-Xcheck:jni",
                "-J-Xms512m",
                "-J-Xmx512m",
                "-J-DIGNITE_QUIET=false"
                );

            grid = GridFactory.Start(new GridConfiguration
            {
                JvmClasspath = GridTestUtils.CreateTestClasspath(),
                JvmOptions = GridTestUtils.TestJavaOptions(),
                SpringConfigUrl = springConfigUrl
            });

            Assert.IsTrue(grid.WaitTopology(2, 30000));
        }

        /// <summary>
        /// Tear down.
        /// </summary>
        [TestFixtureTearDown]
        public void TearDown()
        {
            GridProcess.KillAll();

            GridFactory.StopAll(true);
        }

        /// <summary>
        /// Tests cache clear.
        /// </summary>
        [Test]
        public void TestClearCache()
        {
            grid.Cache<object, object>(null).Clear();
        }
    }
}