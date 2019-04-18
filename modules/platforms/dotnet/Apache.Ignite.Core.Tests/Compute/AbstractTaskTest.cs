/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

namespace Apache.Ignite.Core.Tests.Compute
{
    using System;
    using System.Linq;
    using System.Threading;
    using Apache.Ignite.Core.Tests.Process;
    using NUnit.Framework;

    /// <summary>
    /// Base class for all task-related tests.
    /// </summary>
    public abstract class AbstractTaskTest
    {
        /** */
        protected const string Grid1Name = "grid1";

        /** */
        protected const string Grid2Name = "grid2";

        /** */
        protected const string Grid3Name = "grid3";

        /** */
        protected const string Cache1Name = "cache1";

        /** Whether this is a test with forked JVMs. */
        private readonly bool _fork;

        /** First node. */
        [NonSerialized]
        protected IIgnite Grid1;

        /** Second node. */
        [NonSerialized]
        // ReSharper disable once NotAccessedField.Local
        private IIgnite _grid2;

        /** Third node. */
        [NonSerialized]
        // ReSharper disable once NotAccessedField.Local
        private IIgnite _grid3;

        /** Second process. */
        [NonSerialized]
        private IgniteProcess _proc2;

        /** Third process. */
        [NonSerialized]
        private IgniteProcess _proc3;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="fork">Fork flag.</param>
        protected AbstractTaskTest(bool fork)
        {
            _fork = fork;
        }

        /// <summary>
        /// Initialization routine.
        /// </summary>
        [TestFixtureSetUp]
        public void InitClient()
        {
            TestUtils.KillProcesses();

            if (_fork)
            {
                Grid1 = Ignition.Start(GetConfiguration("config\\compute\\compute-standalone.xml"));

                _proc2 = Fork("config\\compute\\compute-standalone.xml");

                while (true)
                {
                    if (!_proc2.Alive)
                        throw new Exception("Process 2 died unexpectedly: " + _proc2.Join());

                    if (Grid1.GetCluster().GetNodes().Count < 2)
                        Thread.Sleep(100);
                    else
                        break;
                }

                _proc3 = Fork("config\\compute\\compute-standalone.xml");

                while (true)
                {
                    if (!_proc3.Alive)
                        throw new Exception("Process 3 died unexpectedly: " + _proc3.Join());

                    if (Grid1.GetCluster().GetNodes().Count < 3)
                        Thread.Sleep(100);
                    else
                        break;
                }
            }
            else
            {
                Grid1 = Ignition.Start(GetConfiguration("config\\compute\\compute-grid1.xml"));
                _grid2 = Ignition.Start(GetConfiguration("config\\compute\\compute-grid2.xml"));
                _grid3 = Ignition.Start(GetConfiguration("config\\compute\\compute-grid3.xml"));
            }
        }

        [SetUp]
        public void BeforeTest()
        {
            Console.WriteLine("Test started: " + TestContext.CurrentContext.Test.Name);
        }

        [TestFixtureTearDown]
        public void StopClient()
        {
            Ignition.StopAll(true);
            IgniteProcess.KillAll();
        }

        /// <summary>
        /// Configuration for node.
        /// </summary>
        /// <param name="path">Path to Java XML configuration.</param>
        /// <returns>Node configuration.</returns>
        private static IgniteConfiguration GetConfiguration(string path)
        {
            return new IgniteConfiguration(TestUtils.GetTestConfiguration())
            {
                SpringConfigUrl = path,
            };
        }

        /// <summary>
        /// Create forked process with the following Spring config.
        /// </summary>
        /// <param name="path">Path to Java XML configuration.</param>
        /// <returns>Forked process.</returns>
        private static IgniteProcess Fork(string path)
        {
            return new IgniteProcess(
                "-springConfigUrl=" + path,
                "-J-ea",
                "-J-Xcheck:jni",
                "-J-Xms512m",
                "-J-Xmx512m",
                "-J-DIGNITE_QUIET=false"
                //"-J-Xnoagent", "-J-Djava.compiler=NONE", "-J-Xdebug", "-J-Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=5006"
            );
        }

        /// <summary>
        /// Gets the server count.
        /// </summary>
        protected int GetServerCount()
        {
            return Grid1.GetCluster().GetNodes().Count(x => !x.IsClient);
        }
    }
}
