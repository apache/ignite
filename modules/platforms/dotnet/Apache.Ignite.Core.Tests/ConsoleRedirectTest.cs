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

namespace Apache.Ignite.Core.Tests
{
    using System;
    using System.IO;
    using System.Text;
    using System.Text.RegularExpressions;
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Communication.Tcp;
    using NUnit.Framework;

    /// <summary>
    /// Tests that Java console output is redirected to .NET console.
    /// </summary>
    public class ConsoleRedirectTest
    {
        /** */
        private StringBuilder _outSb;

        /** */
        private StringBuilder _errSb;

        /** */
        private TextWriter _stdOut;

        /** */
        private TextWriter _stdErr;

        /// <summary>
        /// Sets up the test.
        /// </summary>
        [SetUp]
        public void SetUp()
        {
            _stdOut = Console.Out;
            _stdErr = Console.Error;

            _outSb = new StringBuilder();
            Console.SetOut(new StringWriter(_outSb));

            _errSb = new StringBuilder();
            Console.SetError(new StringWriter(_errSb));
        }

        /// <summary>
        /// Tears down the test.
        /// </summary>
        [TearDown]
        public void TearDown()
        {
            Console.SetOut(_stdOut);
            Console.SetError(_stdErr);
        }

        /// <summary>
        /// Tests the startup output.
        /// </summary>
        [Test]
        public void TestStartupOutput()
        {
            using (Ignition.Start(TestUtils.GetTestConfiguration()))
            {
                Assert.IsTrue(_outSb.ToString().Contains("[ver=1, servers=1, clients=0,"));
            }
        }

        /// <summary>
        /// Tests startup error in Java.
        /// </summary>
        [Test]
        public void TestStartupJavaError()
        {
            // Invalid config
            Assert.Throws<IgniteException>(() =>
                Ignition.Start(new IgniteConfiguration(TestUtils.GetTestConfiguration())
                {
                    CommunicationSpi = new TcpCommunicationSpi
                    {
                        IdleConnectionTimeout = TimeSpan.MinValue
                    }
                }));

            Assert.IsTrue(_errSb.ToString().Contains("SPI parameter failed condition check: idleConnTimeout > 0"));
        }

        /// <summary>
        /// Tests multiple appdomains and multiple console handlers.
        /// </summary>
        [Test]
        public void TestMultipleDomains()
        {
            using (var ignite = Ignition.Start(TestUtils.GetTestConfiguration()))
            {
                Assert.IsTrue(_outSb.ToString().Contains("[ver=1, servers=1, clients=0,"));

                // Run twice
                RunInNewDomain();
                RunInNewDomain();

                Assert.AreEqual(5, ignite.GetCluster().TopologyVersion);

                var outTxt = _outSb.ToString();

                // Check output from another domain (2 started + 2 stopped = 4)
                Assert.AreEqual(4, Regex.Matches(outTxt, ">>> Grid name: newDomainGrid").Count);

                // Both domains produce the topology snapshot on node enter
                Assert.AreEqual(2, Regex.Matches(outTxt, "ver=2, servers=2, clients=0,").Count);
                Assert.AreEqual(1, Regex.Matches(outTxt, "ver=3, servers=1, clients=0,").Count);
                Assert.AreEqual(2, Regex.Matches(outTxt, "ver=4, servers=2, clients=0,").Count);
                Assert.AreEqual(1, Regex.Matches(outTxt, "ver=5, servers=1, clients=0,").Count);
            }
        }

        /// <summary>
        /// Runs the Ignite in a new domain.
        /// </summary>
        private static void RunInNewDomain()
        {
            AppDomain childDomain = null;

            try
            {
                childDomain = AppDomain.CreateDomain("Child", null, new AppDomainSetup
                {
                    ApplicationBase = AppDomain.CurrentDomain.SetupInformation.ApplicationBase,
                    ConfigurationFile = AppDomain.CurrentDomain.SetupInformation.ConfigurationFile,
                    ApplicationName = AppDomain.CurrentDomain.SetupInformation.ApplicationName,
                    LoaderOptimization = LoaderOptimization.MultiDomainHost
                });

                var runner = (IIgniteRunner)childDomain.CreateInstanceAndUnwrap(
                    typeof(IgniteRunner).Assembly.FullName, typeof(IgniteRunner).FullName);

                runner.Run();
            }
            finally
            {
                if (childDomain != null)
                    AppDomain.Unload(childDomain);
            }
        }

        private interface IIgniteRunner
        {
            void Run();
        }

        private class IgniteRunner : MarshalByRefObject, IIgniteRunner
        {
            public void Run()
            {
                var ignite = Ignition.Start(new IgniteConfiguration(TestUtils.GetTestConfiguration())
                {
                    GridName = "newDomainGrid"
                });
                Ignition.Stop(ignite.Name, true);
            }
        }
    }
}
