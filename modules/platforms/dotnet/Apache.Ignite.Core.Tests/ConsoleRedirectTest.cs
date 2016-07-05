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

                RunInNewDomain();

                var outTxt = _outSb.ToString();

                // Check output from another domain
                Assert.IsTrue(outTxt.Contains("[ver=2, servers=2, clients=0,"));
                Assert.IsTrue(outTxt.Contains(">>> Grid name: newDomainGrid"));

                // Check that current domain produces output again
                Assert.AreEqual(3, ignite.GetCluster().TopologyVersion);
                Assert.IsTrue(outTxt.Contains("[ver=3, servers=1, clients=0,"));
            }
        }

        private static void RunInNewDomain()
        {
            AppDomain childDomain = null;

            try
            {
                // Construct and initialize settings for a second AppDomain.
                var domainSetup = new AppDomainSetup
                {
                    ApplicationBase = AppDomain.CurrentDomain.SetupInformation.ApplicationBase,
                    ConfigurationFile = AppDomain.CurrentDomain.SetupInformation.ConfigurationFile,
                    ApplicationName = AppDomain.CurrentDomain.SetupInformation.ApplicationName,
                    LoaderOptimization = LoaderOptimization.MultiDomainHost
                };

                // Create the child AppDomain used for the service tool at runtime.
                childDomain = AppDomain.CreateDomain(
                    "Your Child AppDomain", null, domainSetup);

                // Create an instance of the runtime in the second AppDomain. 
                // A proxy to the object is returned.
                var runtime = (IIgniteRunner)childDomain.CreateInstanceAndUnwrap(
                    typeof(IgniteRunner).Assembly.FullName, typeof(IgniteRunner).FullName);

                // start the runtime.  call will marshal into the child runtime appdomain
                runtime.Run();
            }
            finally
            {
                // runtime has exited, finish off by unloading the runtime appdomain
                if (childDomain != null) AppDomain.Unload(childDomain);
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
