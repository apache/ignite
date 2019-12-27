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

namespace Apache.Ignite.Core.Tests.Examples
{
    extern alias ExamplesDll;
    using System;
    using System.Collections.Generic;
    using System.Configuration;
    using System.IO;
    using System.Linq;
    using Apache.Ignite.Core.Impl.Common;
    using Apache.Ignite.Core.Tests.Process;
    using Apache.Ignite.Examples.Compute;
    using Apache.Ignite.Examples.Datagrid;
    using Apache.Ignite.Examples.Messaging;
    using Apache.Ignite.Examples.Misc;
    using Apache.Ignite.Examples.Sql;
    using Apache.Ignite.Examples.ThinClient;
    using NUnit.Framework;

    /// <summary>
    /// Tests all examples in various modes.
    /// </summary>
    [Category(TestUtils.CategoryExamples)]
    public class ExamplesTest
    {
        /** */
        private static readonly Example[] AllExamples = Example.GetExamples().ToArray();

        /** */
        private static readonly Type[] LocalOnlyExamples =
        {
            typeof(LifecycleExample), typeof(ClientReconnectExample), typeof(MultiTieredCacheExample)
        };

        /** */
        private static readonly Type[] RemoteOnlyExamples =
        {
            typeof(PeerAssemblyLoadingExample), typeof(MessagingExample), typeof(NearCacheExample),
            typeof(ThinClientPutGetExample), typeof(ThinClientQueryExample), typeof(ThinClientSqlExample)
        };

        /** */
        private static readonly Type[] NoDllExamples =
        {
            typeof(BinaryModeExample), typeof(NearCacheExample), typeof(PeerAssemblyLoadingExample),
            typeof(ThinClientPutGetExample), typeof(SqlExample), typeof(LinqExample), typeof(SqlDmlExample),
            typeof(SqlDdlExample), typeof(ThinClientSqlExample)
        };

        /** Config file path. */
        private string _configPath;

        /** */
        private IDisposable _changedConfig;

        /** */
        private IDisposable _changedEnvVar;

        /** */
        private bool _remoteNodeStarted;

        /// <summary>
        /// Tests the example in a single node mode.
        /// </summary>
        /// <param name="example">The example to run.</param>
        [Test, TestCaseSource("TestCasesLocal")]
        public void TestLocalNode(Example example)
        {
            StopRemoteNodes();

            if (LocalOnlyExamples.Contains(example.ExampleType))
            {
                Assert.IsFalse(example.NeedsTestDll, "Local-only example should not mention test dll.");
                Assert.IsNull(example.ConfigPath, "Local-only example should not mention app.config path.");
            }

            example.Run();
        }

        /// <summary>
        /// Tests the example with standalone Apache.Ignite.exe nodes.
        /// </summary>
        /// <param name="example">The example to run.</param>
        [Test, TestCaseSource("TestCasesRemote")]
        public void TestRemoteNodes(Example example)
        {
            TestRemoteNodes(example, false);
        }

        /// <summary>
        /// Tests the example with standalone Apache.Ignite.exe nodes while local node is in client mode.
        /// </summary>
        /// <param name="example">The example to run.</param>
        [Test, TestCaseSource("TestCasesRemote")]
        public void TestRemoteNodesClientMode(Example example)
        {
            TestRemoteNodes(example, true);
        }

        /// <summary>
        /// Tests the example with standalone Apache.Ignite.exe nodes.
        /// </summary>
        /// <param name="example">The example to run.</param>
        /// <param name="clientMode">Client mode flag.</param>
        private void TestRemoteNodes(Example example, bool clientMode)
        {
            Assert.IsTrue(PathUtil.ExamplesAppConfigPath.EndsWith(example.ConfigPath,
                StringComparison.OrdinalIgnoreCase), "All examples should use the same app.config.");

            Assert.IsTrue(example.NeedsTestDll || NoDllExamples.Contains(example.ExampleType),
                "Examples that allow standalone nodes should mention test dll.");

            StartRemoteNodes();

            Ignition.ClientMode = clientMode;

            // Run twice to catch issues with standalone node state
            example.Run();
            example.Run();
        }

        /// <summary>
        /// Starts standalone node.
        /// </summary>
        private void StartRemoteNodes()
        {
            if (_remoteNodeStarted)
                return;

            // Start a grid to monitor topology;
            // Stop it after topology check so we don't interfere with example.
            Ignition.ClientMode = false;

            var fileMap = new ExeConfigurationFileMap { ExeConfigFilename = _configPath };
            var config = ConfigurationManager.OpenMappedExeConfiguration(fileMap, ConfigurationUserLevel.None);
            var section = (IgniteConfigurationSection) config.GetSection("igniteConfiguration");

            // Disable client connector so that temporary node does not occupy the port.
            var cfg = new IgniteConfiguration(section.IgniteConfiguration)
            {
                ClientConnectorConfigurationEnabled = false,
                CacheConfiguration = null
            };

            using (var ignite = Ignition.Start(cfg))
            {
                var args = new List<string>
                {
                    "-configFileName=" + _configPath,
                    "-assembly=" + typeof(ExamplesDll::Apache.Ignite.ExamplesDll.Compute.AverageSalaryJob)
                        .Assembly.Location
                };

                var proc = new IgniteProcess(args.ToArray());

                Assert.IsTrue(ignite.WaitTopology(2),
                    string.Format("Standalone node failed to join topology: [{0}]", proc.GetInfo()));

                Assert.IsTrue(proc.Alive, string.Format("Standalone node stopped unexpectedly: [{0}]",
                    proc.GetInfo()));
            }

            _remoteNodeStarted = true;
        }

        /// <summary>
        /// Stops standalone nodes.
        /// </summary>
        private void StopRemoteNodes()
        {
            if (_remoteNodeStarted)
            {
                IgniteProcess.KillAll();
                _remoteNodeStarted = false;
            }
        }

        /// <summary>
        /// Fixture setup.
        /// </summary>
        [TestFixtureSetUp]
        public void FixtureSetUp()
        {
            _changedEnvVar = EnvVar.Set(Classpath.EnvIgniteNativeTestClasspath, bool.TrueString);

            Directory.SetCurrentDirectory(PathUtil.IgniteHome);

            // Copy file to a temp location and replace multicast IP finder with static.
            _configPath = Path.GetTempFileName();
            
            var configText = File.ReadAllText(PathUtil.ExamplesAppConfigPath)
                .Replace("TcpDiscoveryMulticastIpFinder", "TcpDiscoveryStaticIpFinder");

            File.WriteAllText(_configPath, configText);

            _changedConfig = TestAppConfig.Change(_configPath);
        }

        /// <summary>
        /// Fixture teardown.
        /// </summary>
        [TestFixtureTearDown]
        public void FixtureTearDown()
        {
            _changedConfig.Dispose();

            Ignition.StopAll(true);

            IgniteProcess.KillAll();

            File.Delete(_configPath);

            _changedEnvVar.Dispose();
        }

        /// <summary>
        /// Test teardown.
        /// </summary>
        [TearDown]
        public void TearDown()
        {
            Ignition.ClientMode = false;
        }

        /// <summary>
        /// Gets the test cases for local-only scenario.
        /// </summary>
        // ReSharper disable once MemberCanBePrivate.Global
        // ReSharper disable once MemberCanBeMadeStatic.Global
        public IEnumerable<Example> TestCasesLocal
        {
            get { return AllExamples.Where(x => !RemoteOnlyExamples.Contains(x.ExampleType)); }
        }

        /// <summary>
        /// Gets the test cases for remote node scenario.
        /// </summary>
        // ReSharper disable once MemberCanBePrivate.Global
        // ReSharper disable once MemberCanBeMadeStatic.Global
        public IEnumerable<Example> TestCasesRemote
        {
            get
            {
                return AllExamples.Where(x => !LocalOnlyExamples.Contains(x.ExampleType));
            }
        }
    }
}
