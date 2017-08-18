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

namespace Apache.Ignite.Core.Tests.Deployment
{
    extern alias ExamplesDll;
    using System;
    using System.IO;
    using System.Threading;
    using Apache.Ignite.Core.Cluster;
    using Apache.Ignite.Core.Compute;
    using Apache.Ignite.Core.Deployment;
    using Apache.Ignite.Core.Impl;
    using Apache.Ignite.Core.Impl.Common;
    using Apache.Ignite.Core.Tests.Process;
    using Apache.Ignite.NLog;
    using NUnit.Framework;
    using Address = ExamplesDll::Apache.Ignite.ExamplesDll.Binary.Address;

    /// <summary>
    /// Tests peer assembly loading feature:
    /// when user-defined type is not found within loaded assemblies on remote node,
    /// corresponding assembly is sent and loaded automatically.
    /// </summary>
    public class PeerAssemblyLoadingTest
    {
        /// <summary>
        /// Tests that peer loading does not happen when not enabled in config, and error message is informative.
        /// </summary>
        [Test]
        public void TestDisabledPeerLoading()
        {
            TestDeployment(remoteCompute =>
            {
                var ex = Assert.Throws<AggregateException>(() => remoteCompute.Call(new ProcessNameFunc()))
                    .InnerException;

                Assert.IsNotNull(ex);
                Assert.AreEqual("Compute job has failed on remote node, examine InnerException for details.", 
                    ex.Message);

                Assert.IsNotNull(ex.InnerException);
                Assert.AreEqual("Failed to deserialize the job [errType=BinaryObjectException, errMsg=No matching " +
                                "type found for object", ex.InnerException.Message.Substring(0, 102));
            }, false);
        }

        /// <summary>
        /// Tests single assembly deployment (basic test).
        /// </summary>
        [Test]
        public void TestSingleAssembly()
        {
            TestDeployment(remoteCompute =>
            {
                Assert.AreEqual("Apache.Ignite", remoteCompute.Call(new ProcessNameFunc()));
            });
        }

        /// <summary>
        /// Tests that a type which requires multiple assemblies can be peer deployed.
        /// </summary>
        [Test]
        public void TestMultipleAssemblies()
        {
            TestDeployment(remoteCompute =>
            {
                // GetAddressFunc requires Tests and Examples assemblies.
                var result = remoteCompute.Apply(new GetAddressFunc(), 3);

                Assert.IsNotNull(result);

                Assert.AreEqual(3, result.Zip);
                Assert.AreEqual("addr3", result.Street);
            });
        }

        /// <summary>
        /// Tests that a type which requires multiple assemblies can be peer deployed.
        /// </summary>
        [Test]
        public void TestMultipleAssembliesIndirectDependency()
        {
            TestDeployment(remoteCompute =>
            {
                // Arg is object, but value is from Examples assembly.
                Assert.AreEqual("Apache.IgniteAddress [street=Central, zip=2]", remoteCompute.Call(
                    new ProcessNameFunc {Arg = new Address("Central", 2)}));
            });
        }

        /// <summary>
        /// Tests that a type which requires multiple assemblies can be peer deployed.
        /// </summary>
        [Test]
        public void TestMultipleAssembliesIndirectDependencyMultiLevel()
        {
            TestDeployment(remoteCompute =>
            {
                // Arg is object, value is from Apache.Ignite.Log4Net, and it further depends on NLog.
                Assert.AreEqual("Apache.IgniteApache.Ignite.NLog.IgniteNLogLogger", remoteCompute.Call(
                    new ProcessNameFunc {Arg = new IgniteNLogLogger()}));
            });
        }

        /// <summary>
        /// Tests the runtime dependency: AssemblyResolve event fires during job execution,
        /// not during deserialization. This happens with static classes, for example.
        /// </summary>
        [Test]
        public void TestRuntimeDependency()
        {
            TestDeployment(remoteCompute =>
            {
                Assert.AreEqual("dcba", remoteCompute.Apply(new RuntimeDependencyFunc(), "abcd"));
            });
        }

        /// <summary>
        /// Tests the cache get operation on remote node.
        /// </summary>
        [Test]
        public void TestCacheGetOnRemoteNode()
        {
            TestDeployment(remoteCompute =>
            {
                var cache = remoteCompute.ClusterGroup.Ignite.GetOrCreateCache<int, Address>("addr");
                cache[1] = new Address("street", 123);

                // This will fail for <object, object> func, because cache operations are not p2p-enabled.
                // However, generic nature of the func causes Address to be peer-deployed before cache.Get call.
                var func = new CacheGetFunc<int, Address>
                {
                    CacheName = cache.Name,
                    Key = 1
                };

                var res = remoteCompute.Call(func);
                Assert.AreEqual("street", res.Street);
            });
        }

        /// <summary>
        /// Tests the peer deployment.
        /// </summary>
        public static void TestDeployment(Action<ICompute> test, bool enablePeerDeployment = true)
        {
            TestDeployment((IClusterGroup remoteCluster) => test(remoteCluster.GetCompute()), enablePeerDeployment);
        }

        /// <summary>
        /// Tests the peer deployment.
        /// </summary>
        private static void TestDeployment(Action<IClusterGroup> test, bool enablePeerDeployment = true)
        {
            TestDeployment(ignite => test(ignite.GetCluster().ForRemotes()), enablePeerDeployment);
        }

        /// <summary>
        /// Tests the peer deployment.
        /// </summary>
        public static void TestDeployment(Action<IIgnite> test, bool enablePeerDeployment = true)
        {
            // Copy Apache.Ignite.exe and Apache.Ignite.Core.dll 
            // to a separate folder so that it does not locate our assembly automatically.
            var folder = IgniteUtils.GetTempDirectoryName();
            foreach (var asm in new[] {typeof(IgniteRunner).Assembly, typeof(Ignition).Assembly})
            {
                Assert.IsNotNull(asm.Location);
                File.Copy(asm.Location, Path.Combine(folder, Path.GetFileName(asm.Location)));
            }

            var exePath = Path.Combine(folder, "Apache.Ignite.exe");

            // Start separate Ignite process without loading current dll.
            // ReSharper disable once AssignNullToNotNullAttribute
            var config = Path.Combine(Path.GetDirectoryName(typeof(PeerAssemblyLoadingTest).Assembly.Location),
                "Deployment\\peer_assembly_app.config");

            var proc = IgniteProcess.Start(exePath, IgniteHome.Resolve(null), null,
                "-ConfigFileName=" + config, "-ConfigSectionName=igniteConfiguration");

            Thread.Sleep(300);
            Assert.IsFalse(proc.HasExited);

            // Start Ignite and execute computation on remote node.
            var cfg = new IgniteConfiguration(TestUtils.GetTestConfiguration())
            {
                PeerAssemblyLoadingMode = enablePeerDeployment
                    ? PeerAssemblyLoadingMode.CurrentAppDomain
                    : PeerAssemblyLoadingMode.Disabled
            };

            using (var ignite = Ignition.Start(cfg))
            {
                Assert.IsTrue(ignite.WaitTopology(2));

                for (var i = 0; i < 10; i++)
                {
                    test(ignite);
                }
            }
        }

        /// <summary>
        /// Tears down the test.
        /// </summary>
        [TearDown]
        public void TearDown()
        {
            Ignition.StopAll(true);
            IgniteProcess.KillAll();
        }
    }
}
