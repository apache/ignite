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
    using System.Linq;
    using System.ServiceProcess;
    using Apache.Ignite.Core.Cluster;
    using Apache.Ignite.Core.Tests.Process;
    using NUnit.Framework;

    /// <summary>
    /// Tests windows service deployment and lifecycle.
    /// <para />
    /// NOTE: This fixture requires administrative privileges.
    /// </summary>
    public class WindowsServiceTest
    {
        /// <summary>
        /// Test fixture set up.
        /// </summary>
        [TestFixtureSetUp]
        public void TestFixtureSetUp()
        {
            StopServiceAndUninstall();
        }

        /// <summary>
        /// Test fixture tear down.
        /// </summary>
        [TestFixtureTearDown]
        public void TestFixtureTearDown()
        {
            StopServiceAndUninstall();
        }

        /// <summary>
        /// Tests that service stops when Ignition stops.
        /// </summary>
        [Test]
        public void TestStopFromJava()
        {
            var exePath = typeof(IgniteRunner).Assembly.Location;
            var springPath = Path.GetFullPath(@"config\compute\compute-grid1.xml");

            IgniteProcess.Start(exePath, string.Empty, args: new[]
            {
                "/install",
                "ForceTestClasspath=true",
                "-springConfigUrl=" + springPath,
                "-J-Xms513m",
                "-J-Xmx555m"
            }).WaitForExit();

            var service = GetIgniteService();
            Assert.IsNotNull(service);

            service.Start();  // see IGNITE_HOME\work\log for service instance logs
            service.WaitForStatus(ServiceControllerStatus.Running, TimeSpan.FromSeconds(30));

            using (var ignite = Ignition.Start(new IgniteConfiguration(TestUtils.GetTestConfiguration())
            {
                SpringConfigUrl = springPath
            }))
            {
                Assert.IsTrue(ignite.WaitTopology(2), "Failed to join with service node");

                // Stop remote node via Java task
                // Doing so will fail the task execution
                Assert.Throws<ClusterGroupEmptyException>(() =>
                    ignite.GetCluster().ForRemotes().GetCompute().ExecuteJavaTask<object>(
                        "org.apache.ignite.platform.PlatformStopIgniteTask", ignite.Name));

                Assert.IsTrue(ignite.WaitTopology(1), "Failed to stop remote node");

                // Check that service has stopped
                service.WaitForStatus(ServiceControllerStatus.Stopped, TimeSpan.FromSeconds(30));
            }
        }

        /// <summary>
        /// Stops the service and uninstalls it.
        /// </summary>
        private static void StopServiceAndUninstall()
        {
            var controller = GetIgniteService();

            if (controller != null)
            {
                if (controller.CanStop)
                    controller.Stop();

                controller.WaitForStatus(ServiceControllerStatus.Stopped, TimeSpan.FromSeconds(30));

                var exePath = typeof(IgniteRunner).Assembly.Location;
                IgniteProcess.Start(exePath, string.Empty, args: new[] {"/uninstall"}).WaitForExit();
            }
        }

        /// <summary>
        /// Gets the ignite service.
        /// </summary>
        private static ServiceController GetIgniteService()
        {
            return ServiceController.GetServices().FirstOrDefault(x => x.ServiceName.StartsWith("Apache Ignite.NET"));
        }
    }
}
