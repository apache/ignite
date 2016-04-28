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
    using System.IO;
    using System.Linq;
    using System.Reflection;
    using System.ServiceProcess;
    using Apache.Ignite.Core.Tests.Process;
    using NUnit.Framework;

    /// <summary>
    /// Tests windows service deployment and lifecycle.
    /// </summary>
    public class WindowsServiceTest
    {
        /// <summary>
        /// Tests that service stops when Ignition stops.
        /// </summary>
        [Test]
        public void TestStopFromJava()
        {
            var exePath = typeof(IgniteRunner).Assembly.Location;
            var springPath = Path.GetFullPath("config\\compute\\compute-grid2.xml");

            StopServiceAndUninstall();

            IgniteProcess.Start(exePath, string.Empty, args: new[]
            {
                "-springConfigUrl=" + springPath,
                "-jvmClasspath=" + TestUtils.CreateTestClasspath(),
                "/install"
            });



            // TODO: Deploy and start service
            // Start ignite from here
            // ExecuteJavaTask on the remote service node to stop Ignite there
            // Check that service has stopped
        }

        private static void StopServiceAndUninstall()
        {
            var controller = ServiceController.GetServices().FirstOrDefault(x => x.ServiceName == "Apache Ignite.NET");

            if (controller != null && controller.CanStop)
                controller.Stop();

            var exePath = typeof(IgniteRunner).Assembly.Location;
            IgniteProcess.Start(exePath, string.Empty, args: new[] {"/uninstall"});
        }
    }
}
