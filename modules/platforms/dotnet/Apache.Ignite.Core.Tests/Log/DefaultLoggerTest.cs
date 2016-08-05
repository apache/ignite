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

namespace Apache.Ignite.Core.Tests.Log
{
    using System;
    using System.IO;
    using System.Linq;
    using Apache.Ignite.Core.Impl.Common;
    using NUnit.Framework;

    /// <summary>
    /// Tests the default logger.
    /// </summary>
    public class DefaultLoggerTest
    {
        /// <summary>
        /// Tests that default Java mechanism is used when there is no custom logger.
        /// </summary>
        [Test]
        public void TestJavaLogger()
        {
            // Run the test in a separate process because log4jlogger has some static state,
            // and after Ignite has been started once, it is not possible to start a new node 
            // with a different logger config.
            const string envVar = "DefaultLoggerTest.TestJavaLogger";

            if (Environment.GetEnvironmentVariable(envVar) == "true")
            {
                // Delete all log files from the work dir
                Func<string[]> getLogs = () =>
                    Directory.GetFiles(IgniteHome.Resolve(null), "dotnet-logger-test.log", SearchOption.AllDirectories);

                getLogs().ToList().ForEach(File.Delete);

                // Start Ignite and verify file log
                using (Ignition.Start(new IgniteConfiguration(TestUtils.GetTestConfiguration(false))
                {
                    SpringConfigUrl = @"config\log\custom-log.xml"
                }))
                {
                    // No-op.
                }

                using (var fs = File.Open(getLogs().Single(), FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
                {
                    var log = new StreamReader(fs).ReadToEnd();

                    // Check output from Java:
                    Assert.IsTrue(log.Contains(">>> Topology snapshot."));

                    // Check output from .NET:
                    Assert.IsTrue(log.Contains("Starting Ignite.NET " + typeof(Ignition).Assembly.GetName().Version));
                }
            }
            else
            {
                Environment.SetEnvironmentVariable(envVar, "true");
                TestUtils.RunTestInNewProcess(GetType().FullName, "TestJavaLogger");
            }
        }
    }
}
