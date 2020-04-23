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
    using Apache.Ignite.Core.Cache.Configuration;
    using Apache.Ignite.Core.Impl.Common;
    using NUnit.Framework;
    using LogLevel = Apache.Ignite.Core.Log.LogLevel;

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
                    Directory.GetFiles(IgniteHome.Resolve(), "dotnet-logger-test.log", SearchOption.AllDirectories);

                getLogs().ToList().ForEach(File.Delete);

                var cfg = new IgniteConfiguration(TestUtils.GetTestConfiguration(false))
                {
                    SpringConfigUrl = @"config\log\custom-log.xml",
                    CacheConfiguration = new[]
                    {
                        new CacheConfiguration("cache1", new QueryEntity(typeof(uint), typeof(ulong)))
                    }
                };

                // Start Ignite and verify file log
                using (var ignite = Ignition.Start(cfg))
                {
                    // Log with all levels
                    var log = ignite.Logger;
                    var levels = new[] {LogLevel.Trace, LogLevel.Info, LogLevel.Debug, LogLevel.Warn, LogLevel.Error};

                    foreach (var level in levels)
                    {
                        var ex = new Exception("EXCEPTION_TEST_" + level);

                        log.Log(level, "DOTNET-" + level, null, null, "=DOTNET=", null, ex);
                    }
                }

                using (var fs = File.Open(getLogs().Single(), FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
                {
                    var log = new StreamReader(fs).ReadToEnd();

                    // Check output from Java:
                    Assert.IsTrue(log.Contains(">>> Topology snapshot."));

                    // Check output from .NET:
                    Assert.IsTrue(log.Contains("Starting Ignite.NET " + typeof(Ignition).Assembly.GetName().Version));

                    Assert.IsTrue(log.Contains(
                        "Validating cache configuration 'cache1', QueryEntity 'java.lang.Integer:java.lang." +
                        "Long': Type 'System.UInt32' maps to Java type 'java.lang.Integer' using unchecked " +
                        "conversion. This may cause issues in SQL queries. You can use 'System.Int32' " +
                        "instead to achieve direct mapping."));


                    // Check custom log output (trace is disabled, errors are logged from Warn and up):
                    Assert.IsTrue(log.Contains("[INFO ][main][=DOTNET=] DOTNET-Info"));

                    Assert.IsTrue(log.Contains("[DEBUG][main][=DOTNET=] DOTNET-Debug"));

                    Assert.IsTrue(log.Contains("[WARN ][main][=DOTNET=] DOTNET-Warn"));
                    Assert.IsTrue(log.Contains("class org.apache.ignite.IgniteException: " +
                                               "Platform error:System.Exception: EXCEPTION_TEST_Warn"));

                    Assert.IsTrue(log.Contains("[ERROR][main][=DOTNET=] DOTNET-Error"));
                    Assert.IsTrue(log.Contains("class org.apache.ignite.IgniteException: " +
                                               "Platform error:System.Exception: EXCEPTION_TEST_Error"));
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
