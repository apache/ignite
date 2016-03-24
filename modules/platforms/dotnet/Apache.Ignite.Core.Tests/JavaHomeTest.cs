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
    using NUnit.Framework;

    /// <summary>
    /// Tests the JAVA_HOME detection.
    /// </summary>
    public class JavaHomeTest
    {
        /** Environment variable: JAVA_HOME. */
        private const string EnvJavaHome = "JAVA_HOME";

        /** Backed up value. */
        private string _javaHomeBackup;

        /// <summary>
        /// Fixture set up.
        /// </summary>
        [TestFixtureSetUp]
        public void FixtureSetUp()
        {
            _javaHomeBackup = Environment.GetEnvironmentVariable(EnvJavaHome);

            Environment.SetEnvironmentVariable(EnvJavaHome, null);
        }

        /// <summary>
        /// Fixture tear down.
        /// </summary>
        [TestFixtureTearDown]
        public void FixtureTearDown()
        {
            Environment.SetEnvironmentVariable(EnvJavaHome, _javaHomeBackup);
        }

        /// <summary>
        /// Tests the detection.
        /// </summary>
        [Test]
        public void TestDetection([Values(null, "c:\\invalid111")] string javaHome)
        {
            Environment.SetEnvironmentVariable(EnvJavaHome, javaHome);

            using (var ignite = Ignition.Start(TestUtils.GetTestConfiguration()))
            {
                Assert.IsNotNull(ignite);
                Console.WriteLine("Detected JVM dll path: " + ignite.GetConfiguration().JvmDllPath);
            }
        }
    }
}
