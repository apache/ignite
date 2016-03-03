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
    using Apache.Ignite.Core.Impl.Common;
    using NUnit.Framework;

    /// <summary>
    /// Tests custom JAR deployment, classpath and IgniteHome logic.
    /// </summary>
    public class DeploymentTest
    {
        /// <summary>
        /// Tests the custom deployment where IGNITE_HOME can't be resolved, and there is a user-defined classpath.
        /// </summary>
        [Test]
        public void TestCustomDeployment()
        {
            // Create temp folder
            var folder = GetTempFolder();

            // Copy jars
            var home = IgniteHome.Resolve(null);

            var jarNames = new[] {@"\ignite-core-", @"\cache-api-1.0.0.jar", @"\ignite-indexing\", @"\ignite-spring\" };

            var jars = Directory.GetFiles(home, "*.jar", SearchOption.AllDirectories)
                .Where(jarPath => jarNames.Any(jarPath.Contains)).ToArray();

            Assert.Greater(jars.Length, 3);

            foreach (var jar in jars)
                // ReSharper disable once AssignNullToNotNullAttribute
                File.Copy(jar, Path.Combine(folder, Path.GetFileName(jar)));

            // Build classpath
            var classpath = string.Join(";", Directory.GetFiles(folder));

            // Copy .NET binaries
            // Start a node and make sure it works properly
        }

        private static string GetTempFolder()
        {
            const string prefix = "ig-test-";
            var temp = Path.GetTempPath();

            for (int i = 0; i < int.MaxValue; i++)
            {
                {
                    try
                    {
                        var path = Path.Combine(temp, prefix, i.ToString());

                        return Directory.CreateDirectory(path).FullName;
                    }
                    catch (Exception)
                    {
                        continue;
                    }
                }

            }

            throw new InvalidOperationException();
        }
    }
}
