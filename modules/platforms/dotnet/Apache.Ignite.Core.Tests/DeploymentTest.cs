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
            // 1. Create temp folder
            var folder = GetTempFolder();


            // 2. Copy jars 
            // 3. Copy .NET binaries
            // 4. Start a node and make sure it works properly
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
