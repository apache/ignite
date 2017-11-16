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

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Apache.Ignite.Core.Tests.DotNetCore
{
    /// <summary>
    /// Tests Ignite startup.
    /// </summary>
    [TestClass]
    public class IgnitionStartTest
    {
        /// <summary>
        /// Tests that Ignite starts with default configuration.
        /// </summary>
        [TestMethod]
        public void TestIgniteStartsWithDefaultConfig()
        {
            var jvmDll = FindJvmDll().FirstOrDefault();
            Console.WriteLine(jvmDll);

            var ignite = Ignition.Start(TestUtils.GetTestConfiguration());
            Assert.IsNotNull(ignite);
        }

        private static IEnumerable<string> FindJvmDll()
        {
            const string javaExec = "/usr/bin/java";
            if (!File.Exists(javaExec))
            {
                return Enumerable.Empty<string>();
            }

            // /usr/lib/jvm/java-8-openjdk-amd64/jre/bin/java
            var file = BashExecute("readlink -f /usr/bin/java");
            if (!File.Exists(file))
            {
                return Enumerable.Empty<string>();
            }

            // /usr/lib/jvm/java-8-openjdk-amd64/jre/lib/amd64/server/libjvm.so
            var libFolder = Path.GetFullPath(Path.Combine(Path.GetDirectoryName(file), "../lib/"));
            if (!Directory.Exists(libFolder))
            {
                return Enumerable.Empty<string>();
            }

            return Directory.GetFiles(libFolder, "libjvm.so", SearchOption.AllDirectories);
        }

        /// <summary>
        /// Fixture cleanup.
        /// </summary>
        [ClassCleanup]
        public static void ClassCleanup()
        {
            Ignition.StopAll(true);
        }

        private static string BashExecute(string args)
        {
            var escapedArgs = args.Replace("\"", "\\\"");

            var process = new Process
            {
                StartInfo = new ProcessStartInfo
                {
                    FileName = "/bin/bash",
                    Arguments = $"-c \"{escapedArgs}\"",
                    RedirectStandardOutput = true,
                    UseShellExecute = false,
                    CreateNoWindow = true
                }
            };

            process.Start();
            
            var res = process.StandardOutput.ReadToEnd();
            process.WaitForExit();

            return res;
        }
    }
}
