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
    using System.Diagnostics;
    using System.IO;
    using System.Linq;
    using Apache.Ignite.Core.Failure;
    using Apache.Ignite.Core.Log;
    using Apache.Ignite.Core.Tests.DotNetCore.Common;

    public static partial class TestUtils
    {
        /// <summary>
        /// Gets the default code-based test configuration.
        /// </summary>
        public static IgniteConfiguration GetTestConfiguration(string name = null)
        {
            TestLogger.Instance.Info("GetTestConfiguration: " + GetTestName());

            Environment.SetEnvironmentVariable("IGNITE_NATIVE_TEST_CLASSPATH", "true");
            Environment.SetEnvironmentVariable("IGNITE_NET_SUPPRESS_JAVA_ILLEGAL_ACCESS_WARNINGS", "true");

            return new IgniteConfiguration
            {
                DiscoverySpi = GetStaticDiscovery(),
                Localhost = "127.0.0.1",
                JvmOptions = TestJavaOptions(),
                IgniteInstanceName = name,
                Logger = TestLogger.Instance,
                WorkDirectory = WorkDir,
                FailureHandler = new NoOpFailureHandler()
            };
        }

        /// <summary>
        /// Gets the name of the test.
        /// </summary>
        private static string GetTestName()
        {
            var st = new StackTrace();

            for (var i = 0; i < st.FrameCount; i++)
            {
                var frame = st.GetFrame(i);
                var method = frame.GetMethod();

                if (method.DeclaringType != typeof(TestUtils) 
                    && method.DeclaringType != typeof(TestBase))
                {
                    return $"{method.DeclaringType.Name}.{method.Name}";
                }
            }

            return st.GetFrames().Skip(2).Select(x => x.ToString()).FirstOrDefault() ?? "unknown";
        }

        /// <summary>
        /// Creates a uniquely named, empty temporary directory on disk and returns the full path of that directory.
        /// </summary>
        /// <returns>The full path of the temporary directory.</returns>
        internal static string GetTempDirectoryName()
        {
            var baseDir = Path.Combine(Path.GetTempPath(), "IgniteTemp_");

            while (true)
            {
                try
                {
                    return Directory.CreateDirectory(baseDir + Path.GetRandomFileName()).FullName;
                }
                catch (IOException)
                {
                    // Expected
                }
                catch (UnauthorizedAccessException)
                {
                    // Expected
                }
            }
        }
    }
}
