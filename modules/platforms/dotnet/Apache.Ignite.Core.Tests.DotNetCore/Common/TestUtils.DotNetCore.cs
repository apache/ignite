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
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;

    public static partial class TestUtils
    {
        /** */
        private static readonly IList<string> JvmOpts =
            new List<string>
            {
                "-Duser.timezone=UTC"

                // Uncomment to debug Java
                //"-Xdebug",
                //"-Xnoagent",
                //"-Djava.compiler=NONE",
                //"-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005"
            };

        /// <summary>
        /// Gets the default code-based test configuration.
        /// </summary>
        public static IgniteConfiguration GetTestConfiguration(string name = null)
        {
            return new IgniteConfiguration
            {
                DiscoverySpi = GetStaticDiscovery(),
                Localhost = "127.0.0.1",
                JvmOptions = JvmOpts,
                IgniteInstanceName = name
            };
        }

        /// <summary>
        /// Serializes and deserializes back an object.
        /// </summary>
        public static T SerializeDeserialize<T>(T obj)
        {
            var marshType = typeof(IIgnite).Assembly.GetType("Apache.Ignite.Core.Impl.Binary.Marshaller");
            var marsh = Activator.CreateInstance(marshType, new object[] { null, null });
            marshType.GetProperty("CompactFooter").SetValue(marsh, false);

            var bytes = marshType.GetMethod("Marshal").MakeGenericMethod(typeof(object))
                .Invoke(marsh, new object[] { obj });

            var res = marshType.GetMethods().Single(mi =>
                    mi.Name == "Unmarshal" && mi.GetParameters().First().ParameterType == typeof(byte[]))
                .MakeGenericMethod(typeof(object)).Invoke(marsh, new[] { bytes, 0 });

            return (T)res;
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
