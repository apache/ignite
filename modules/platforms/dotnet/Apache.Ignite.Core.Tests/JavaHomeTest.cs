/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

namespace Apache.Ignite.Core.Tests
{
    using NUnit.Framework;

    /// <summary>
    /// Tests the JAVA_HOME detection.
    /// </summary>
    public class JavaHomeTest
    {
        /** Environment variable: JAVA_HOME. */
        private const string EnvJavaHome = "JAVA_HOME";

        /// <summary>
        /// Tests the detection.
        /// </summary>
        [Test]
        public void TestDetection([Values(null, "c:\\invalid111")] string javaHome)
        {
            using (EnvVar.Set(EnvJavaHome, javaHome))
            using (var ignite = Ignition.Start(TestUtils.GetTestConfiguration()))
            {
                Assert.IsNotNull(ignite);
            }
        }
    }
}
