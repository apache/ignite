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

namespace Apache.Ignite.Core.Tests.Unmanaged
{
    using System.Linq;
    using NUnit.Framework;

    /// <summary>
    /// Tests JVM thread detach - verify that there are no leaks caused by JNI.
    /// </summary>
    public class JniThreadDetachTest : TestBase
    {
        /// <summary>
        /// Tests that using Ignite APIs from CLR threads does not leak JVM threads.
        /// </summary>
        [Test]
        public void TestUseIgniteFromClrThreadsDoesNotLeakJvmThreads()
        {
            var cache = Ignite.GetOrCreateCache<int, int>("c");
            cache.Put(0, 0);

            var threadNamesBefore = GetJavaThreadNames();

            TestUtils.RunMultiThreaded(() => cache.Put(1, 1), 10);

            var threadNamesAfter = GetJavaThreadNames();
            Assert.AreEqual(threadNamesBefore, threadNamesAfter);
            Assert.IsNotEmpty(threadNamesAfter);
        }

        /// <summary>
        /// Gets Java thread names.
        /// </summary>
        private string[] GetJavaThreadNames()
        {
            return Ignite.GetCompute()
                .ExecuteJavaTask<string[]>("org.apache.ignite.platform.PlatformThreadNamesTask", null)
                .Where(x => !x.StartsWith("pub-#") && !x.StartsWith("jvm-"))
                .OrderBy(x => x)
                .ToArray();
        }
    }
}
