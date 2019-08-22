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

namespace Apache.Ignite.Core.Tests.ApiParity
{
    using System.Collections.Generic;
    using NUnit.Framework;

    /// <summary>
    /// Tests that <see cref="IIgnite"/> has all APIs from Java Ignite interface.
    /// </summary>
    [Ignore(ParityTest.IgnoreReason)]
    public class IgniteParityTest
    {
        /** Methods that are not needed on .NET side. */
        private static readonly string[] UnneededMembers =
        {
            "scheduler",
            "close",
            "executorService",
            "fileSystem",
            "fileSystems"
        };

        /** Members that are missing on .NET side and should be added in future. */
        private static readonly string[] MissingMembers =
        {
            "createCaches",   // IGNITE-7100
            "orCreateCaches", // IGNITE-7100
            "destroyCaches",  // IGNITE-7100

            // Data structures.
            "atomicStamped", // IGNITE-7104
            "countDownLatch", // IGNITE-1418
            "semaphore", // IGNITE-7103
            "reentrantLock", // IGNITE-7105
            "queue", // IGNITE-1417
            "set" // IGNITE-6834
        };

        /** Known name mappings. */
        private static readonly Dictionary<string, string> KnownMappings = new Dictionary<string, string>
        {
            {"message", "GetMessaging"},
            {"log", "Logger"}
        };

        /// <summary>
        /// Tests the IIgnite parity.
        /// </summary>
        [Test]
        public void TestIgnite()
        {
            ParityTest.CheckInterfaceParity(
                @"modules\core\src\main\java\org\apache\ignite\Ignite.java",
                typeof(IIgnite),
                UnneededMembers,
                MissingMembers,
                KnownMappings);
        }
    }
}
