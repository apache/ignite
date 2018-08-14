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

namespace Apache.Ignite.Core.Tests.ApiParity
{
    using Apache.Ignite.Core.Cache;
    using NUnit.Framework;

    /// <summary>
    /// Tests that <see cref="ICache{TK,TV}"/> has all APIs from Java Ignite interface.
    /// </summary>
    public class CacheParityTest
    {
        /** Members that are not needed on .NET side. */
        private static readonly string[] UnneededMembers =
        {
            // Java-specific.
            "close",
            "mxBean",
            "localMxBean",
            "indexReadyFuture",

            // No needed, see IIgnite.DestroyCache
            "destroy",

            // The following look pointless, same as get, getAll, etc:
            "Entry",
            "EntryAsync",
            "Entries",
            "EntriesAsync",

            // Questionable methods:
            "AllOutTx",
            "AllOutTxAsync"
        };

        /** Members that are missing on .NET side and should be added in future. */
        private static readonly string[] MissingMembers =
        {
            "queryDetailMetrics",  // IGNITE-6680
            "resetQueryDetailMetrics",  // IGNITE-6680
            "sizeLong",  // 	IGNITE-6563
            "sizeLongAsync",  // IGNITE-6563
            "localSizeLong",  // IGNITE-6563
            "enableStatistics",  // IGNITE-7276
            "clearStatistics"  // IGNITE-9017
        };

        /// <summary>
        /// Tests the API parity.
        /// </summary>
        [Test]
        public void TestCache()
        {
            ParityTest.CheckInterfaceParity(
                @"modules\core\src\main\java\org\apache\ignite\IgniteCache.java",
                typeof(ICache<,>),
                UnneededMembers,
                MissingMembers);
        }
    }
}