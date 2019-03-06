/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

namespace Apache.Ignite.Core.Tests.ApiParity
{
    using System.Collections.Generic;
    using Apache.Ignite.Core.Cache.Configuration;
    using NUnit.Framework;

    /// <summary>
    /// Tests that .NET <see cref="CacheConfiguration"/> has all properties from Java configuration APIs.
    /// </summary>
    public class CacheConfigurationParityTest
    {
        /** Known property name mappings. */
        private static readonly Dictionary<string, string> KnownMappings = new Dictionary<string, string>
        {
            {"isStoreKeepBinary", "KeepBinaryInStore"},
            {"Affinity", "AffinityFunction"},
            {"DefaultLockTimeout", "LockTimeout"}
        };

        /** Properties that are not needed on .NET side. */
        private static readonly string[] UnneededProperties =
        {
            // False matches.
            "clearQueryEntities",

            // Java-specific.
            "CacheStoreSessionListenerFactories",
            "CacheEntryListenerConfigurations",
            "TopologyValidator",
            "SqlFunctionClasses",
            "Interceptor",
            "EvictionFilter",
            "IndexedTypes",

            // Deprecated, but not marked so.
            "AffinityMapper"
        };

        /** Properties that are missing on .NET side. */
        private static readonly string[] MissingProperties =
        {
            "NodeFilter",  // IGNITE-2890
            "EvictionPolicyFactory",  // IGNITE-6649,
            "isSqlOnheapCacheEnabled",  // IGNITE-7379,
            "SqlOnheapCacheMaxSize", // IGNITE-7379,
            "isEventsDisabled",  // IGNITE-7346
            "DiskPageCompression", // IGNITE-10332
            "DiskPageCompressionLevel" // IGNITE-10332
        };

        /// <summary>
        /// Tests the cache configuration parity.
        /// </summary>
        [Test]
        public void TestCacheConfiguration()
        {
            ParityTest.CheckConfigurationParity(
                @"modules\core\src\main\java\org\apache\ignite\configuration\CacheConfiguration.java", 
                typeof(CacheConfiguration),
                UnneededProperties,
                MissingProperties,
                KnownMappings);
        }
    }
}
