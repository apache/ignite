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
    using System.Collections.Generic;
    using Apache.Ignite.Core.Cache.Configuration;
    using NUnit.Framework;

    /// <summary>
    /// Tests that .NET <see cref="CacheConfiguration"/> has all properties from Java configuration APIs.
    /// </summary>
    [Ignore(ParityTest.IgnoreReason)]
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
