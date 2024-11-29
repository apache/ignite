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

namespace Apache.Ignite.Core.Tests.Cache.Platform
{
    using System;
    using System.IO;
    using System.Linq;
    using Apache.Ignite.Core.Cache.Configuration;
    using Apache.Ignite.Core.Cluster;
    using NUnit.Framework;

    /// <summary>
    /// Tests node filter of platform cache.
    /// </summary>
    public sealed class PlatformCacheNodeFilterTest
    {
        /** */
        public const string NamePref = "node_";
        
        /** */
        private const string CacheName = "cache";

        /** */
        private const int NodesCnt = 4;

        /** */
        private const int ClientIdx = NodesCnt - 1;

        /** */
        private const string WithFilterPref = "with-node-filter-";
        
        /** */
        private const string NoFilterPref = "no-node-filter-";
        
        /// <summary>
        /// Test tear down.
        /// </summary>
        [TearDown]
        public void TearDown()
        {
            Ignition.StopAll(true);
        }

        /// <summary>
        /// Tests dynamic starts of native cache with .NET filter for platform cache.
        /// Platform cache should be started only on the specific nodes.
        /// </summary>
        [TestCase(CacheMode.Partitioned)]
        [TestCase(CacheMode.Replicated)]
        public void TestDynamicStart(CacheMode cacheMode)
        {
            var nodes = RunNodes(i => GetConfiguration(i));

            DoTestDynamic(nodes, 0, "StartFromFilteredSrv", cacheMode, 0, 1);
            DoTestDynamic(nodes, 1, "StartFromNonFilteredSrv", cacheMode, 0, 2);
            DoTestDynamic(nodes, ClientIdx, "StartFromFilteredCln", cacheMode, 1, ClientIdx);
            DoTestDynamic(nodes, ClientIdx, "StartFromNonFilteredCln", cacheMode, 0, 2);
        }

        private void DoTestDynamic(IIgnite[] nodes, int startCacheIdx, string cacheName, CacheMode cacheMode, 
            params int[] filteredNodeIdxs)
        {
            nodes[startCacheIdx].CreateCache<int, int>(GetCacheConfiguration(cacheName, cacheMode, filteredNodeIdxs));

            for (var i = 0; i < NodesCnt; i++)
            {
                bool hasPlatformCache = nodes[i].GetCache<int, int>(cacheName).HasPlatformCache;

                Assert.IsTrue(hasPlatformCache == filteredNodeIdxs.Contains(i));
            }
        }

        /// <summary>
        /// Tests that platform cache starts only on the specific nodes.
        /// Native cache is static, i.e. specified in configuration.
        /// </summary>
        [TestCase(CacheMode.Partitioned)]
        [TestCase(CacheMode.Replicated)]
        public void TestDotnetFilter(CacheMode cacheMode)
        {
            DoTestStatic(
                DotnetConfig(cacheMode, 0, 3), 
                0, 3);
        }


        /// <summary>
        /// Tests that platform cache starts only on the specific nodes.
        /// Native cache is static, i.e. specified in configuration.
        /// </summary>
        [TestCase(CacheMode.Partitioned)]
        [TestCase(CacheMode.Replicated)]
        public void TestJavaFilter(CacheMode cacheMode)
        {
            DoTestStatic(
                XmlConfig(WithFilterPref, cacheMode), 
                2, 3);
        }

        /// <summary>
        /// Tests that platform cache starts on all nodes, when node filter is not configured.
        /// </summary>
        [TestCase(CacheMode.Partitioned)]
        [TestCase(CacheMode.Replicated)]
        public void TestNoFilterDotnetConfig(CacheMode cacheMode)
        {
            DoTestStatic(
                DotnetConfig(cacheMode), 
                0, 1, 2, 3);
        }

        /// <summary>
        /// Tests that platform cache starts on all nodes, when node filter is not configured.
        /// </summary>
        [TestCase(CacheMode.Partitioned)]
        [TestCase(CacheMode.Replicated)]
        public void TestNoFilterJavaConfig(CacheMode cacheMode)
        {
            DoTestStatic(
                XmlConfig(NoFilterPref, cacheMode), 
                0, 1, 2, 3);
        }

        private void DoTestStatic(Func<int, IgniteConfiguration> cfgFunc, params int[] expIdxs)
        {
            var nodes = RunNodes(cfgFunc);

            for (var i = 0; i < NodesCnt; i++)
            {
                var hasCache = nodes[i].GetCache<int, int>(CacheName).HasPlatformCache;
                
                Assert.IsTrue(hasCache == expIdxs.Contains(i), $"Unexpected state: [nodeIdx={i}, hasCache={hasCache}]");
            }
        }

        private Func<int, IgniteConfiguration> DotnetConfig(CacheMode cacheMode, params int[] filteredNodeIdxs)
        {
            return i => GetConfiguration(i, GetCacheConfiguration(CacheName, cacheMode, filteredNodeIdxs));
        }

        private Func<int, IgniteConfiguration> XmlConfig(string cfgUrlPref, CacheMode cacheMode)
        {
            return i => new IgniteConfiguration(TestUtils.GetTestConfiguration())
            {
                IgniteInstanceName = NamePref + i,
                SpringConfigUrl = ConfigUrl(cfgUrlPref, cacheMode),
                ClientMode = i == ClientIdx
            };
        }

        private IIgnite[] RunNodes(Func<int, IgniteConfiguration> cfgFunc)
        {
            return Enumerable.Range(0, NodesCnt)
                .Select(i => Ignition.Start(cfgFunc.Invoke(i)))
                .ToArray();
        }

        private IgniteConfiguration GetConfiguration(int idx, params CacheConfiguration[] cacheCfg)
        {
            var name = NamePref + idx;

            var igniteConfig = new IgniteConfiguration(TestUtils.GetTestConfiguration())
            {
                IgniteInstanceName = name,
                ClientMode = idx == ClientIdx,
                CacheConfiguration = cacheCfg
            };

            return igniteConfig;
        }

        private CacheConfiguration GetCacheConfiguration(string cacheName, CacheMode cacheMode, params int[] filteredNodeIdxs)
        {
            return new CacheConfiguration(cacheName)
            {
                CacheMode = cacheMode,
                Backups = 1,
                PlatformCacheConfiguration = new PlatformCacheConfiguration
                {
                    NodeFilter = filteredNodeIdxs.Length == 0 ? null : new NodeNameFilter(filteredNodeIdxs)
                }
            };
        }

        private string ConfigUrl(string filePref, CacheMode cacheMode)
        {
            var suf = (cacheMode == CacheMode.Replicated ? "replicated" : "partitioned") + ".xml";

            return Path.Combine("Config", "Cache", "Platform", filePref + suf);
        }
    }

    /// <summary>
    /// .NET test node filter.
    /// </summary>
    [Serializable]
    public class NodeNameFilter : IClusterNodeFilter
    {
        private readonly string[] _names;

        public NodeNameFilter(int[] idxs)
        {
            _names = idxs.Select(i => PlatformCacheNodeFilterTest.NamePref + i)
                .ToArray();
        }

        public bool Invoke(IClusterNode node)
        {
            return _names.Contains(node.GetAttribute<String>("org.apache.ignite.ignite.name"));
        }
    }
}