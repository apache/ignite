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
    using System.Collections.Generic;
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
        private const string WithFilterPref = "with-node-filter";
        
        /** */
        private const string NoFilterPref = "no-node-filter";

        /** */
        private const string AttrFilterPref = "attr-node-filter";

        /** */
        private const string TestAttr = "test_attribute";

        /** */
        private const string FilteredVal = "filtered";

        /** */
        private const string NonFilteredVal = "non_filtered";

        private readonly AttributeNodeFilter _attributeNodeFilter = new AttributeNodeFilter
        {
            Attributes = new Dictionary<string, object>
            {
                { TestAttr, FilteredVal }
            }
        };

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

        private static void DoTestDynamic(IIgnite[] nodes, int startCacheIdx, string cacheName, CacheMode cacheMode,
            params int[] filteredNodeIdxs)
        {
            nodes[startCacheIdx].CreateCache<int, int>(GetCacheConfiguration(cacheName, cacheMode, filteredNodeIdxs));

            CheckHasPlatformCache(nodes, filteredNodeIdxs, cacheName);

            var entries = nodes[..(ClientIdx - 1)]
                .Select(ignite => TestUtils.GetKey(ignite, cacheName, null,true))
                .ToDictionary(i => i);

            nodes[0].GetCache<int, int>(cacheName).PutAll(entries);

            foreach (var ignite in nodes)
                ignite.GetCache<int, int>(cacheName).GetAll(entries.Keys);

            for (var i = 0; i < NodesCnt; i++)
            {
                var cacheMetrics = nodes[i].GetCache<int, int>(cacheName).GetMetrics();

                var expCnt = filteredNodeIdxs.Contains(i) ? 3 : 9;

                Assert.AreEqual(expCnt, cacheMetrics.CacheGets, "Unexpected cache get count: nodeIdx=" + i);
            }
        }

        /// <summary>
        /// Tests that platform cache with custom .NET filter starts only on the specific nodes.
        /// Native cache is static, i.e. specified in .NET configuration.
        /// </summary>
        [TestCase(CacheMode.Partitioned)]
        [TestCase(CacheMode.Replicated)]
        public void TestDotnetFilter(CacheMode cacheMode)
        {
            DoTestStatic(
                i => GetConfiguration(i, GetCacheConfiguration(CacheName, cacheMode, 0, 3)),
                0, ClientIdx);
        }

        /// <summary>
        /// Tests that platform cache with custom Java filter starts only on the specific nodes.
        /// Native cache is static, i.e. specified in XML configuration.
        /// </summary>
        [TestCase(CacheMode.Partitioned)]
        [TestCase(CacheMode.Replicated)]
        public void TestJavaFilter(CacheMode cacheMode)
        {
            DoTestStatic(
                i => XmlConfiguration(i, WithFilterPref, cacheMode),
                2, ClientIdx);
        }

        /// <summary>
        /// Tests that platform cache starts on all nodes, when node filter is not configured in .NET configuration.
        /// </summary>
        [TestCase(CacheMode.Partitioned)]
        [TestCase(CacheMode.Replicated)]
        public void TestNoFilterDotnetConfig(CacheMode cacheMode)
        {
            DoTestStatic(
                i => GetConfiguration(i, GetCacheConfiguration(CacheName, cacheMode)),
                0, 1, 2, 3);
        }

        /// <summary>
        /// Tests that platform cache starts on all nodes, when node filter is not configured in XML configuration.
        /// </summary>
        [TestCase(CacheMode.Partitioned)]
        [TestCase(CacheMode.Replicated)]
        public void TestNoFilterJavaConfig(CacheMode cacheMode)
        {
            DoTestStatic(
                i => XmlConfiguration(i, NoFilterPref, cacheMode),
                0, 1, 2, 3);
        }
        
        /// <summary>
        /// Tests that platform cache with already implemented AttributeNodeFilter starts only on the specific
        /// nodes.
        /// Native cache is static, i.e. specified in .NET configuration.
        ///
        /// <see cref="AttributeNodeFilter"/>
        /// </summary>
        [TestCase(CacheMode.Partitioned)]
        [TestCase(CacheMode.Replicated)]
        public void TestAttributesNodeFilterDotnetConfig(CacheMode cacheMode)
        {
            var cacheCfg = GetCacheConfiguration(cacheMode, nodeFilter: _attributeNodeFilter);

            DoTestStatic(
                i => AddAttributes(GetConfiguration(i, cacheCfg), 2, ClientIdx),
                2, ClientIdx);
        }

        /// <summary>
        /// Tests that platform cache with already implemented AttributeNodeFilter starts only on the specific
        /// nodes.
        /// Native cache is static, i.e. specified in XML configuration.
        ///
        /// <see cref="AttributeNodeFilter"/>
        /// </summary>
        [TestCase(CacheMode.Partitioned)]
        [TestCase(CacheMode.Replicated)]
        public void TestAttributesNodeFilterXmlConfig(CacheMode cacheMode)
        {
            DoTestStatic(
                i => AddAttributes(XmlConfiguration(i, AttrFilterPref, cacheMode), 0, ClientIdx),
                0, ClientIdx);
        }

        private static void DoTestStatic(Func<int, IgniteConfiguration> cfgFunc, params int[] expIdxs)
        {
            var nodes = RunNodes(cfgFunc);

            CheckHasPlatformCache(nodes, expIdxs);
        }
        
        private static IIgnite[] RunNodes(Func<int, IgniteConfiguration> cfgFunc)
        {
            return Enumerable.Range(0, NodesCnt)
                .Select(i => Ignition.Start(cfgFunc.Invoke(i)))
                .ToArray();
        }

        private static void CheckHasPlatformCache(IIgnite[] nodes, int[] expIdxs, string cacheName = CacheName)
        {
            for (var i = 0; i < NodesCnt; i++)
            {
                var hasPlatformCache = nodes[i].GetCache<int, int>(cacheName).HasPlatformCache;

                Assert.AreEqual(expIdxs.Contains(i), hasPlatformCache,
                    $"Unexpected state: [nodeIdx={i}, hasPlatformCache={hasPlatformCache}]");
            }
        }

        private static IgniteConfiguration GetConfiguration(int idx, params CacheConfiguration[] cacheCfg)
        {
            return new IgniteConfiguration(TestUtils.GetTestConfiguration())
            {
                IgniteInstanceName = NamePref + idx,
                ClientMode = idx == ClientIdx,
                CacheConfiguration = cacheCfg
            };
        }

        private static CacheConfiguration GetCacheConfiguration(string cacheName, CacheMode cacheMode,
            params int[] filteredNodeIdxs)
        {
            return GetCacheConfiguration(
                cacheMode,
                cacheName,
                filteredNodeIdxs.Length == 0 ? null : new NodeNameFilter(filteredNodeIdxs));
        }

        private static CacheConfiguration GetCacheConfiguration(CacheMode cacheMode, string cacheName = CacheName,
            IClusterNodeFilter nodeFilter = null)
        {
            return new CacheConfiguration(cacheName)
            {
                CacheMode = cacheMode,
                Backups = 1,
                PlatformCacheConfiguration = new PlatformCacheConfiguration
                {
                    NodeFilter =  nodeFilter
                },
                EnableStatistics = true
            };
        }

        private static IgniteConfiguration XmlConfiguration(int idx, string cfgUrlPref, CacheMode cacheMode)
        {
            var suf = (cacheMode == CacheMode.Replicated ? "-replicated" : "-partitioned") + ".xml";

            var cfgUrl = Path.Combine("Config", "Cache", "Platform", cfgUrlPref + suf);

            return new IgniteConfiguration(TestUtils.GetTestConfiguration())
            {
                IgniteInstanceName = NamePref + idx,
                SpringConfigUrl = cfgUrl,
                ClientMode = idx == ClientIdx
            };
        }

        private static IgniteConfiguration AddAttributes(IgniteConfiguration cfg, params int[] filteredIdxs)
        {
            var filteredNames = filteredIdxs
                .Select(i => NamePref + i);

            return new IgniteConfiguration(cfg)
            {
                UserAttributes = new Dictionary<string, object>
                {
                    {
                        TestAttr,
                        filteredNames.Contains(cfg.IgniteInstanceName) ? FilteredVal : NonFilteredVal
                    }
                }
            };
        }
    }

    /// <summary>
    /// .NET test node filter.
    /// </summary>
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
