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
        private const string CacheName = "cache";

        /** */
        private const int NodesCnt = 4;

        /** */
        private const int ClientIdx = NodesCnt - 1;

        /** */
        private const string NodeFromJavaFilterName = "NodeFromJavaFilter";
        
        /** */
        private const string WithFilterPref = "with-node-filter-";
        
        /** */
        private const string NoFilterPref = "no-node-filter-";
        
        /** Tests configuration */
        public static object[][] TestCases =
        {
            new object[] { false, CacheMode.Partitioned },
            new object[] { false, CacheMode.Replicated },
            new object[] { true, CacheMode.Partitioned },
            new object[] { true, CacheMode.Replicated }
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
        /// Tests .NET filter for platform cache.
        /// Platform cache should be started only on the specific nodes.
        /// Native cache is started from server nodes.
        /// </summary>
        [TestCaseSource(nameof(TestCases))]
        public void TestHasPlatformCacheFromServer(bool startFromFilteredNode, CacheMode cacheMode)
        {
            var filteredSrvNodeIdx = new Random().Next(0, ClientIdx);

            var nonFilteredServNodeIdx = Enumerable.Range(0, ClientIdx)
                .First(i => i != filteredSrvNodeIdx);

            var cacheStartIdx = startFromFilteredNode ? filteredSrvNodeIdx : nonFilteredServNodeIdx;

            DoTestWithFilter(cacheMode, filteredSrvNodeIdx, cacheStartIdx);
        }

        /// <summary>
        /// Tests .NET filter for platform cache.
        /// Platform cache should be started only on the specific nodes.
        /// Native cache is started from client node.
        /// </summary>
        [TestCaseSource(nameof(TestCases))]
        public void TestHasPlatformCacheStartFromClient(bool startFromFilteredNode, CacheMode cacheMode)
        {
            var filteredNodeIdx = ClientIdx;

            var cacheStartIdx = startFromFilteredNode ? filteredNodeIdx : 0;

            DoTestWithFilter(cacheMode, filteredNodeIdx, cacheStartIdx);
        }

        /// <summary>
        /// Tests Java filter for platform cache.
        /// Platform cache should be started only on the specific nodes.
        /// Native cache is started during nodes startup.
        /// </summary>
        [TestCase(CacheMode.Partitioned)]
        [TestCase(CacheMode.Replicated)]
        public void TestHasPlatformCacheJavaConfig(CacheMode cacheMode)
        {
            DoTest(
                XmlConfig(WithFilterPref, cacheMode), 
                cacheMode, 
                2, 3);
        }

        /// <summary>
        /// Tests that platform cache starts on all nodes, when node filter is not configured.
        /// </summary>
        [TestCase(CacheMode.Partitioned)]
        [TestCase(CacheMode.Replicated)]
        public void TestNoFilterDotnetConfig(CacheMode cacheMode)
        {
            DoTest(
                DotnetConfig(cacheMode), 
                cacheMode, 
                0, 1, 2, 3);
        }

        /// <summary>
        /// Tests that platform cache starts on all nodes, when node filter is not configured.
        /// </summary>
        [TestCase(CacheMode.Partitioned)]
        [TestCase(CacheMode.Replicated)]
        public void TestNoFilterJavaConfig(CacheMode cacheMode)
        {
            DoTest(
                XmlConfig(NoFilterPref, cacheMode), 
                cacheMode, 
                0, 1, 2, 3);
        }

        private void DoTest(Func<int, IgniteConfiguration> cfgFunc, CacheMode cacheMode, params int[] expIdxs)
        {
            var nodes = RunNodes(cfgFunc);

            for (var i = 0; i < NodesCnt; i++)
            {
                var hasCache = nodes[i].GetCache<int, int>(CacheName).HasPlatformCache;
                
                Assert.IsTrue(hasCache == expIdxs.Contains(i), $"Unexpected state: [nodeIdx={i}, hasCache={hasCache}]");
            }

        }

        private Func<int, IgniteConfiguration> DotnetConfig(CacheMode cacheMode, params string[] filteredNodeNames)
        {
            return i => GetConfiguration(i, GetCacheConfiguration(cacheMode, filteredNodeNames));
        }

        private Func<int, IgniteConfiguration> XmlConfig(string pref, CacheMode cacheMode)
        {
            return i => GetConfigurationFromXml(i, pref, cacheMode);
        }

        private void DoTestWithFilter(CacheMode cacheMode, int filteredNodeIdx, int cacheStartIdx)
        {
            var nodes = RunNodes(i => GetConfiguration(i));

            var filteredNodeName = nodes[filteredNodeIdx].Name;

            nodes[cacheStartIdx].CreateCache<int, int>(GetCacheConfiguration(cacheMode, filteredNodeName));

            for (var i = 0; i < NodesCnt; i++)
            {
                bool hasPlatformCache = nodes[i].GetCache<int, int>(CacheName).HasPlatformCache;

                Assert.IsTrue(hasPlatformCache == filteredNodeName.Equals(nodes[i].Name));
            }
        }

        private IIgnite[] RunNodes(Func<int, IgniteConfiguration> cfgFunc)
        {
            return Enumerable.Range(0, NodesCnt)
                .Select(i => Ignition.Start(cfgFunc.Invoke(i)))
                .ToArray();
        }

        private IgniteConfiguration GetConfiguration(int idx, params CacheConfiguration[] cacheCfg)
        {
            var name = "node_" + idx;

            var igniteConfig = new IgniteConfiguration(TestUtils.GetTestConfiguration())
            {
                IgniteInstanceName = name,
                ClientMode = idx == ClientIdx,
                CacheConfiguration = cacheCfg
            };

            return igniteConfig;
        }

        private IgniteConfiguration GetConfigurationFromXml(int idx, string cfgUrlPref, CacheMode cacheMode)
        {
            return GetConfigurationFromXml("node_" + idx, cfgUrlPref, idx == ClientIdx, cacheMode);
        }

        private IgniteConfiguration GetConfigurationFromXml(string nodeName, string cfgUrlPref, bool clientMode, 
            CacheMode cacheMode)
        {
            return new IgniteConfiguration(TestUtils.GetTestConfiguration())
            {
                IgniteInstanceName = nodeName,
                SpringConfigUrl = ConfigUrl(cfgUrlPref, cacheMode),
                ClientMode = clientMode
            };
        }

        private CacheConfiguration GetCacheConfiguration(CacheMode cacheMode, params string[] filteredNodeNames)
        {
            return new CacheConfiguration(CacheName)
            {
                CacheMode = cacheMode,
                Backups = 1,
                PlatformCacheConfiguration = new PlatformCacheConfiguration
                {
                    NodeFilter = filteredNodeNames.Length == 0 ? null : new NodeNameFilter(filteredNodeNames)
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

        public NodeNameFilter(string[] names)
        {
            _names = names;
        }

        public bool Invoke(IClusterNode node)
        {
            return _names.Contains(node.GetAttribute<String>("org.apache.ignite.ignite.name"));
        }
    }
}