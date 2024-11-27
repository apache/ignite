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
        private const string WithNodeFilterPref = "with-node-filter-";
        
        /** */
        private const string NoNodeFilterPref = "no-node-filter-";

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
        [Test]
        public void TestHasPlatformCacheJavaConfig(
            [Values(true, false)] bool clientMode,
            [Values(CacheMode.Partitioned, CacheMode.Replicated)]
            CacheMode cacheMode)
        {
            var xmlCfg = XmlConfig(WithNodeFilterPref, cacheMode);

            var nonFilteredNode = Ignition.Start(GetConfigurationFromXml(xmlCfg, "NonFilteredNode", false));
            var javaFilteredNode = Ignition.Start(GetConfigurationFromXml(xmlCfg, NodeFromJavaFilterName, clientMode));

            Assert.IsFalse(nonFilteredNode.GetCache<int, int>(CacheName).HasPlatformCache);
            Assert.IsTrue(javaFilteredNode.GetCache<int, int>(CacheName).HasPlatformCache);
        }

        /// <summary>
        /// Tests that platform cache starts on all nodes, when node filter is not configured.
        /// </summary>
        [TestCase(CacheMode.Partitioned)]
        [TestCase(CacheMode.Replicated)]
        public void TestHasPlatformCacheNoFilterDotnet(CacheMode cacheMode)
        {
            DoTestNoFilter(
                i => Ignition.Start(GetConfiguration(i)),
                ign => CreateCache(ign, cacheMode, null));
        }

        /// <summary>
        /// Tests that platform cache starts on all nodes, when node filter is not configured.
        /// </summary>
        [TestCase(CacheMode.Partitioned)]
        [TestCase(CacheMode.Replicated)]
        public void TestHasPlatformCacheNoFilterJava(CacheMode cacheMode)
        {
            DoTestNoFilter(
                i => Ignition.Start(
                    GetConfigurationFromXml(XmlConfig(NoNodeFilterPref, cacheMode), i)),
                null);
        }

        private void DoTestWithFilter(CacheMode cacheMode, int filteredNodeIdx, int cacheStartIdx)
        {
            var nodes = Enumerable.Range(0, NodesCnt)
                .Select(i => Ignition.Start(GetConfiguration(i)))
                .ToArray();

            var filteredNodeName = nodes[filteredNodeIdx].Name;

            CreateCache(nodes[cacheStartIdx], cacheMode, filteredNodeName);

            for (var i = 0; i < NodesCnt; i++)
            {
                bool hasPlatformCache = nodes[i].GetCache<int, int>(CacheName).HasPlatformCache;

                Assert.IsTrue(hasPlatformCache == filteredNodeName.Equals(nodes[i].Name));
            }
        }

        private void DoTestNoFilter(Func<int, IIgnite> ignSup, Action<IIgnite> cacheCreator)
        {
            var nodes = Enumerable.Range(0, NodesCnt)
                .Select(ignSup)
                .ToArray();

            if (cacheCreator != null)
                cacheCreator.Invoke(nodes[0]);

            foreach (var ignite in nodes)
                Assert.IsTrue(ignite.GetCache<int, int>(CacheName).HasPlatformCache);
        }

        private IgniteConfiguration GetConfiguration(int idx)
        {
            var name = "node_" + idx;

            var igniteConfig = new IgniteConfiguration(TestUtils.GetTestConfiguration())
            {
                ClientMode = idx == ClientIdx,
                IgniteInstanceName = name
            };

            return igniteConfig;
        }

        private IgniteConfiguration GetConfigurationFromXml(string cfgUrl, int idx)
        {
            return GetConfigurationFromXml(cfgUrl, "node_" + idx, idx == ClientIdx);
        }

        private IgniteConfiguration GetConfigurationFromXml(string cfgUrl, string nodeName, bool clientMode)
        {
            return new IgniteConfiguration(TestUtils.GetTestConfiguration())
            {
                IgniteInstanceName = nodeName,
                SpringConfigUrl = cfgUrl,
                ClientMode = clientMode
            };
        }

        private void CreateCache(IIgnite ignite, CacheMode cacheMode, string filteredNodeName)
        {
            var cacheConfig = new CacheConfiguration(CacheName)
            {
                CacheMode = cacheMode,
                Backups = 1,
                PlatformCacheConfiguration = new PlatformCacheConfiguration
                {
                    NodeFilter = filteredNodeName is null ? null : new NodeNameFilter(filteredNodeName)
                }
            };

            ignite.CreateCache<int, int>(cacheConfig);
        }

        private string XmlConfig(string filePref, CacheMode cacheMode)
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
        private readonly string _name;

        public NodeNameFilter(string name)
        {
            _name = name;
        }

        public bool Invoke(IClusterNode node)
        {
            return _name.Equals(node.GetAttribute<String>("org.apache.ignite.ignite.name"));
        }
    }
}