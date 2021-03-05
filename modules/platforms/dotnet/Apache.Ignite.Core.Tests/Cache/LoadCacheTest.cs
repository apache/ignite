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

namespace Apache.Ignite.Core.Tests.Cache
{
    using System;
    using System.IO;
    using System.Reflection;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Cache.Configuration;
    using Apache.Ignite.Platform.Model;
    using NUnit.Framework;

    /// <summary>
    /// Tests checks ability to execute loadCache method without explicit registration of binary type.
    /// </summary>
    public class LoadCacheTest
    {
        /** */
        private IIgnite _grid1;
        
        /** */
        private IIgnite _client;

        [TestFixtureTearDown]
        public void FixtureTearDown()
        {
            StopGrids();
        }

        /// <summary>
        /// Executes before each test.
        /// </summary>
        [SetUp]
        public void SetUp()
        {
            StartGrids();
        }

        /// <summary>
        /// Executes after each test.
        /// </summary>
        [TearDown]
        public void TearDown()
        {
            try
            {
                TestUtils.AssertHandleRegistryIsEmpty(1000, _grid1);
            }
            catch (Exception)
            {
                // Restart grids to cleanup
                StopGrids();

                throw;
            }
            finally
            {
                if (TestContext.CurrentContext.Test.Name.StartsWith("TestEventTypes"))
                    StopGrids(); // clean events for other tests
            }
        }

        /// <summary>
        /// Starts the grids.
        /// </summary>
        private void StartGrids()
        {
            if (_grid1 != null)
                return;

            var path = Path.Combine("Config", "Cache", "load-cache-config.xml");

            _grid1 = Ignition.Start(GetConfiguration(path));

            var clientWorkDir = Path.Combine(Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location), 
                "client_work");

            _client = Ignition.Start(new IgniteConfiguration(TestUtils.GetTestConfiguration())
            {
                WorkDirectory = clientWorkDir,
                ClientMode = true,
                IgniteInstanceName = "client",
                BinaryConfiguration = new BinaryConfiguration
                {
                    NameMapper = new BinaryBasicNameMapper {NamespaceToLower = true}
                }
            });
        }

        /// <summary>
        /// Gets the Ignite configuration.
        /// </summary>
        private IgniteConfiguration GetConfiguration(string springConfigUrl)
        {
            springConfigUrl = ReplaceFooterSetting(springConfigUrl);

            return new IgniteConfiguration(TestUtils.GetTestConfiguration())
            {
                IgniteInstanceName = "server",
                SpringConfigUrl = springConfigUrl,
                BinaryConfiguration = new BinaryConfiguration
                {
                    NameMapper = new BinaryBasicNameMapper {NamespaceToLower = true}
                }
            };
        }

        /// <summary>
        /// Replaces the footer setting.
        /// </summary>
        internal static string ReplaceFooterSetting(string path)
        {
            var text = File.ReadAllText(path).Replace(
                "property name=\"compactFooter\" value=\"true\"",
                "property name=\"compactFooter\" value=\"false\"");

            path += "_fullFooter";

            File.WriteAllText(path, text);

            Assert.IsTrue(File.Exists(path));

            return path;
        }

        /// <summary>
        /// Stops the grids.
        /// </summary>
        private void StopGrids()
        {
            _grid1 = null;

            Ignition.StopAll(true);
        }

        [Test]
        public void LoadCacheOnServer()
        {
            var cache = _grid1.GetCache<int, V17>("V17");

            cache.LoadCache(null);

            var v = cache.LocalPeek(1, CachePeekMode.Platform);

            Assert.AreEqual(1, v.Id);
            Assert.AreEqual("v1", v.Name);
        }

        [Test]
        public void LoadCacheOnClient()
        {
            var cache = _client.GetOrCreateNearCache<int, V18>(
                "V18",
                new NearCacheConfiguration(),
                new PlatformCacheConfiguration
                {
                    KeyTypeName = typeof(int).FullName,
                    ValueTypeName = typeof(V18).FullName
                }
            );

            cache.LoadCache(null);

            var v = cache.Get(1);

            Assert.AreEqual(1, v.Id);
            Assert.AreEqual("v1", v.Name);
        }
    }
}
