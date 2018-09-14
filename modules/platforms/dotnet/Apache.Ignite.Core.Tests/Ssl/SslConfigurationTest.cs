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

namespace Apache.Ignite.Core.Tests.Ssl 
{
    using System.Linq;
    using Apache.Ignite.Core.Ssl;
    using Apache.Ignite.Core.Common;
    using NUnit.Framework;

    /// <summary>
    /// SSL configuration tests.
    /// </summary>
    [Category(TestUtils.CategoryIntensive)]
    public class SslConfigurationTest
    {
        /** Test Password. */
        private const string Password = "123456";

        /** Key Store file. */
        private const string KeyStoreFilePath = @"Config/KeyStore/server.jks";

        /** Trust Store file. */
        private const string TrustStoreFilePath = @"Config/KeyStore/trust.jks";

        /// <summary>
        /// Test teardown.
        /// </summary>
        [TearDown]
        public void TearDown()
        {
            Ignition.StopAll(true);
        }

        /// <summary>
        /// Returns SSL Context factory for tests.
        /// </summary>
        private static SslContextFactory GetSslContextFactory()
        {
            return new SslContextFactory(KeyStoreFilePath, Password, TrustStoreFilePath, Password);
        }

        /// <summary>
        /// Tests Node Start with SslContextFactory
        /// </summary>
        [Test]
        public void TestStart([Values(null, TrustStoreFilePath)] string trustStoreFilePath)
        {
            var factory = GetSslContextFactory();
            factory.TrustStoreFilePath = trustStoreFilePath;

            var cfg = new IgniteConfiguration(TestUtils.GetTestConfiguration())
            {
                SslContextFactory = factory
            };

            var grid = Ignition.Start(cfg);

            Assert.IsNotNull(grid);

            Assert.AreEqual(1, grid.GetCluster().GetNodes().Count);

            var cfgFactory = grid.GetConfiguration().SslContextFactory;

            Assert.True(cfgFactory is SslContextFactory);
            var sslContextFactory = (SslContextFactory)cfgFactory;
            
            Assert.AreEqual(Password, sslContextFactory.KeyStorePassword);
            Assert.AreEqual(Password, sslContextFactory.TrustStorePassword);
            Assert.AreEqual(KeyStoreFilePath, sslContextFactory.KeyStoreFilePath);
            Assert.AreEqual(trustStoreFilePath, sslContextFactory.TrustStoreFilePath);
        }

        /// <summary>
        /// Tests IgniteException when SSL configuration
        /// </summary>
        [Test]
        public void TestConfigurationExceptions()
        {
            var cfg = new IgniteConfiguration(TestUtils.GetTestConfiguration())
            {
                SslContextFactory = new SslContextFactory(@"WrongPath/server.jks", Password, 
                                                          TrustStoreFilePath, Password)
            };

            var ex = Assert.Throws<IgniteException>(() => Ignition.Start(cfg));
            Assert.True(ex.Message.StartsWith(@"Failed to initialize key store (key store file was not found): " + 
                @"[path=WrongPath/server.jks"));

            cfg.SslContextFactory = new SslContextFactory(KeyStoreFilePath, Password,
                                                          @"WrongPath/trust.jks", Password);

            ex = Assert.Throws<IgniteException>(() => Ignition.Start(cfg));
            Assert.True(ex.Message.StartsWith(@"Failed to initialize key store (key store file was not found): " + 
                @"[path=WrongPath/trust.jks"));

            cfg.SslContextFactory = new SslContextFactory(KeyStoreFilePath, "654321",
                                                          TrustStoreFilePath, Password);

            ex = Assert.Throws<IgniteException>(() => Ignition.Start(cfg));
            Assert.AreEqual(@"Failed to initialize key store (I/O error occurred): Config/KeyStore/server.jks",
                ex.Message);

            cfg.SslContextFactory = new SslContextFactory(KeyStoreFilePath, Password,
                                                          TrustStoreFilePath, "654321");

            ex = Assert.Throws<IgniteException>(() => Ignition.Start(cfg));
            Assert.AreEqual(@"Failed to initialize key store (I/O error occurred): Config/KeyStore/trust.jks",
                ex.Message);
        }

        /// <summary>
        /// Tests Node Start with SslContextFactory from Spring xml.
        /// </summary>
        [Test]
        public void TestStartWithConfigPath()
        {
            var cfg = new IgniteConfiguration(TestUtils.GetTestConfiguration())
            {
                SpringConfigUrl = @"Config/ssl.xml",
            };

            var grid = Ignition.Start(cfg);

            Assert.IsNotNull(grid);

            Assert.AreEqual(1, grid.GetCluster().GetNodes().Count);

            var factory = grid.GetConfiguration().SslContextFactory;

            Assert.True(factory is SslContextFactory);
            var sslContextFactory = (SslContextFactory)factory;

            Assert.AreEqual(Password, sslContextFactory.KeyStorePassword);
            Assert.AreEqual(Password, sslContextFactory.TrustStorePassword);
            Assert.AreEqual(KeyStoreFilePath, sslContextFactory.KeyStoreFilePath);
            Assert.AreEqual(TrustStoreFilePath, sslContextFactory.TrustStoreFilePath);
        }

        /// <summary>
        /// Simple test with 2 SSL nodes.
        /// </summary>
        [Test]
        public void TestTwoServers()
        {
            var cfg1 = new IgniteConfiguration(TestUtils.GetTestConfiguration())
            {
                SpringConfigUrl = @"Config/ssl.xml"
            };

            var cfg2 = new IgniteConfiguration(TestUtils.GetTestConfiguration(name: "grid2"))
            {
                SslContextFactory = GetSslContextFactory()
            };

            var grid1 = Ignition.Start(cfg1);

            Assert.AreEqual("grid1", grid1.Name);
            Assert.AreSame(grid1, Ignition.GetIgnite());
            Assert.AreSame(grid1, Ignition.GetAll().Single());

            var grid2 = Ignition.Start(cfg2);

            Assert.AreEqual("grid2", grid2.Name);
            Assert.Throws<IgniteException>(() => Ignition.GetIgnite());

            Assert.AreSame(grid1, Ignition.GetIgnite("grid1"));
            Assert.AreSame(grid1, Ignition.TryGetIgnite("grid1"));

            Assert.AreSame(grid2, Ignition.GetIgnite("grid2"));
            Assert.AreSame(grid2, Ignition.TryGetIgnite("grid2"));

            Assert.AreEqual(new[] {grid1, grid2}, Ignition.GetAll().OrderBy(x => x.Name).ToArray());

            Assert.AreEqual(2, grid1.GetCluster().GetNodes().Count);
            Assert.AreEqual(2, grid2.GetCluster().GetNodes().Count);
        }

        /// <summary>
        /// Simple test with 1 SSL node and 1 no-SSL node.
        /// </summary>
        [Test]
        public void TestSslConfigurationMismatch()
        {
            var cfg = new IgniteConfiguration(TestUtils.GetTestConfiguration(name: "grid1"));

            var sslCfg = new IgniteConfiguration(TestUtils.GetTestConfiguration(name: "grid2"))
            {
                SslContextFactory = GetSslContextFactory()
            };

            Ignition.Start(cfg);

            var ex = Assert.Throws<IgniteException>(() => Ignition.Start(sslCfg));
            Assert.True(ex.Message.StartsWith(@"Unable to establish secure connection. " + 
                                              @"Was remote cluster configured with SSL?"));

            Ignition.StopAll(true);

            Ignition.Start(sslCfg);

            ex = Assert.Throws<IgniteException>(() => Ignition.Start(cfg));
            Assert.True(ex.Message.StartsWith(@"Unable to establish secure connection. " +
                                              @"Was remote cluster configured with SSL?"));
        }

        /// <summary>
        /// Tests the client-server mode.
        /// </summary>
        [Test]
        public void TestClientServer()
        {
            var factory = GetSslContextFactory();

            var servCfg = new IgniteConfiguration(TestUtils.GetTestConfiguration(name: "grid1"))
            {
                SslContextFactory = factory
            };

            var clientCfg = new IgniteConfiguration(TestUtils.GetTestConfiguration(name: "grid2"))
            {
                SslContextFactory = factory,
                ClientMode = true
            };

            using (var serv = Ignition.Start(servCfg)) // start server-mode ignite first
            {
                Assert.IsFalse(serv.GetCluster().GetLocalNode().IsClient);

                using (var grid = Ignition.Start(clientCfg))
                {
                    Assert.IsTrue(grid.GetCluster().GetLocalNode().IsClient);

                    Assert.AreEqual(2, grid.GetCluster().GetNodes().Count);
                    Assert.AreEqual(2, serv.GetCluster().GetNodes().Count);

                    Assert.AreEqual(1, grid.GetCluster().ForServers().GetNodes().Count);
                    Assert.AreEqual(1, serv.GetCluster().ForServers().GetNodes().Count);
                }
            }
        }
    }
}
