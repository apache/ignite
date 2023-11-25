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

namespace Apache.Ignite.Core.Tests.Services
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Net;
    using Apache.Ignite.Core.Client;
    using Apache.Ignite.Core.Cluster;
    using Apache.Ignite.Core.Log;
    using Apache.Ignite.Core.Services;
    using Apache.Ignite.Core.Tests.Client.Cache;
    using NUnit.Framework;

    /// <summary>
    /// Services tests.
    /// </summary>
#pragma warning disable CS0618 // Method is obsolete.
    public class ServicesAwarenessTest
    {
        /** */
        private const string SvcName = "Service1";

        /** */
        private const string PlatformSvcName = "PlatformTestService";

        // /** Service method call counter. */
        // private static int _serviceCallCounter;

        /** */
        protected IIgnite Grid1;

        /** */
        protected IIgnite Grid2;

        /** */
        protected IIgnite Grid3;


        /** */
        private IIgniteClient _thinClient;

        /** */
        protected IIgnite[] Grids;

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

            _thinClient = Ignition.StartClient(GetClientConfiguration());

            var cfg = new ServiceConfiguration
            {
                Name = PlatformSvcName,
                MaxPerNodeCount = 1,
                TotalCount = 1,
                NodeFilter = new NodeIdFilter {NodeId = Grid1.GetCluster().GetLocalNode().Id},
                Service = new PlatformTestService(),
                Interceptors = new List<IServiceCallInterceptor>
                    {new PlatformTestServiceInterceptor("testInterception")}
            };

            Services.Deploy(cfg);

            TestUtils.DeployJavaService(Grid1, Grids.Select(n=>n.GetCluster().GetLocalNode().Id).ToArray());
        }

        /// <summary>
        /// Executes after each test.
        /// </summary>
        [TearDown]
        public void TearDown()
        {
            try
            {
                Services.CancelAll();
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
            if (Grid1 != null)
                return;
            
            var path = Path.Combine("Config", "Compute", "compute-grid");
            Grid1 = Ignition.Start(GetConfiguration(path + "1.xml"));
            Grid2 = Ignition.Start(GetConfiguration(path + "2.xml"));
            Grid3 = Ignition.Start(GetConfiguration(path + "3.xml"));

            Grids = new[] {Grid1, Grid2, Grid3};
        }

        /// <summary>
        /// Stops the grids.
        /// </summary>
        private void StopGrids()
        {
            Grid1 = Grid2 = Grid3 = null;
            Grids = null;

            Ignition.StopAll(true);
        }

        /// <summary>
        /// Gets the Ignite configuration.
        /// </summary>
        private IgniteConfiguration GetConfiguration(string springConfigUrl)
        {
            return new IgniteConfiguration(TestUtils.GetTestConfiguration())
            {
                SpringConfigUrl = springConfigUrl,
                LifecycleHandlers = null
            };
        }

        /// <summary>
        /// Gets the client configuration.
        /// </summary>
        private IgniteClientConfiguration GetClientConfiguration()
        {
            var port = IgniteClientConfiguration.DefaultPort;

            return new IgniteClientConfiguration
            {
                Endpoints = new List<string> {IPAddress.Loopback + ":" + port},
                SocketTimeout = TimeSpan.FromSeconds(15),
                Logger = new ListLogger(new ConsoleLogger {MinLevel = LogLevel.Trace})
            };
        }

        /// <summary>
        /// Gets the services.
        /// </summary>
        protected virtual IServices Services
        {
            get { return Grid1.GetServices(); }
        }

        /// <summary>
        /// Makes Service1-{i} names for services.
        /// </summary>
        private static string MakeServiceName(int i)
        {
            // Please note that CheckContext() validates Name.StartsWith(SvcName).
            return string.Format("{0}-{1}", SvcName, i);
        }

        /// <summary>
        /// Tests Java service invocation via thin client.
        /// </summary>
        [Test]
        public void TestCallJavaServiceThinClient()
        {
            var svc = _thinClient.GetServices().GetServiceProxy<IJavaService>(TestUtils.JavaServiceName);

            Assert.AreEqual(4, svc.test((byte) 3));
        }
        
        /// <summary>
        /// Test node filter.
        /// </summary>
        [Serializable]
        private class NodeIdFilter : IClusterNodeFilter
        {
            /// <summary>
            /// Gets or sets the node identifier.
            /// </summary>
            public Guid NodeId { get; set; }

            /** <inheritdoc /> */
            public bool Invoke(IClusterNode node)
            {
                return node.Id == NodeId;
            }
        }
    }
}
