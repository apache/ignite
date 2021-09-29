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
    using Apache.Ignite.Core.Services;
    using NUnit.Framework;

    public class ServiceProxyContextTest
    {
        /** */
        private const string SvcName = "Service1";

        /** */
        private IIgnite Grid1;

        /** */
        private IIgnite Grid2;

        /** */
        private IIgnite _client;

        /** */
        private IIgnite[] Grids;

        [TearDown]
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
        /// Starts the grids.
        /// </summary>
        private void StartGrids()
        {
//            if (Grid1 != null)
//                return;

            Grid1 = Ignition.Start(new IgniteConfiguration(TestUtils.GetTestConfiguration(false, "grid1")));
            Grid2 = Ignition.Start(new IgniteConfiguration(TestUtils.GetTestConfiguration(false, "grid2")));

            var cfg = new IgniteConfiguration(TestUtils.GetTestConfiguration(false, "client"));

            cfg.ClientMode = true;
            cfg.IgniteInstanceName = "client";

            _client = Ignition.Start(cfg);

            Grids = new[] { Grid1, Grid2, _client };
        }

        /// <summary>
        /// Stops the grids.
        /// </summary>
        private void StopGrids()
        {
//            Grid1 = Grid2 = Grid3 = null;
//            Grids = null;

            Ignition.StopAll(true);
        }
        
        
        /// <summary>
        /// Tests proxy custom invocation context.
        /// </summary>
        [Test]
        public void TestProxyContext()
        {
            // todo test cases
            CheckProxyContext(Grid1, false, false, false);
            CheckProxyContext(Grid1, false, false, true);
            CheckProxyContext(Grid1, false, true, false);
            CheckProxyContext(Grid1, false, true, true);
            CheckProxyContext(Grid1, true, false, false);
            CheckProxyContext(Grid1, true, false, true);
            CheckProxyContext(Grid1, true, true, false);
            CheckProxyContext(Grid1, true, true, true);


            CheckProxyContext(Grid2, false, false, false);
            CheckProxyContext(Grid2, false, false, true);
            CheckProxyContext(Grid2, false, true, false);
            CheckProxyContext(Grid2, false, true, true);
            CheckProxyContext(Grid2, true, false, false);
            CheckProxyContext(Grid2, true, false, true);
            CheckProxyContext(Grid2, true, true, false);
            CheckProxyContext(Grid2, true, true, true);

            CheckProxyContext(_client, false, false, false);
            CheckProxyContext(_client, false, false, true);
            CheckProxyContext(_client, false, true, false);
            CheckProxyContext(_client, false, true, true);
            CheckProxyContext(_client, true, false, false);
            CheckProxyContext(_client, true, false, true);
            CheckProxyContext(_client, true, true, false);
            CheckProxyContext(_client, true, true, true);
        }

        private void CheckProxyContext(IIgnite ignite, bool nodeSingleton, bool sticky, bool dynamic)
        {
            if (nodeSingleton)
                ignite.GetServices().DeployNodeSingleton(SvcName, new MyService());
            else
                ignite.GetServices().DeployClusterSingleton(SvcName, new MyService());

            try {
                foreach (var grid in Grids)
                {
                    var svcs = grid.GetServices();

                    if (dynamic)
                    {
                        var proxy0 = svcs.GetDynamicServiceProxy(SvcName, sticky, new Dictionary<string, object> {{"id", 123}});
                        var proxy1 = svcs.GetDynamicServiceProxy(SvcName, sticky, new Dictionary<string, object> {{"id", 12345}});

                        Assert.AreEqual(123, proxy0.Method("id"), "Node=" + grid.Name);
                        Assert.AreEqual(12345, proxy1.Method("id"), "Node=" + grid.Name);
                    }
                    else {
                        var proxy0 = svcs.GetServiceProxy<IMyService>(SvcName, sticky, new Dictionary<string, object> {{"id", 123}});
                        var proxy1 = svcs.GetServiceProxy<IMyService>(SvcName, sticky, new Dictionary<string, object> {{"id", 12345}});

                        Assert.AreEqual(123, proxy0.Method("id"), "Node=" + grid.Name);
                        Assert.AreEqual(12345, proxy1.Method("id"), "Node=" + grid.Name);
                    }
                }
            }
            finally
            {
                ignite.GetServices().Cancel(SvcName);
            }
        }

        public interface IMyService : IService
        {
            object Method(string arg);
        }
        
        [Serializable]
        public class MyService : IMyService
        {
            public object Method(string arg)
            {
                return context.Attribute(arg);
            }

            private IServiceContext context;

            public void Init(IServiceContext context)
            {
                this.context = context;
            }

            public void Execute(IServiceContext context)
            {
                // No-op.
            }

            public void Cancel(IServiceContext context)
            {
                // No-op.
            }
        }
    }
}