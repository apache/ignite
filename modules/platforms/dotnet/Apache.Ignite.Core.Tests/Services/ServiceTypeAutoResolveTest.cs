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
    using System.IO;
    using NUnit.Framework;
    using org.apache.ignite.platform;

    /// <summary>
    /// Tests checks ability to execute service method without explicit registration of parameter type.
    /// </summary>
    public class ServicesTypeAutoResolveTest
    {
        /** */
        private IIgnite _grid1;

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
                _grid1.GetServices();

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
        /// Tests Java service invocation with dynamic proxy.
        /// Address type should be resolved implicitly.
        /// </summary>
        [Test]
        public void TestTypeAutoResolvingDynamicProxy() 
        {
            // Deploy Java service
            var javaSvcName = TestUtils.DeployJavaService(_grid1);
            var svc = _grid1.GetServices().GetDynamicServiceProxy(javaSvcName, true);

            Assert.IsNull(svc.testAddress(null));

            Address addr = svc.testAddress(new Address {Zip = "000", Addr = "Moscow"});

            Assert.AreEqual("127000", addr.Zip);
            Assert.AreEqual("Moscow Akademika Koroleva 12", addr.Addr);
        }

        /// <summary>
        /// Tests Java service invocation.
        /// Address type should be resolved implicitly.
        /// </summary>
        [Test]
        public void TestCallJavaService()
        {
            // Deploy Java service
            var javaSvcName = TestUtils.DeployJavaService(_grid1);
            
            var svc = _grid1.GetServices().GetServiceProxy<IJavaService>(javaSvcName, false);
            
            Assert.IsNull(svc.testAddress(null));

            Address addr = svc.testAddress(new Address {Zip = "000", Addr = "Moscow"});

            Assert.AreEqual("127000", addr.Zip);
            Assert.AreEqual("Moscow Akademika Koroleva 12", addr.Addr);

            _grid1.GetServices().Cancel(javaSvcName);
        }

        /// <summary>
        /// Starts the grids.
        /// </summary>
        private void StartGrids()
        {
            if (_grid1 != null)
                return;

            var path = Path.Combine("Config", "Compute", "compute-grid");
            _grid1 = Ignition.Start(GetConfiguration(path + "1.xml"));
        }

        /// <summary>
        /// Stops the grids.
        /// </summary>
        private void StopGrids()
        {
            _grid1 = null;

            Ignition.StopAll(true);
        }
        
        /// <summary>
        /// Gets the Ignite configuration.
        /// </summary>
        private IgniteConfiguration GetConfiguration(string springConfigUrl)
        {
            springConfigUrl = Compute.ComputeApiTestFullFooter.ReplaceFooterSetting(springConfigUrl);

            return new IgniteConfiguration(TestUtils.GetTestConfiguration())
            {
                SpringConfigUrl = springConfigUrl
            };
        }
    }
}
