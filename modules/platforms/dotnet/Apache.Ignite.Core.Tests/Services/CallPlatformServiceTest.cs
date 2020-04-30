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
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Resource;
    using Apache.Ignite.Core.Services;
    using NUnit.Framework;

    /// <summary>
    /// Tests calling platform service from java.
    /// </summary>
    public class CallPlatformServiceTest
    {
        /** */
        private const string ServiceName = "TestPlatformService";

        /** */
        private const string CheckTaskName = "org.apache.ignite.platform.PlatformServiceCallTask";
        
        /** */
        protected IIgnite Grid1;

        /** */
        protected IIgnite Grid2;

        /** */
        protected IIgnite Grid3;

        /** */
        protected IIgnite[] Grids;
        
        /// <summary>
        /// Start grids and deploy test service.
        /// </summary>
        [SetUp]
        public void SetUp()
        {
            StartGrids();
        }
        
        /// <summary>
        /// Stop grids after test.
        /// </summary>
        [TearDown]
        public void TearDown()
        {
            StopGrids();
        }

        [Test]
        public void TestCallPlatformService()
        {
            var cfg = new ServiceConfiguration
            {
                Name = ServiceName,
                TotalCount = 1,
                Service = new TestPlatformService()
            };
            
            Grid1.GetServices().Deploy(cfg);

            Grid1.GetCompute().ExecuteJavaTask<object>(CheckTaskName, ServiceName);
        }
        
        /// <summary>
        /// Starts the grids.
        /// </summary>
        private void StartGrids()
        {
            if (Grid1 != null)
                return;

            Grid1 = Ignition.Start(GetConfiguration(1));
            Grid2 = Ignition.Start(GetConfiguration(2));
            Grid3 = Ignition.Start(GetConfiguration(3));

            Grids = new[] { Grid1, Grid2, Grid3 };
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
        private IgniteConfiguration GetConfiguration(int idx)
        {
            return new IgniteConfiguration(TestUtils.GetTestConfiguration())
            {
                IgniteInstanceName = "grid" + idx,
                BinaryConfiguration = new BinaryConfiguration(typeof(TestValue))
                {
                    NameMapper = BinaryBasicNameMapper.SimpleNameInstance
                }
            };
        }
        
        /** */
        public interface ITestPlatformService: IService
        {
            /** */
            Guid NodeId { get; }
            
            /** */
            Guid? GuidProp { get; set; }
            
            /** */
            TestValue ValueProp { get; set; }

            void ErrorMethod();
        }

        /** */
        public class TestPlatformService : ITestPlatformService
        {
            /** */
            [InstanceResource]
#pragma warning disable 649
            private IIgnite _grid;
#pragma warning restore 649
            
            /** */
            public Guid NodeId => _grid.GetCluster().GetLocalNode().Id;

            /** */
            public Guid? GuidProp { get; set; }
            
            /** */
            public TestValue ValueProp { get; set; }

            /** */
            public void ErrorMethod()
            {
                throw new Exception("Failed method");
            }

            /** */
            public void Init(IServiceContext context)
            {
                // No-op.
            }

            /** */
            public void Execute(IServiceContext context)
            {
                // No-op.
            }

            /** */
            public void Cancel(IServiceContext context)
            {
                // No-op;
            }
        }

        /** */
        public class TestValue
        {
            /** */
            public int Id { get; set; }
            
            /** */
            public string Name { get; set; }
        }
    }
}