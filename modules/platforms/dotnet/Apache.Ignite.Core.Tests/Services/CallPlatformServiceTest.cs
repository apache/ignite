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
    using System.Collections;
    using System.Linq;
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
        private const string CheckCollectionsTaskName = "org.apache.ignite.platform.PlatformServiceCallCollectionsTask";
        
        /** */
        private const string CheckThinTaskName = "org.apache.ignite.platform.PlatformServiceCallThinTask";

        /** */
        private const string CheckCollectionsThinTaskName =
            "org.apache.ignite.platform.PlatformServiceCallCollectionsThinTask";

        /** */
        protected IIgnite Grid1;

        /** */
        protected IIgnite Grid2;

        /** */
        protected IIgnite Grid3;
        
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

        /// <summary>
        /// Tests call a platform service by invoking a special compute java task,
        /// in which real invocation of the service is made.
        /// <para/>
        /// <param name="local">If true call on local node.</param>
        /// <param name="taskName">Task to test.</param>
        /// </summary>
        [Test]
        public void TestCallPlatformService([Values(true, false)] bool local,
            [Values(CheckTaskName, CheckCollectionsTaskName, CheckThinTaskName, CheckCollectionsThinTaskName)]
            string taskName)
        {
            var cfg = new ServiceConfiguration
            {
                Name = ServiceName,
                TotalCount = 1,
                Service = new TestPlatformService()
            };
            
            Grid1.GetServices().Deploy(cfg);

            Grid1.GetCompute().ExecuteJavaTask<object>(taskName, new object[] { ServiceName, local });
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
        }

        /// <summary>
        /// Stops the grids.
        /// </summary>
        private void StopGrids()
        {
            Grid1 = Grid2 = Grid3 = null;

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
                BinaryConfiguration = new BinaryConfiguration(typeof(TestKey), typeof(TestValue), 
                    typeof(BinarizableTestValue))
                {
                    NameMapper = BinaryBasicNameMapper.SimpleNameInstance
                }
            };
        }
        
        /** */
        public interface ITestPlatformService : IService
        {
            /** */
            Guid NodeId { get; }
            
            /** */
            Guid? GuidProp { get; set; }
            
            /** */
            TestValue ValueProp { get; set; }

            /** */
            void ErrorMethod();

            /** */
            TestValue[] AddOneToEach(TestValue[] arr);

            /** */
            ICollection AddOneToEachCollection(ICollection col);

            /** */
            IDictionary AddOneToEachDictionary(IDictionary dict);

            /** */
            BinarizableTestValue AddOne(BinarizableTestValue val);
        }

        #pragma warning disable 649
        
        /** */
        private class TestPlatformService : ITestPlatformService
        {
            /** */
            [InstanceResource]
            private IIgnite _grid;

            /** <inheritdoc /> */
            public Guid NodeId
            {
                get { return _grid.GetCluster().GetLocalNode().Id;}
            }

            /** <inheritdoc /> */
            public Guid? GuidProp { get; set; }
            
            /** <inheritdoc /> */
            public TestValue ValueProp { get; set; }

            /** <inheritdoc /> */
            public void ErrorMethod()
            {
                throw new Exception("Failed method");
            }
            
            /** <inheritdoc /> */
            public TestValue[] AddOneToEach(TestValue[] arr)
            {
                return arr.Select(val => new TestValue()
                {
                    Id = val.Id + 1,
                    Name = val.Name

                }).ToArray();
            }

            /** <inheritdoc /> */
            public ICollection AddOneToEachCollection(ICollection col)
            {
                var res =  col.Cast<TestValue>().Select(val => new TestValue()
                {
                    Id = val.Id + 1,
                    Name = val.Name
                
                }).ToList();

                return new ArrayList(res);
            }

            /** <inheritdoc /> */
            public IDictionary AddOneToEachDictionary(IDictionary dict)
            {
                var res = new Hashtable();

                foreach (DictionaryEntry pair in dict)
                {
                    var k = new TestKey(((TestKey) pair.Key).Id + 1);
                    
                    var v = new TestValue()
                    {
                        Id = ((TestValue)pair.Value).Id + 1,
                        Name = ((TestValue)pair.Value).Name
                    };
                    
                    res.Add(k, v);
                }
                
                return res;
            }

            /** <inheritdoc /> */
            public BinarizableTestValue AddOne(BinarizableTestValue val)
            {
                return new BinarizableTestValue()
                {
                    Id = val.Id + 1,
                    Name = val.Name
                };
            }

            /** <inheritdoc /> */
            public void Init(IServiceContext context)
            {
                // No-op.
            }

            /** <inheritdoc /> */
            public void Execute(IServiceContext context)
            {
                // No-op.
            }

            /** <inheritdoc /> */
            public void Cancel(IServiceContext context)
            {
                // No-op;
            }
        }
        
        #pragma warning restore 649

        /** */
        public class TestKey
        {
            /** */
            public TestKey(int id)
            {
                Id = id;
            }

            /** */
            public int Id { get; set; }
            
            /** <inheritdoc /> */
            public override int GetHashCode()
            {
                return Id;
            }
            
            /** <inheritdoc /> */
            public override bool Equals(object obj)
            {
                if (ReferenceEquals(null, obj)) 
                    return false;
                
                if (ReferenceEquals(this, obj)) 
                    return true;
                
                if (obj.GetType() != GetType()) 
                    return false;
                
                return Id == ((TestKey)obj).Id;
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

        /** */
        public class BinarizableTestValue : TestValue, IBinarizable
        {
            /** <inheritdoc /> */
            public void WriteBinary(IBinaryWriter writer)
            {
                writer.WriteInt("id", Id);
                writer.WriteString("name", Name);
            }

            /** <inheritdoc /> */
            public void ReadBinary(IBinaryReader reader)
            {
                Id = reader.ReadInt("id");
                Name = reader.ReadString("name");
            }
        }
    }
}
