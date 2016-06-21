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

namespace Apache.Ignite.Core.Tests.Cache.Affinity
{
    using System;
    using System.Collections.Generic;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Cache.Affinity;
    using Apache.Ignite.Core.Cache.Configuration;
    using Apache.Ignite.Core.Cluster;
    using NUnit.Framework;

    /// <summary>
    /// Tests user-defined <see cref="IAffinityFunction"/>
    /// </summary>
    public class AffinityFunctionTest
    {
        [Test]
        public void Test()
        {
            var cfg = new IgniteConfiguration(TestUtils.GetTestConfiguration())
            {
                BinaryConfiguration = new BinaryConfiguration(typeof (SimpleAffinityFunction)),
                CacheConfiguration = new[]
                {
                    new CacheConfiguration("cache")
                    {
                        AffinityFunction = new SimpleAffinityFunction()
                    }
                }
            };

            using (var ignite = Ignition.Start(cfg))
            {
                var cache = ignite.GetCache<int, int>("cache");
                Assert.IsNotInstanceOf<SimpleAffinityFunction>(cache.GetConfiguration().AffinityFunction);
            }
        }

        [Test]
        public void TestInject()
        {
            
        }

        [Test]
        public void TestLifetime()
        {
            // TODO: check handle removal on DestroyCache
        }

        private class SimpleAffinityFunction : IAffinityFunction
        {
            public void Reset()
            {
                // No-op.
            }

            public int PartitionCount
            {
                get { return 10; }
            }

            public int GetPartition(object key)
            {
                throw new NotImplementedException();
            }

            public void RemoveNode(Guid nodeId)
            {
                throw new NotImplementedException();
            }

            public IEnumerable<IEnumerable<IClusterNode>> AssignPartitions(IAffinityFunctionContext context)
            {
                throw new NotImplementedException();
            }
        }
    }
}
