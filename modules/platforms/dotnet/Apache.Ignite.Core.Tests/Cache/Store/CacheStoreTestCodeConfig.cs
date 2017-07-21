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

namespace Apache.Ignite.Core.Tests.Cache.Store
{
    using System;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Cache.Configuration;
    using Apache.Ignite.Core.Cache.Store;
    using Apache.Ignite.Core.Common;
    using NUnit.Framework;

    /// <summary>
    /// Tests cache store without Spring.
    /// </summary>
    public class CacheStoreTestCodeConfig : CacheStoreTest
    {
        /// <summary>
        /// Fixture setup.
        /// </summary>
        [TestFixtureSetUp]
        public override void BeforeTests()
        {
            var cfg = new IgniteConfiguration(TestUtils.GetTestConfiguration())
            {
                BinaryConfiguration = new BinaryConfiguration(typeof(Key), typeof(Value)),
                CacheConfiguration = new[]
                {
                    new CacheConfiguration
                    {
                        Name = "binary_store",
                        CacheMode = CacheMode.Local,
                        AtomicityMode = CacheAtomicityMode.Transactional,
                        WriteThrough = true,
                        ReadThrough = true,
                        KeepBinaryInStore = true,
                        CacheStoreFactory = new StoreFactory()
                    }, 
                    new CacheConfiguration
                    {
                        Name = "object_store",
                        CacheMode = CacheMode.Local,
                        AtomicityMode = CacheAtomicityMode.Transactional,
                        WriteThrough = true,
                        ReadThrough = true,
                        KeepBinaryInStore = false,
                        CacheStoreFactory = new StoreFactory()
                    }, 
                    new CacheConfiguration
                    {
                        Name = "template_store*",
                        CacheMode = CacheMode.Local,
                        AtomicityMode = CacheAtomicityMode.Transactional,
                        WriteThrough = true,
                        ReadThrough = true,
                        KeepBinaryInStore = false,
                        CacheStoreFactory = new StoreFactory()
                    }, 
                    new CacheConfiguration
                    {
                        Name = "custom_store",
                        CacheMode = CacheMode.Local,
                        AtomicityMode = CacheAtomicityMode.Transactional,
                        WriteThrough = true,
                        ReadThrough = true,
                        CacheStoreFactory = new CustomStoreFactory()
                    }, 
                }
            };

            Ignition.Start(cfg);
        }

        [Serializable]
        private class StoreFactory : IFactory<ICacheStore>
        {
            public ICacheStore CreateInstance()
            {
                return new CacheTestStore();
            }
        }

        [Serializable]
        private class CustomStoreFactory : IFactory<ICacheStore>
        {
            public ICacheStore CreateInstance()
            {
                return new CacheTestStore {IntProperty = 42, StringProperty = "String value"};
            }
        }
    }
}
