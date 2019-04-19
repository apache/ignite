/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 * 
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

namespace Apache.Ignite.Core.Tests.Cache.Store
{
    using Apache.Ignite.Core.Cache.Configuration;
    using Apache.Ignite.Core.Cache.Store;
    using Apache.Ignite.Core.Common;
    using NUnit.Framework;

    /// <summary>
    /// Tests store session with programmatic configuration (uses different store factory on Java side).
    /// </summary>
    [TestFixture]
    public class CacheStoreSessionTestCodeConfig : CacheStoreSessionTest
    {
        /** <inheritdoc /> */
        protected override IgniteConfiguration GetIgniteConfiguration()
        {
            return new IgniteConfiguration(TestUtils.GetTestConfiguration())
            {
                CacheConfiguration = new[]
                {
                    new CacheConfiguration(Cache1)
                    {
                        AtomicityMode = CacheAtomicityMode.Transactional,
                        ReadThrough = true,
                        WriteThrough = true,
                        CacheStoreFactory = new StoreFactory()
                    },
                    new CacheConfiguration(Cache2)
                    {
                        AtomicityMode = CacheAtomicityMode.Transactional,
                        ReadThrough = true,
                        WriteThrough = true,
                        CacheStoreFactory = new StoreFactory()
                    }
                }
            };
        }

        /// <summary>
        /// Store factory.
        /// </summary>
        private class StoreFactory : IFactory<ICacheStore>
        {
            /** <inheritdoc /> */
            public ICacheStore CreateInstance()
            {
                return new Store();
            }
        }
    }
}