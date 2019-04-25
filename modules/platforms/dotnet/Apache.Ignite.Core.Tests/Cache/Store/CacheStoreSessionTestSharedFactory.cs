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
    using NUnit.Framework;

    /// <summary>
    /// Session test with shared PlatformDotNetCacheStoreFactory,
    /// which causes the same store insance to be used for both caches.
    /// </summary>
    [TestFixture]
    public class CacheStoreSessionTestSharedFactory : CacheStoreSessionTest
    {
        /** <inheritdoc /> */
        protected override IgniteConfiguration GetIgniteConfiguration()
        {
            return new IgniteConfiguration(TestUtils.GetTestConfiguration())
            {
                SpringConfigUrl = @"config\cache\store\cache-store-session-shared-factory.xml"
            };
        }

        /** <inheritdoc /> */
        protected override int StoreCount
        {
            get
            {
                // Shared PlatformDotNetCacheStoreFactory results in a single store instance.
                return 2;
            }
        }
    }
}