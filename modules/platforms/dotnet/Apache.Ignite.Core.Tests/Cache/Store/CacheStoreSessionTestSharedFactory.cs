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