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

namespace Apache.Ignite.EntityFramework
{
    using System.Data.Entity;
    using System.Data.Entity.Core.Common;
    using Apache.Ignite.Core;
    using Apache.Ignite.Core.Cache.Configuration;
    using EFCache;

    /// <summary>
    /// <see cref="DbConfiguration"/> implementation that uses Ignite as a second-level cache 
    /// for Entity Framework queries.
    /// <para />
    /// This implementation uses default Ignite instance (with null <see cref="IgniteConfiguration.GridName"/>) 
    /// and default cache (null <see cref="CacheConfiguration.Name"/>).
    /// <para />
    /// Ignite instance will be started automatically, if it is not started yet.
    /// <para /> 
    /// <see cref="IgniteConfigurationSection"/> with name "igniteConfiguration" will be picked up 
    /// when starting Ignite, if present.
    /// </summary>
    public class IgniteDbConfiguration : DbConfiguration
    {
        public IgniteDbConfiguration()
        {
            // TODO: Read default config?
            var ignite = Ignition.TryGetIgnite() ?? Ignition.Start();
            var cache = ignite.GetOrCreateCache<string, object>((string) null);

            var efCache = new IgniteEntityFrameworkCache(cache);
            var transactionHandler = new CacheTransactionHandler(efCache);

            AddInterceptor(transactionHandler);

            var cachingPolicy = new CachingPolicy();

            Loaded +=
                (sender, args) => args.ReplaceService<DbProviderServices>(
                    (s, _) => new CachingProviderServices(s, transactionHandler,
                        cachingPolicy));
        }
    }
}