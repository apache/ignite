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
    using System;
    using System.Collections.Generic;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Impl.Common;
    using EFCache;

    /// <summary>
    /// Ignite-base EntityFramework second-level cache.
    /// </summary>
    public class IgniteEntityFrameworkCache : ICache
    {
        private readonly ICache<string, object> _cache;

        public IgniteEntityFrameworkCache(ICache<string, object> cache)
        {
            IgniteArgumentCheck.NotNull(cache, "cache");

            _cache = cache;
        }

        public bool GetItem(string key, out object value)
        {
            return _cache.TryGet(key, out value);
        }

        public void PutItem(string key, object value, IEnumerable<string> dependentEntitySets, 
            TimeSpan slidingExpiration, DateTimeOffset absoluteExpiration)
        {
            throw new NotImplementedException();
        }

        public void InvalidateSets(IEnumerable<string> entitySets)
        {
            throw new NotImplementedException();
        }

        public void InvalidateItem(string key)
        {
            throw new NotImplementedException();
        }
    }
}
