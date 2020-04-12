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

namespace Apache.Ignite.Core.Tests.Cache.Platform
{
    using System.Security;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Resource;
    using NUnit.Framework;

    /// <summary>
    /// Scan query filter that checks whether values come from platform cache.
    /// </summary>
    public class ScanQueryPlatformCacheFilter : ICacheEntryFilter<int, Foo>
    {
        /// <summary>
        /// Gets or sets the cache name.
        /// </summary>
        public string CacheName { get; set; }
        
        /// <summary>
        /// Gets or sets the key that should cause an exception in <see cref="Invoke"/>. 
        /// </summary>
        public int? FailKey { get; set; }
        
        /// <summary>
        /// Injected Ignite.
        /// </summary>
        [InstanceResource]
        public IIgnite Ignite { get; set; }
        
        /** <inheritdoc /> */
        public bool Invoke(ICacheEntry<int, Foo> entry)
        {
            if (entry.Key == FailKey)
            {
                throw new SecurityException("Crash in filter");
            }
            
            var cache = Ignite.GetCache<int, Foo>(CacheName);
            var platformVal = cache.LocalPeek(entry.Key, CachePeekMode.Platform);

            Assert.AreSame(platformVal, entry.Value);

            return true;
        }
    }
}