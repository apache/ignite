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

namespace Apache.Ignite.Core.Tests.Deployment
{
    using Apache.Ignite.Core.Compute;
    using Apache.Ignite.Core.Resource;

    /// <summary>
    /// Function to get a value from cache.
    /// </summary>
    public class CacheGetFunc<TKey, TValue> : IComputeFunc<TValue>
    {
        /// <summary>
        /// Gets or sets the cache key.
        /// </summary>
        public TKey Key { get; set; }

        /// <summary>
        /// Gets or sets the name of the cache.
        /// </summary>
        public string CacheName { get; set; }

        /// <summary>
        /// Gets or sets the ignite.
        /// </summary>
        [InstanceResource]
        public IIgnite Ignite { get;set; }

        /** <inheritdoc /> */
        public TValue Invoke()
        {
            return Ignite.GetCache<TKey, TValue>(CacheName).Get(Key);
        }
    }
}
