/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

namespace Apache.Ignite.AspNet
{
    using System;
    using System.Collections.Specialized;
    using System.Diagnostics.CodeAnalysis;
    using System.Web.Caching;
    using Apache.Ignite.AspNet.Impl;
    using Apache.Ignite.Core;
    using Apache.Ignite.Core.Cache;

    /// <summary>
    /// ASP.NET output cache provider that uses Ignite cache as a storage.
    /// <para />
    /// You can either start Ignite yourself, and provide <c>gridName</c> attribute, 
    /// or provide <c>igniteConfigurationSectionName</c> attribute to start Ignite automatically from specified
    /// configuration section (see <see cref="IgniteConfigurationSection"/>) 
    /// using <c>igniteConfigurationSectionName</c>.
    /// <para />
    /// <c>cacheName</c> attribute specifies Ignite cache name to use for data storage. This attribute can be omitted 
    /// if cache name is null.
    /// </summary>
    public class IgniteOutputCacheProvider : OutputCacheProvider
    {
        /** */
        private volatile ExpiryCacheHolder<string, object> _expiryCacheHolder;

        /// <summary>
        /// Returns a reference to the specified entry in the output cache.
        /// </summary>
        /// <param name="key">A unique identifier for a cached entry in the output cache.</param>
        /// <returns>
        /// The <paramref name="key" /> value that identifies the specified entry in the cache, or null if the specified entry is not in the cache.
        /// </returns>
        public override object Get(string key)
        {
            object res;

            return Cache.TryGet(key, out res) ? res : null;
        }

        /// <summary>
        /// Inserts the specified entry into the output cache.
        /// </summary>
        /// <param name="key">A unique identifier for <paramref name="entry" />.</param>
        /// <param name="entry">The content to add to the output cache.</param>
        /// <param name="utcExpiry">The time and date on which the cached entry expires.</param>
        /// <returns>
        /// A reference to the specified provider.
        /// </returns>
        public override object Add(string key, object entry, DateTime utcExpiry)
        {
            return _expiryCacheHolder.GetCacheWithExpiry(utcExpiry).GetAndPutIfAbsent(key, entry).Value;
        }

        /// <summary>
        /// Inserts the specified entry into the output cache, overwriting the entry if it is already cached.
        /// </summary>
        /// <param name="key">A unique identifier for <paramref name="entry" />.</param>
        /// <param name="entry">The content to add to the output cache.</param>
        /// <param name="utcExpiry">The time and date on which the cached <paramref name="entry" /> expires.</param>
        public override void Set(string key, object entry, DateTime utcExpiry)
        {
            _expiryCacheHolder.GetCacheWithExpiry(utcExpiry)[key] = entry;
        }

        /// <summary>
        /// Removes the specified entry from the output cache.
        /// </summary>
        /// <param name="key">The unique identifier for the entry to remove from the output cache.</param>
        public override void Remove(string key)
        {
            Cache.Remove(key);
        }

        /// <summary>
        /// Initializes the provider.
        /// </summary>
        /// <param name="name">The friendly name of the provider.</param>
        /// <param name="config">A collection of the name/value pairs representing the provider-specific attributes specified in the configuration for this provider.</param>
        [SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods")]
        public override void Initialize(string name, NameValueCollection config)
        {
            base.Initialize(name, config);

            var cache = ConfigUtil.InitializeCache<string, object>(config, GetType(), null);

            _expiryCacheHolder = new ExpiryCacheHolder<string, object>(cache);
        }


        /// <summary>
        /// Gets the cache.
        /// </summary>
        private ICache<string, object> Cache
        {
            get
            {
                var holder = _expiryCacheHolder;

                if (holder == null)
                    throw new InvalidOperationException(GetType() + " has not been initialized.");

                return holder.Cache;
            }
        }
    }
}
