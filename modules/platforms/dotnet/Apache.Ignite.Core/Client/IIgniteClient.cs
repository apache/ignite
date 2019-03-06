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

namespace Apache.Ignite.Core.Client
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics.CodeAnalysis;
    using System.Net;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Client.Cache;

    /// <summary>
    /// Main entry point for Ignite Thin Client APIs.
    /// You can obtain an instance of <see cref="IIgniteClient"/> through one of the
    /// <see cref="Ignition.StartClient()"/> overloads.
    /// <para />
    /// Instances of this class and all nested APIs are thread safe.
    /// </summary>
    public interface IIgniteClient : IDisposable
    {
        /// <summary>
        /// Gets the cache instance for the given name to work with keys and values of specified types.
        /// <para/>
        /// You can get instances of <see cref="ICacheClient{TK,TV}"/> of the same name,
        /// but with different key/value types.
        /// These will use the same named cache, but only allow working with entries of specified types.
        /// Attempt to retrieve an entry of incompatible type will result in <see cref="InvalidCastException"/>.
        /// Use <see cref="GetCache{TK,TV}"/> in order to work with entries of arbitrary types.
        /// </summary>
        /// <param name="name">Cache name.</param>
        /// <returns>Cache instance for given name.</returns>
        /// <typeparam name="TK">Cache key type.</typeparam>
        /// <typeparam name="TV">Cache value type.</typeparam>
        ICacheClient<TK, TV> GetCache<TK, TV>(string name);

        /// <summary>
        /// Gets existing cache with the given name or creates new one using template configuration.
        /// </summary>
        /// <typeparam name="TK">Cache key type.</typeparam>
        /// <typeparam name="TV">Cache value type.</typeparam>
        /// <param name="name">Cache name.</param>
        /// <returns>Existing or newly created cache.</returns>
        ICacheClient<TK, TV> GetOrCreateCache<TK, TV>(string name);

        /// <summary>
        /// Gets existing cache with the given name or creates new one using provided configuration.
        /// </summary>
        /// <typeparam name="TK">Cache key type.</typeparam>
        /// <typeparam name="TV">Cache value type.</typeparam>
        /// <param name="configuration">Cache configuration.</param>
        /// <returns>Existing or newly created cache.</returns>
        ICacheClient<TK, TV> GetOrCreateCache<TK, TV>(CacheClientConfiguration configuration);

        /// <summary>
        /// Dynamically starts new cache using template configuration.
        /// </summary>
        /// <typeparam name="TK">Cache key type.</typeparam>
        /// <typeparam name="TV">Cache value type.</typeparam>
        /// <param name="name">Cache name.</param>
        /// <returns>Existing or newly created cache.</returns>
        ICacheClient<TK, TV> CreateCache<TK, TV>(string name);

        /// <summary>
        /// Dynamically starts new cache using provided configuration.
        /// </summary>
        /// <typeparam name="TK">Cache key type.</typeparam>
        /// <typeparam name="TV">Cache value type.</typeparam>
        /// <param name="configuration">Cache configuration.</param>
        /// <returns>Existing or newly created cache.</returns>
        ICacheClient<TK, TV> CreateCache<TK, TV>(CacheClientConfiguration configuration);

        /// <summary>
        /// Gets the collection of names of currently available caches, or empty collection if there are no caches.
        /// </summary>
        /// <returns>Collection of names of currently available caches.</returns>
        ICollection<string> GetCacheNames();

        /// <summary>
        /// Destroys dynamically created (with <see cref="CreateCache{TK,TV}(string)"/> or 
        /// <see cref="GetOrCreateCache{TK,TV}(string)"/>) cache.
        /// </summary>
        /// <param name="name">The name of the cache to stop.</param>
        void DestroyCache(string name);

        /// <summary>
        /// Gets Ignite binary services.
        /// </summary>
        /// <returns>Instance of <see cref="IBinary"/> interface</returns>
        [SuppressMessage("Microsoft.Design", "CA1024:UsePropertiesWhereAppropriate", Justification = "Semantics.")]
        IBinary GetBinary();

        /// <summary>
        /// Gets the configuration.
        /// </summary>
        [SuppressMessage("Microsoft.Design", "CA1024:UsePropertiesWhereAppropriate", Justification = "Semantics.")]
        IgniteClientConfiguration GetConfiguration();

        /// <summary>
        /// Gets the current remote EndPoint.
        /// </summary>
        [SuppressMessage("Microsoft.Naming", "CA1702:CompoundWordsShouldBeCasedCorrectly",
            Justification = "Consistency with EndPoint class name.")]
        EndPoint RemoteEndPoint { get; }

        /// <summary>
        /// Gets the current local EndPoint.
        /// </summary>
        [SuppressMessage("Microsoft.Naming", "CA1702:CompoundWordsShouldBeCasedCorrectly",
            Justification = "Consistency with EndPoint class name.")]
        EndPoint LocalEndPoint { get; }
    }
}
