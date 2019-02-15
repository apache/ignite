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

namespace Apache.Ignite.Core.Cache.Configuration
{
    /// <summary>
    /// Cache rebalance mode. When rebalancing is enabled (i.e. has value other than <see cref="None"/>), 
    /// distributed caches will attempt to rebalance all necessary values from other grid nodes. 
    /// <para />
    /// Replicated caches will try to load the full set of cache entries from other nodes, 
    /// while partitioned caches will only load the entries for which current node is primary or backup.
    /// <para />
    /// Note that rebalance mode only makes sense for <see cref="CacheMode.Replicated"/> 
    /// and <see cref="CacheMode.Partitioned"/> caches. Caches with <see cref="CacheMode.Local"/> 
    /// mode are local by definition and therefore cannot rebalance any values from neighboring nodes.
    /// </summary>
    public enum CacheRebalanceMode
    {
        /// <summary>
        /// Synchronous rebalance mode. Distributed caches will not start until all necessary data
        /// is loaded from other available grid nodes.
        /// </summary>
        Sync,

        /// <summary>
        /// Asynchronous rebalance mode. Distributed caches will start immediately and will load all necessary
        /// data from other available grid nodes in the background.
        /// </summary>
        Async,

        /// <summary>
        /// In this mode no rebalancing will take place which means that caches will be either loaded on
        /// demand from persistent store whenever data is accessed, or will be populated explicitly.
        /// </summary>
        None
    }
}