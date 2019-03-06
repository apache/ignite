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

namespace Apache.Ignite.EntityFramework.Impl
{
    using System.Collections.Generic;
    using System.Data.Entity.Core.Metadata.Edm;
    using System.Diagnostics;

    /// <summary>
    /// Represents a cache key, including dependent entity sets and their versions.
    /// </summary>
    internal class DbCacheKey
    {
        /** Original string key. */
        private readonly string _key;

        /** Ordered entity sets. */
        private readonly ICollection<EntitySetBase> _entitySets;

        /** Entity set versions. */
        private readonly IDictionary<string, long> _entitySetVersions;

        /// <summary>
        /// Initializes a new instance of the <see cref="DbCacheKey"/> class.
        /// </summary>
        public DbCacheKey(string key, ICollection<EntitySetBase> entitySets, 
            IDictionary<string, long> entitySetVersions)
        {
            Debug.Assert(key != null);

            _key = key;
            _entitySetVersions = entitySetVersions;
            _entitySets = entitySets;
        }

        /// <summary>
        /// Gets the key.
        /// </summary>
        public string Key
        {
            get { return _key; }
        }

        /// <summary>
        /// Gets the entity sets.
        /// </summary>
        public ICollection<EntitySetBase> EntitySets
        {
            get { return _entitySets; }
        }

        /// <summary>
        /// Gets the entity set versions.
        /// </summary>
        public IDictionary<string, long> EntitySetVersions
        {
            get { return _entitySetVersions; }
        }

        ///// <summary>
        ///// Gets the versioned key.
        ///// </summary>
        //public void GetStringKey()
        //{
        //    if (_entitySetVersions == null)
        //        return _key;

        //    var sb = new StringBuilder(_key);

        //    // Versions should be in the same order, so we can't iterate over the dictionary.
        //    foreach (var entitySet in _entitySets)
        //        sb.AppendFormat("_{0}", _entitySetVersions[entitySet.Name]);

        //    return sb.ToString();
        //}
    }
}
