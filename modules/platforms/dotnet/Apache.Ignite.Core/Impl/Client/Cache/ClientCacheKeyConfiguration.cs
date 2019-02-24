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

namespace Apache.Ignite.Core.Impl.Client.Cache
{
    /// <summary>
    /// Client cache key configuration.
    /// </summary>
    internal class ClientCacheKeyConfiguration  // TODO: Struct?
    {
        /** Cache ID. */
        private int _cacheId;

        /** Key type ID. */
        private readonly int _keyTypeId;

        /** Affinity key field ID. */
        private readonly int _affinityKeyFieldId;

        /// <summary>
        /// Initializes a new instance of the <see cref="ClientCacheKeyConfiguration"/> class.
        /// </summary>
        public ClientCacheKeyConfiguration(int cacheId, int keyTypeId, int affinityKeyFieldId)
        {
            _cacheId = cacheId;
            _keyTypeId = keyTypeId;
            _affinityKeyFieldId = affinityKeyFieldId;
        }

        /// <summary>
        /// Gets the cache ID.
        /// </summary>
        public int CacheId
        {
            get { return _cacheId; }
        }

        /// <summary>
        /// Gets the key type ID.
        /// </summary>
        public int KeyTypeId
        {
            get { return _keyTypeId; }
        }

        /// <summary>
        /// Gets the Affinity Key field ID.
        /// </summary>
        public int AffinityKeyFieldId
        {
            get { return _affinityKeyFieldId; }
        }
    }
}
