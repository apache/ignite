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

namespace Apache.Ignite.Core.Impl.EntityFramework
{
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Impl.Binary;

    /// <summary>
    /// EntityFramework cache entry.
    /// </summary>
    public class EntityFrameworkCacheEntry : IBinaryWriteAware
    {
        /** Cached data. */
        private readonly object _data;

        /** Dependent entity sets and their versions.  */
        private readonly IDictionary<string, long> _entitySets;

        /// <summary>
        /// Initializes a new instance of the <see cref="EntityFrameworkCacheEntry"/> class.
        /// </summary>
        /// <param name="data">The data.</param>
        /// <param name="entitySets">The entity sets.</param>
        public EntityFrameworkCacheEntry(object data, IDictionary<string, long> entitySets)
        {
            Debug.Assert(data != null);

            _data = data;
            _entitySets = entitySets;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="EntityFrameworkCacheEntry"/> class.
        /// </summary>
        /// <param name="reader">The reader.</param>
        public EntityFrameworkCacheEntry(IBinaryReader reader)
        {
            var raw = reader.GetRawReader();

            var count = raw.ReadInt();

            if (count > 0)
            {
                _entitySets = new Dictionary<string, long>(count);

                for (var i = 0; i < count; i++)
                    _entitySets[raw.ReadString()] = raw.ReadLong();
            }

            _data = raw.ReadObject<object>();
        }

        /// <summary>
        /// Writes this object to the given writer.
        /// </summary>
        public void WriteBinary(IBinaryWriter writer)
        {
            var raw = writer.GetRawWriter();

            if (_entitySets != null)
            {
                raw.WriteInt(_entitySets.Count);

                foreach (var entry in _entitySets)
                {
                    raw.WriteString(entry.Key);
                    raw.WriteLong(entry.Value);
                }
            }
            else
                raw.WriteInt(0);

            raw.WriteObject(_data);
        }

        /// <summary>
        /// Gets the data.
        /// </summary>
        public object Data
        {
            get { return _data; }
        }

        /// <summary>
        /// Returns a <see cref="string" /> that represents this instance.
        /// </summary>
        public override string ToString()
        {
            var setVersions = _entitySets == null
                ? "-"
                : _entitySets.Select(x => string.Format("{0}:{1}", x.Key, x.Value))
                    .Aggregate((x, y) => x + ", " + y);

            return string.Format("{0} [EntitySets=({1})]", GetType().Name, setVersions);
        }
    }
}
