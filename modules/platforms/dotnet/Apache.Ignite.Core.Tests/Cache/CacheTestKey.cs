/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

namespace Apache.Ignite.Core.Tests.Cache
{
    using System.Text;

    /// <summary>
    /// Test key.
    /// </summary>
    internal class CacheTestKey
    {
        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="id">ID.</param>
        public CacheTestKey(int id)
        {
            Id = id;
        }

        /// <summary>
        /// ID.
        /// </summary>
        public int Id
        {
            get;
            set;
        }

        /** <inheritdoc /> */
        public override bool Equals(object obj)
        {
            CacheTestKey other = obj as CacheTestKey;

            return other != null && Id == other.Id;
        }

        /** <inheritdoc /> */
        public override int GetHashCode()
        {
            return Id;
        }

        /** <inheritdoc /> */
        public override string ToString()
        {
            return new StringBuilder()
                .Append(typeof(CacheTestKey).Name)
                .Append(" [id=").Append(Id)
                .Append(']').ToString();
        }
    }
}