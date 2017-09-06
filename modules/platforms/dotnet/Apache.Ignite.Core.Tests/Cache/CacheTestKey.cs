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