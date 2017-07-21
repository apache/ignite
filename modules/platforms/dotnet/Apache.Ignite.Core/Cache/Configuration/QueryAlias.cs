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

namespace Apache.Ignite.Core.Cache.Configuration
{
    using Apache.Ignite.Core.Impl.Common;

    /// <summary>
    /// Represents cache query configuration alias.
    /// </summary>
    public class QueryAlias
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="QueryAlias"/> class.
        /// </summary>
        public QueryAlias()
        {
            // No-op.
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="QueryAlias"/> class.
        /// </summary>
        /// <param name="fullName">The full name.</param>
        /// <param name="alias">The alias.</param>
        public QueryAlias(string fullName, string alias)
        {
            IgniteArgumentCheck.NotNullOrEmpty(fullName, "fullName");
            IgniteArgumentCheck.NotNullOrEmpty(alias, "alias");

            FullName = fullName;
            Alias = alias;
        }

        /// <summary>
        /// Gets or sets the full name of the query field.
        /// </summary>
        public string FullName { get; set; }
        
        /// <summary>
        /// Gets or sets the alias for the full name.
        /// </summary>
        public string Alias { get; set; }
    }
}
