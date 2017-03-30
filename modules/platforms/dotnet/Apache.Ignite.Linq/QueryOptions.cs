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

namespace Apache.Ignite.Linq
{
    using System.ComponentModel;
    using Apache.Ignite.Core.Cache.Configuration;
    using Apache.Ignite.Core.Cache.Query;

    /// <summary>
    /// Cache query options.
    /// </summary>
    public class QueryOptions
    {
        /// <summary> Default page size. </summary>
        public const int DefaultPageSize = SqlFieldsQuery.DfltPageSize;

        /// <summary>
        /// Initializes a new instance of the <see cref="QueryOptions"/> class.
        /// </summary>
        public QueryOptions()
        {
            PageSize = DefaultPageSize;
        }

        /// <summary>
        /// Local flag. When set query will be executed only on local node, so only local 
        /// entries will be returned as query result.
        /// <para />
        /// Defaults to <c>false</c>.
        /// </summary>
        public bool Local { get; set; }

        /// <summary>
        /// Page size, defaults to <see cref="DefaultPageSize"/>.
        /// </summary>
        [DefaultValue(DefaultPageSize)]
        public int PageSize { get; set; }

        /// <summary>
        /// Gets or sets the name of the table. 
        /// <para />
        /// Table name is equal to short class name of a cache value.
        /// When a cache has only one type of values, or only one <see cref="QueryEntity" /> defined,
        /// table name will be inferred and can be omitted (null).
        /// </summary>
        /// <value>
        /// The name of the table.
        /// </value>
        public string TableName { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether distributed joins should be enabled for this query.
        /// <para />
        /// When disabled, join results will only contain colocated data (joins work locally).
        /// When enabled, joins work as expected, no matter how the data is distributed.
        /// </summary>
        /// <value>
        /// <c>true</c> if enable distributed joins should be enabled; otherwise, <c>false</c>.
        /// </value>
        public bool EnableDistributedJoins { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether join order of tables should be enforced.
        /// <para />
        /// When true, query optimizer will not reorder tables in join.
        /// <para />
        /// It is not recommended to enable this property until you are sure that your indexes
        /// and the query itself are correct and tuned as much as possible but
        /// query optimizer still produces wrong join order.
        /// </summary>
        /// <value>
        ///   <c>true</c> if join order should be enforced; otherwise, <c>false</c>.
        /// </value>
        public bool EnforceJoinOrder { get; set; }
    }
}
