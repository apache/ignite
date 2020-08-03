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
    using System;
    using System.ComponentModel;
    using Apache.Ignite.Core.Cache.Configuration;
    using Apache.Ignite.Core.Cache.Query;

    /// <summary>
    /// Cache query options.
    /// </summary>
    public class QueryOptions
    {
        /// <summary> Default page size. </summary>
        public const int DefaultPageSize = SqlFieldsQuery.DefaultPageSize;

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

        /// <summary>
        /// Gets or sets the query timeout. Query will be automatically cancelled if the execution timeout is exceeded.
        /// Default is <see cref="TimeSpan.Zero"/>, which means no timeout.
        /// </summary>
        public TimeSpan Timeout { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether this query contains only replicated tables.
        /// This is a hint for potentially more effective execution.
        /// </summary>
        [Obsolete("No longer used as of Apache Ignite 2.8.")]
        public bool ReplicatedOnly { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether this query operates on colocated data.
        /// <para />
        /// Whenever Ignite executes a distributed query, it sends sub-queries to individual cluster members.
        /// If you know in advance that the elements of your query selection are colocated together on the same
        /// node and you group by colocated key (primary or affinity key), then Ignite can make significant
        /// performance and network optimizations by grouping data on remote nodes.
        /// </summary>
        public bool Colocated { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether this query is lazy.
        /// <para />
        /// By default Ignite attempts to fetch the whole query result set to memory and send it to the client.
        /// For small and medium result sets this provides optimal performance and minimize duration of internal
        /// database locks, thus increasing concurrency.
        /// <para />
        /// If result set is too big to fit in available memory this could lead to excessive GC pauses and even
        /// OutOfMemoryError. Use this flag as a hint for Ignite to fetch result set lazily, thus minimizing memory
        /// consumption at the cost of moderate performance hit.
        /// </summary>
        public bool Lazy { get; set; }
    }
}
