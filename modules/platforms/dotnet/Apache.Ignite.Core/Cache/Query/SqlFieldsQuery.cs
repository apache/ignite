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

namespace Apache.Ignite.Core.Cache.Query
{
    using System;
    using System.Diagnostics.CodeAnalysis;
    using System.Linq;

    /// <summary>
    /// SQL fields query.
    /// </summary>
    public class SqlFieldsQuery
    {
        /// <summary> Default page size. </summary>
        public const int DefaultPageSize = 1024;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="sql">SQL.</param>
        /// <param name="args">Arguments.</param>
        public SqlFieldsQuery(string sql, params object[] args) : this(sql, false, args)
        {
            // No-op.
        }

        /// <summary>
        /// Constructor,
        /// </summary>
        /// <param name="sql">SQL.</param>
        /// <param name="loc">Whether query should be executed locally.</param>
        /// <param name="args">Arguments.</param>
        public SqlFieldsQuery(string sql, bool loc, params object[] args)
        {
            Sql = sql;
            Local = loc;
            Arguments = args;

            PageSize = DefaultPageSize;
        }

        /// <summary>
        /// SQL.
        /// </summary>
        public string Sql { get; set; }
        
        /// <summary>
        /// Arguments.
        /// </summary>
        [SuppressMessage("Microsoft.Performance", "CA1819:PropertiesShouldNotReturnArrays")]
        public object[] Arguments { get; set; }

        /// <summary>
        /// Local flag. When set query will be executed only on local node, so only local 
        /// entries will be returned as query result.
        /// <para />
        /// Defaults to <c>false</c>.
        /// </summary>
        public bool Local { get; set; }

        /// <summary>
        /// Optional page size.
        /// <para />
        /// Defaults to <see cref="DefaultPageSize"/>.
        /// </summary>
        public int PageSize { get; set; }

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
        /// Returns a <see cref="string" /> that represents this instance.
        /// </summary>
        /// <returns>
        /// A <see cref="string" /> that represents this instance.
        /// </returns>
        public override string ToString()
        {
            var args = string.Join(", ", Arguments.Select(x => x == null ? "null" : x.ToString()));

            return string.Format("SqlFieldsQuery [Sql={0}, Arguments=[{1}], Local={2}, PageSize={3}, " +
                                 "EnableDistributedJoins={4}, EnforceJoinOrder={5}, Timeout={6}, ReplicatedOnly={7}" +
                                 ", Colocated={8}]", Sql, args, Local,
                                 PageSize, EnableDistributedJoins, EnforceJoinOrder, Timeout, ReplicatedOnly,
                                 Colocated);
        }
    }
}
