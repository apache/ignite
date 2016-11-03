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
    using System.Diagnostics.CodeAnalysis;

    /// <summary>
    /// SQL fields query.
    /// </summary>
    public class SqlFieldsQuery
    {
        /// <summary> Default page size. </summary>
        public const int DfltPageSize = 1024;

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

            PageSize = DfltPageSize;
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
        /// Defaults to <see cref="DfltPageSize"/>.
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
    }
}
