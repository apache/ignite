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
    using Apache.Ignite.Core.Impl.Binary;
    using Apache.Ignite.Core.Impl.Cache;
    using Apache.Ignite.Core.Impl.Common;

    /// <summary>
    /// SQL Query.
    /// </summary>
    public class SqlQuery : QueryBase
    {
        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="queryType">Type.</param>
        /// <param name="sql">SQL.</param>
        /// <param name="args">Arguments.</param>
        public SqlQuery(Type queryType, string sql, params object[] args) : this(queryType, sql, false, args)
        {
            // No-op.
        }

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="queryType">Type.</param>
        /// <param name="sql">SQL.</param>
        /// <param name="local">Whether query should be executed locally.</param>
        /// <param name="args">Arguments.</param>
        public SqlQuery(Type queryType, string sql, bool local, params object[] args) 
            : this(queryType == null ? null : queryType.Name, sql, local, args)
        {
            // No-op.
        }
        
        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="queryType">Type.</param>
        /// <param name="sql">SQL.</param>
        /// <param name="args">Arguments.</param>
        public SqlQuery(string queryType, string sql, params object[] args) : this(queryType, sql, false, args)
        {
            // No-op.
        }

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="queryType">Type.</param>
        /// <param name="sql">SQL.</param>
        /// <param name="local">Whether query should be executed locally.</param>
        /// <param name="args">Arguments.</param>
        public SqlQuery(string queryType, string sql, bool local, params object[] args)
        {
            IgniteArgumentCheck.NotNullOrEmpty("queryType", queryType);
            IgniteArgumentCheck.NotNullOrEmpty("sql", sql);

            QueryType = queryType;
            Sql = sql;
            Local = local;
            Arguments = args;
        }

        /// <summary>
        /// Type.
        /// </summary>
        public string QueryType { get; set; }

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
        /// Gets or sets the query timeout. Query will be automatically cancelled if the execution timeout is exceeded.
        /// Default is <see cref="TimeSpan.Zero"/>, which means no timeout.
        /// </summary>
        public TimeSpan Timeout { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether this query contains only replicated tables.
        /// This is a hint for potentially more effective execution.
        /// </summary>
        public bool ReplicatedOnly { get; set; }

        /** <inheritDoc /> */
        internal override void Write(BinaryWriter writer, bool keepBinary)
        {
            if (string.IsNullOrEmpty(Sql))
                throw new ArgumentException("Sql cannot be null or empty");

            if (string.IsNullOrEmpty(QueryType))
                throw new ArgumentException("QueryType cannot be null or empty");

            // 2. Prepare.
            writer.WriteBoolean(Local);
            writer.WriteString(Sql);
            writer.WriteString(QueryType);
            writer.WriteInt(PageSize);

            WriteQueryArgs(writer, Arguments);

            writer.WriteBoolean(EnableDistributedJoins);
            writer.WriteInt((int) Timeout.TotalMilliseconds);
            writer.WriteBoolean(ReplicatedOnly);
        }

        /** <inheritDoc /> */
        internal override CacheOp OpId
        {
            get { return CacheOp.QrySql; }
        }

        /// <summary>
        /// Returns a <see cref="string" /> that represents this instance.
        /// </summary>
        /// <returns>
        /// A <see cref="string" /> that represents this instance.
        /// </returns>
        public override string ToString()
        {
            var args = string.Join(", ", Arguments.Select(x => x == null ? "null" : x.ToString()));

            return string.Format("SqlQuery [Sql={0}, Arguments=[{1}], Local={2}, PageSize={3}, " +
                                 "EnableDistributedJoins={4}, Timeout={5}, ReplicatedOnly={6}]", Sql, args, Local,
                PageSize, EnableDistributedJoins, Timeout, ReplicatedOnly);
        }

    }
}
