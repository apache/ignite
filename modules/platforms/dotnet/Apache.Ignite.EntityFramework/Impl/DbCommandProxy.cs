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

namespace Apache.Ignite.EntityFramework.Impl
{
    using System;
    using System.Data;
    using System.Data.Common;
    using System.Diagnostics;
    using System.Diagnostics.CodeAnalysis;
    using System.Text;

    /// <summary>
    /// Command proxy.
    /// </summary>
    internal class DbCommandProxy : DbCommand
    {
        /** */
        private readonly DbCommand _command;

        /** */
        private readonly DbCommandInfo _commandInfo;

        /// <summary>
        /// Initializes a new instance of the <see cref="DbCommandProxy"/> class.
        /// </summary>
        public DbCommandProxy(DbCommand command, DbCommandInfo info)
        {
            Debug.Assert(command != null);
            Debug.Assert(info != null);

            _command = command;
            _commandInfo = info;
        }

        /// <summary>
        /// Gets the inner command.
        /// </summary>
        [ExcludeFromCodeCoverage]
        public DbCommand InnerCommand
        {
            get { return _command; }
        }

        /// <summary>
        /// Gets the command information.
        /// </summary>
        [ExcludeFromCodeCoverage]
        public DbCommandInfo CommandInfo
        {
            get { return _commandInfo; }
        }

        /** <inheritDoc /> */
        [ExcludeFromCodeCoverage]
        public override void Prepare()
        {
            _command.Prepare();
        }

        /** <inheritDoc /> */
        [SuppressMessage("Microsoft.Security", "CA2100:Review SQL queries for security vulnerabilities", Justification = "This class is just a proxy.")]
        public override string CommandText
        {
            get { return _command.CommandText; }
            set { _command.CommandText = value; }
        }

        /** <inheritDoc /> */
        public override int CommandTimeout
        {
            get { return _command.CommandTimeout; }
            set { _command.CommandTimeout = value; }
        }

        /** <inheritDoc /> */
        [ExcludeFromCodeCoverage]
        public override CommandType CommandType
        {
            get { return _command.CommandType; }
            set { _command.CommandType = value; }
        }

        /** <inheritDoc /> */
        public override UpdateRowSource UpdatedRowSource
        {
            get { return _command.UpdatedRowSource; }
            set { _command.UpdatedRowSource = value; }
        }

        /** <inheritDoc /> */
        protected override DbConnection DbConnection
        {
            get { return _command.Connection; }
            set { _command.Connection = value; }
        }

        /** <inheritDoc /> */
        protected override DbParameterCollection DbParameterCollection
        {
            get { return _command.Parameters; }
        }

        /** <inheritDoc /> */
        protected override DbTransaction DbTransaction
        {
            get { return _command.Transaction; }
            set { _command.Transaction = value; }
        }

        /** <inheritDoc /> */
        [ExcludeFromCodeCoverage]
        public override bool DesignTimeVisible
        {
            get { return _command.DesignTimeVisible; }
            set { _command.DesignTimeVisible = value; }
        }

        /** <inheritDoc /> */
        [ExcludeFromCodeCoverage]
        public override void Cancel()
        {
            _command.Cancel();
        }

        /** <inheritDoc /> */
        [ExcludeFromCodeCoverage]
        protected override DbParameter CreateDbParameter()
        {
            return _command.CreateParameter();
        }

        /** <inheritDoc /> */
        protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
        {
            if (_commandInfo.IsModification)
            {
                // Execute reader, then invalidate cached data.
                var dbReader = _command.ExecuteReader(behavior);

                InvalidateCache();

                return dbReader;
            }

            if (Transaction != null)
            {
                return _command.ExecuteReader(behavior);
            }

            var queryInfo = GetQueryInfo();
            var strategy = _commandInfo.Policy.GetCachingMode(queryInfo);
            var cacheKey = _commandInfo.Cache.GetCacheKey(GetKey(), _commandInfo.AffectedEntitySets, strategy);

            object cachedRes;
            if (_commandInfo.Cache.GetItem(cacheKey, out cachedRes))
                return ((DataReaderResult) cachedRes).CreateReader();

            var reader = _command.ExecuteReader(behavior);

            if (reader.RecordsAffected > 0)
                return reader;  // Queries that modify anything are never cached.

            // Check if cacheable.
            if (!_commandInfo.Policy.CanBeCached(queryInfo))
                return reader;

            // Read into memory.
            var res = new DataReaderResult(reader);

            // Check if specific row count is cacheable.
            if (!_commandInfo.Policy.CanBeCached(queryInfo, res.RowCount))
                return res.CreateReader();

            PutResultToCache(cacheKey, res, queryInfo);

            return res.CreateReader();
        }

        /// <summary>
        /// Invalidates the cache.
        /// </summary>
        private void InvalidateCache()
        {
            _commandInfo.TxHandler.InvalidateCache(_commandInfo.AffectedEntitySets, Transaction);
        }

        /** <inheritDoc /> */
        public override int ExecuteNonQuery()
        {
            var res = _command.ExecuteNonQuery();

            // Invalidate AFTER updating the data.
            if (_commandInfo.IsModification)
            {
                InvalidateCache();
            }

            return res;
        }

        /** <inheritDoc /> */
        [ExcludeFromCodeCoverage]
        public override object ExecuteScalar()
        {
            // This method is never used by EntityFramework.
            // Even EntityCommand.ExecuteScalar goes to ExecuteDbDataReader.
            return _command.ExecuteScalar();
        }

        /// <summary>
        /// Puts the result to cache.
        /// </summary>
        private void PutResultToCache(DbCacheKey key, object result, DbQueryInfo queryInfo)
        {
            var expiration = _commandInfo.Policy != null
                ? _commandInfo.Policy.GetExpirationTimeout(queryInfo)
                : TimeSpan.MaxValue;

            _commandInfo.Cache.PutItem(key, result, expiration);
        }

        /// <summary>
        /// Gets the cache key.
        /// </summary>
        private string GetKey()
        {
            if (string.IsNullOrEmpty(CommandText))
                throw new NotSupportedException("Ignite Entity Framework Caching " +
                                                "requires non-empty DbCommand.CommandText.");

            var sb = new StringBuilder();

            sb.AppendFormat("{0}:{1}|", Connection.Database, CommandText);

            foreach (DbParameter param in Parameters)
                sb.AppendFormat("{0}={1},", param.ParameterName, param.Value);

            return sb.ToString();
        }

        /// <summary>
        /// Gets the query information.
        /// </summary>
        private DbQueryInfo GetQueryInfo()
        {
            return new DbQueryInfo(_commandInfo.AffectedEntitySets, CommandText, DbParameterCollection);
        }
    }
}
