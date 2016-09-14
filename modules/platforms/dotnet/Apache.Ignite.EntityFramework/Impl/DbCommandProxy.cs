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
    using System.Linq;

    /// <summary>
    /// Command proxy.
    /// </summary>
    internal class DbCommandProxy : DbCommand
    {
        /** */
        private readonly DbCommand _command;

        /** */
        private readonly DbCommandInfo _info;

        /// <summary>
        /// Initializes a new instance of the <see cref="DbCommandProxy"/> class.
        /// </summary>
        public DbCommandProxy(DbCommand command, DbCommandInfo info)
        {
            Debug.Assert(command != null);
            Debug.Assert(info != null);

            _command = command;
            _info = info;
        }

        /// <summary>
        /// Gets the inner command.
        /// </summary>
        public DbCommand InnerCommand
        {
            get { return _command; }
        }

        /// <summary>
        /// Gets the command information.
        /// </summary>
        public DbCommandInfo CommandInfo
        {
            get { return _info; }
        }

        /** <inheritDoc /> */
        public override void Prepare()
        {
            _command.Prepare();
        }

        /** <inheritDoc /> */
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
        public override bool DesignTimeVisible
        {
            get { return _command.DesignTimeVisible; }
            set { _command.DesignTimeVisible = value; }
        }

        /** <inheritDoc /> */
        public override void Cancel()
        {
            _command.Cancel();
        }

        /** <inheritDoc /> */
        protected override DbParameter CreateDbParameter()
        {
            return _command.CreateParameter();
        }

        /** <inheritDoc /> */
        protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
        {
            if (_info.IsModification)
            {
                _info.Cache.InvalidateSets(_info.AffectedEntitySets);

                return _command.ExecuteReader(behavior);
            }

            if (Transaction != null)
            {
                return _command.ExecuteReader(behavior);
            }

            var cacheKey = GetKey();
            var queryInfo = GetQueryInfo();
            var strategy = _info.Policy.GetCachingStrategy(queryInfo);

            object cachedRes;
            if (_info.Cache.GetItem(cacheKey, _info.AffectedEntitySets, strategy, out cachedRes))
                return ((DataReaderResult) cachedRes).CreateReader();

            var reader = _command.ExecuteReader(behavior);

            if (reader.RecordsAffected > 0)
                return reader;  // Queries that modify anything are never cached.

            // Check if cacheable.
            if (!_info.Policy.CanBeCached(queryInfo))
                return reader;

            // Read into memory.
            var res = new DataReaderResult(reader);

            // Check if specific row count is cacheable.
            if (!_info.Policy.CanBeCached(queryInfo, res.RowCount))
                return res.CreateReader();

            PutResultToCache(cacheKey, res, strategy, queryInfo);

            return res.CreateReader();
        }

        /** <inheritDoc /> */
        public override int ExecuteNonQuery()
        {
            if (_info.IsModification)
                _info.Cache.InvalidateSets(_info.AffectedEntitySets);

            return _command.ExecuteNonQuery();
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
        private void PutResultToCache(string key, object result, DbCachingStrategy strategy, DbQueryInfo queryInfo)
        {
            var expiration = _info.Policy != null
                ? _info.Policy.GetExpirationTimeout(queryInfo)
                : TimeSpan.MaxValue;

            _info.Cache.PutItem(key, result, _info.AffectedEntitySets, strategy, expiration);
        }

        /// <summary>
        /// Gets the cache key.
        /// </summary>
        private string GetKey()
        {
            if (string.IsNullOrEmpty(CommandText))
                throw new NotSupportedException("Ignite Entity Framework Caching " +
                                                "requires non-empty DbCommand.CommandText.");

            var parameters = string.Join("|",
                Parameters.Cast<DbParameter>().Select(x => x.ParameterName + "=" + x.Value));

            return string.Format("{0}:{1}|{2}", Connection.Database, CommandText, parameters);
        }

        /// <summary>
        /// Gets the query information.
        /// </summary>
        private DbQueryInfo GetQueryInfo()
        {
            return new DbQueryInfo(_info.AffectedEntitySets, CommandText, DbParameterCollection);
        }
    }
}
