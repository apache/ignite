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
    using System.Threading;

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
                Console.WriteLine("ExecuteReaderModify... | {0}", Thread.CurrentThread.ManagedThreadId);

                var readerRes = new DataReaderResult(_command.ExecuteReader(behavior));
                var dbReader = readerRes.CreateReader();

                Console.WriteLine("ExecuteReaderModify done | {0}", Thread.CurrentThread.ManagedThreadId);

                InvalidateCache();

                return dbReader;
            }

            if (Transaction != null)
            {
                return _command.ExecuteReader(behavior);
            }

            var queryInfo = GetQueryInfo();
            var strategy = _commandInfo.Policy.GetCachingStrategy(queryInfo);
            var cacheKey = _commandInfo.Cache.GetCacheKey(GetKey(), _commandInfo.AffectedEntitySets, strategy);

            //Console.WriteLine("Got cache key: {0} | {1}", cacheKey.GetStringKey(), Thread.CurrentThread.ManagedThreadId);

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
            Console.WriteLine("ExecuteReader... | {0}", Thread.CurrentThread.ManagedThreadId);
            var res = new DataReaderResult(reader);
            Console.WriteLine("ExecuteReader done | {0}", Thread.CurrentThread.ManagedThreadId);

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
            // Invalidate after connection is closed
            var conn = _command.Connection;

            conn.StateChange -= ConnectionOnStateChange;
            conn.StateChange += ConnectionOnStateChange;
        }

        /// <summary>
        /// Handles the OnStateChange event of the Connection.
        /// </summary>
        /// <param name="sender">The source of the event.</param>
        /// <param name="stateChangeEventArgs">The <see cref="StateChangeEventArgs"/> instance 
        /// containing the event data.</param>
        private void ConnectionOnStateChange(object sender, StateChangeEventArgs stateChangeEventArgs)
        {
            // Invalidate sets on connection close.
            if (stateChangeEventArgs.CurrentState == ConnectionState.Closed)
            {
                _commandInfo.Cache.InvalidateSets(_commandInfo.AffectedEntitySets);

                _command.Connection.StateChange -= ConnectionOnStateChange;
            }
        }

        /** <inheritDoc /> */
        public override int ExecuteNonQuery()
        {
            Console.WriteLine("ExecuteNonQuery... | {0}", Thread.CurrentThread.ManagedThreadId);

            var res = _command.ExecuteNonQuery();

            Console.WriteLine("ExecuteNonQuery done {0} | {1}", GetKey(), Thread.CurrentThread.ManagedThreadId);

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

            _commandInfo.Cache.PutItemAsync(key, result, expiration);
        }

        /// <summary>
        /// Gets the cache key.
        /// </summary>
        private string GetKey()
        {
            if (string.IsNullOrEmpty(CommandText))
                throw new NotSupportedException("Ignite Entity Framework Caching " +
                                                "requires non-empty DbCommand.CommandText.");

            // TODO: Remove LINQ, use single StringBuilder!
            var parameters = string.Join("|",
                Parameters.Cast<DbParameter>().Select(x => x.ParameterName + "=" + x.Value));

            return string.Format("{0}:{1}|{2}", Connection.Database, CommandText, parameters);
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
