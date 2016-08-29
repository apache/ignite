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

        public DbCommandProxy(DbCommand command, DbCommandInfo info)
        {
            Debug.Assert(command != null);
            Debug.Assert(info != null);

            _command = command;
            _info = info;
        }

        public DbCommand InnerCommand
        {
            get { return _command; }
        }

        public DbCommandInfo CommandInfo
        {
            get { return _info; }
        }

        public override void Prepare()
        {
            _command.Prepare();
        }

        public override string CommandText
        {
            get { return _command.CommandText; }
            set { _command.CommandText = value; }
        }

        public override int CommandTimeout
        {
            get { return _command.CommandTimeout; }
            set { _command.CommandTimeout = value; }
        }

        public override CommandType CommandType
        {
            get { return _command.CommandType; }
            set { _command.CommandType = value; }
        }

        public override UpdateRowSource UpdatedRowSource
        {
            get { return _command.UpdatedRowSource; }
            set { _command.UpdatedRowSource = value; }
        }

        protected override DbConnection DbConnection
        {
            get { return _command.Connection; }
            set { _command.Connection = value; }
        }

        protected override DbParameterCollection DbParameterCollection
        {
            get { return _command.Parameters; }
        }

        protected override DbTransaction DbTransaction
        {
            get { return _command.Transaction; }
            set { _command.Transaction = value; }
        }

        public override bool DesignTimeVisible
        {
            get { return _command.DesignTimeVisible; }
            set { _command.DesignTimeVisible = value; }
        }

        public override void Cancel()
        {
            _command.Cancel();
        }

        protected override DbParameter CreateDbParameter()
        {
            return _command.CreateParameter();
        }

        protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
        {
            if (_info.IsModification)
            {
                _info.Cache.InvalidateSets(_info.AffectedEntitySets);

                return _command.ExecuteReader(behavior);
            }

            // TODO: Check policy

            var cacheKey = GetKey();

            object cachedRes;
            if (_info.Cache.GetItem(cacheKey, _info.AffectedEntitySets, out cachedRes))
                return ((DataReaderResult) cachedRes).CreateReader();

            var reader = _command.ExecuteReader(behavior);

            if (reader.RecordsAffected > 0)
                return reader;  // Queries that modify anything are never cached.

            // Check if cacheable
            var policy = _info.Policy;

            if (policy != null && !policy.CanBeCached(_info.AffectedEntitySets, CommandText, Parameters))
                return reader;

            // Read into memory.
            var res = new DataReaderResult(reader);

            // Check if specific row count is cacheable.
            if (policy != null && !policy.CanBeCached(_info.AffectedEntitySets, CommandText, Parameters, res.RowCount))
                return res.CreateReader();

            var expiration = policy != null
                ? policy.GetExpirationTimeout(_info.AffectedEntitySets, CommandText, Parameters)
                : TimeSpan.MaxValue;

            _info.Cache.PutItem(cacheKey, res, _info.AffectedEntitySets, expiration);

            return res.CreateReader();
        }

        private string GetKey()
        {
            if (string.IsNullOrEmpty(CommandText))
                throw new NotSupportedException("Ignite Entity Framework Caching " +
                                                "requires non-empty DbCommand.CommandText.");

            var parameters = string.Join("|",
                Parameters.Cast<DbParameter>().Select(x => x.ParameterName + "=" + x.Value));

            return string.Format("{0}:{1}|{2}", Connection.Database, CommandText, parameters);
        }

#if !NET40
        // TODO: ExecuteDbDataReaderAsync in newer frameworks? How does this stack up?
#endif

        public override int ExecuteNonQuery()
        {
            // TODO: Invalidate sets
            return _command.ExecuteNonQuery();
        }

        public override object ExecuteScalar()
        {
            return _command.ExecuteScalar();
        }
    }
}
