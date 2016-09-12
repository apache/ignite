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

#pragma warning disable 618, 672
namespace Apache.Ignite.EntityFramework.Impl
{
    using System;
    using System.Collections.Generic;
    using System.Data.Common;
    using System.Data.Entity.Core.Common;
    using System.Data.Entity.Core.Common.CommandTrees;
    using System.Data.Entity.Core.Metadata.Edm;
    using System.Data.Entity.Spatial;
    using System.Diagnostics;

    /// <summary>
    /// DbProviderServices proxy which substitutes custom commands.
    /// </summary>
    internal class DbProviderServicesProxy : DbProviderServices
    {
        /** */
        private readonly DbCachingPolicy _policy;
        
        /** */
        private readonly DbProviderServices _services;
        
        /** */
        private readonly DbCache _cache;

        /// <summary>
        /// Initializes a new instance of the <see cref="DbProviderServicesProxy"/> class.
        /// </summary>
        /// <param name="services">The services.</param>
        /// <param name="policy">The policy.</param>
        /// <param name="cache">The cache.</param>
        public DbProviderServicesProxy(DbProviderServices services, DbCachingPolicy policy, DbCache cache)
        {
            Debug.Assert(services != null);
            Debug.Assert(cache != null);

            var proxy = services as DbProviderServicesProxy;
            _services = proxy != null ? proxy._services : services;

            _policy = policy;
            _cache = cache;
        }

        /** <inheritDoc /> */
        public override DbCommandDefinition CreateCommandDefinition(DbCommand prototype)
        {
            var proxy = prototype as DbCommandProxy;

            if (proxy == null)
                return _services.CreateCommandDefinition(prototype);

            return new DbCommandDefinitionProxy(_services.CreateCommandDefinition(proxy.InnerCommand), 
                proxy.CommandInfo);
        }

        /** <inheritDoc /> */
        protected override DbCommandDefinition CreateDbCommandDefinition(DbProviderManifest providerManifest, 
            DbCommandTree commandTree)
        {
            return new DbCommandDefinitionProxy(_services.CreateCommandDefinition(providerManifest, commandTree), 
                new DbCommandInfo(commandTree, _cache, _policy));
        }

        /** <inheritDoc /> */
        protected override string GetDbProviderManifestToken(DbConnection connection)
        {
            return _services.GetProviderManifestToken(connection);
        }

        /** <inheritDoc /> */
        protected override DbProviderManifest GetDbProviderManifest(string manifestToken)
        {
            return _services.GetProviderManifest(manifestToken);
        }

        /** <inheritDoc /> */
        public override void RegisterInfoMessageHandler(DbConnection connection, Action<string> handler)
        {
            _services.RegisterInfoMessageHandler(connection, handler);
        }

        /** <inheritDoc /> */
        protected override DbSpatialDataReader GetDbSpatialDataReader(DbDataReader fromReader, string manifestToken)
        {
            return _services.GetSpatialDataReader(fromReader, manifestToken);
        }

        /** <inheritDoc /> */
        protected override DbSpatialServices DbGetSpatialServices(string manifestToken)
        {
            return _services.GetSpatialServices(manifestToken);
        }
        protected override void SetDbParameterValue(DbParameter parameter, TypeUsage parameterType, object value)
        {
            _services.SetParameterValue(parameter, parameterType, value);
        }

        /** <inheritDoc /> */
        protected override string DbCreateDatabaseScript(string providerManifestToken, StoreItemCollection storeItemCollection)
        {
            return _services.CreateDatabaseScript(providerManifestToken, storeItemCollection);
        }

        /** <inheritDoc /> */
        protected override void DbCreateDatabase(DbConnection connection, int? commandTimeout, StoreItemCollection storeItemCollection)
        {
            _services.CreateDatabase(connection, commandTimeout, storeItemCollection);
        }

        /** <inheritDoc /> */
        protected override bool DbDatabaseExists(DbConnection connection, int? commandTimeout, StoreItemCollection storeItemCollection)
        {
            return _services.DatabaseExists(connection, commandTimeout, storeItemCollection);
        }

        /** <inheritDoc /> */
        protected override void DbDeleteDatabase(DbConnection connection, int? commandTimeout, StoreItemCollection storeItemCollection)
        {
            _services.DeleteDatabase(connection, commandTimeout, storeItemCollection);
        }

        /** <inheritDoc /> */
        public override object GetService(Type type, object key)
        {
            return _services.GetService(type, key);
        }

        /** <inheritDoc /> */
        public override IEnumerable<object> GetServices(Type type, object key)
        {
            return _services.GetServices(type, key);
        }
    }
}
