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

    internal class DbProviderServicesProxy : DbProviderServices
    {
        private readonly IgniteEntityFrameworkCachingPolicy _policy;
        private readonly DbProviderServices _services;
        private readonly TransactionInterceptor _interceptor;

        public DbProviderServicesProxy(DbProviderServices services, TransactionInterceptor txInterceptor, IgniteEntityFrameworkCachingPolicy policy)
        {
            _services = services;
            _interceptor = txInterceptor;
            _policy = policy;
        }


        protected override DbCommandDefinition CreateDbCommandDefinition(DbProviderManifest providerManifest, DbCommandTree commandTree)
        {
            return _services.CreateCommandDefinition(providerManifest, commandTree);
        }

        protected override string GetDbProviderManifestToken(DbConnection connection)
        {
            return _services.GetProviderManifestToken(connection);
        }

        protected override DbProviderManifest GetDbProviderManifest(string manifestToken)
        {
            return _services.GetProviderManifest(manifestToken);
        }

        public override void RegisterInfoMessageHandler(DbConnection connection, Action<string> handler)
        {
            _services.RegisterInfoMessageHandler(connection, handler);
        }

        public override DbCommandDefinition CreateCommandDefinition(DbCommand prototype)
        {
            return _services.CreateCommandDefinition(prototype);
        }

        protected override DbSpatialDataReader GetDbSpatialDataReader(DbDataReader fromReader, string manifestToken)
        {
            return _services.GetSpatialDataReader(fromReader, manifestToken);
        }

        protected override DbSpatialServices DbGetSpatialServices(string manifestToken)
        {
            return _services.GetSpatialServices(manifestToken);
        }
        protected override void SetDbParameterValue(DbParameter parameter, TypeUsage parameterType, object value)
        {
            _services.SetParameterValue(parameter, parameterType, value);
        }

        protected override string DbCreateDatabaseScript(string providerManifestToken, StoreItemCollection storeItemCollection)
        {
            return _services.CreateDatabaseScript(providerManifestToken, storeItemCollection);
        }

        protected override void DbCreateDatabase(DbConnection connection, int? commandTimeout, StoreItemCollection storeItemCollection)
        {
            _services.CreateDatabase(connection, commandTimeout, storeItemCollection);
        }

        protected override bool DbDatabaseExists(DbConnection connection, int? commandTimeout, StoreItemCollection storeItemCollection)
        {
            return _services.DatabaseExists(connection, commandTimeout, storeItemCollection);
        }

        protected override void DbDeleteDatabase(DbConnection connection, int? commandTimeout, StoreItemCollection storeItemCollection)
        {
            _services.DeleteDatabase(connection, commandTimeout, storeItemCollection);
        }

        public override object GetService(Type type, object key)
        {
            return _services.GetService(type, key);
        }

        public override IEnumerable<object> GetServices(Type type, object key)
        {
            return _services.GetServices(type, key);
        }
    }
}
