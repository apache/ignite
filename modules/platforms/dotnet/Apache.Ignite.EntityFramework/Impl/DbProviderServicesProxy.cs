/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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
    using System.Diagnostics.CodeAnalysis;

    /// <summary>
    /// DbProviderServices proxy which substitutes custom commands.
    /// </summary>
    internal class DbProviderServicesProxy : DbProviderServices
    {
        /** */
        private static readonly DbCachingPolicy DefaultPolicy = new DbCachingPolicy();

        /** */
        private readonly IDbCachingPolicy _policy;
        
        /** */
        private readonly DbProviderServices _services;
        
        /** */
        private readonly DbCache _cache;

        /** */
        private readonly DbTransactionInterceptor _txHandler;

        /// <summary>
        /// Initializes a new instance of the <see cref="DbProviderServicesProxy"/> class.
        /// </summary>
        /// <param name="services">The services.</param>
        /// <param name="policy">The policy.</param>
        /// <param name="cache">The cache.</param>
        /// <param name="txHandler">Transaction handler.</param>
        public DbProviderServicesProxy(DbProviderServices services, IDbCachingPolicy policy, DbCache cache, 
            DbTransactionInterceptor txHandler)
        {
            Debug.Assert(services != null);
            Debug.Assert(cache != null);
            Debug.Assert(txHandler != null);

            var proxy = services as DbProviderServicesProxy;
            _services = proxy != null ? proxy._services : services;

            _policy = policy ?? DefaultPolicy;
            _cache = cache;
            _txHandler = txHandler;
        }

        /** <inheritDoc /> */
        [ExcludeFromCodeCoverage]
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
                new DbCommandInfo(commandTree, _cache, _policy, _txHandler));
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
        [ExcludeFromCodeCoverage]
        public override void RegisterInfoMessageHandler(DbConnection connection, Action<string> handler)
        {
            _services.RegisterInfoMessageHandler(connection, handler);
        }

        /** <inheritDoc /> */
        [ExcludeFromCodeCoverage]
        protected override DbSpatialDataReader GetDbSpatialDataReader(DbDataReader fromReader, string manifestToken)
        {
            return _services.GetSpatialDataReader(fromReader, manifestToken);
        }

        /** <inheritDoc /> */
        [ExcludeFromCodeCoverage]
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
        [ExcludeFromCodeCoverage]
        public override object GetService(Type type, object key)
        {
            return _services.GetService(type, key);
        }

        /** <inheritDoc /> */
        [ExcludeFromCodeCoverage]
        public override IEnumerable<object> GetServices(Type type, object key)
        {
            return _services.GetServices(type, key);
        }
    }
}
