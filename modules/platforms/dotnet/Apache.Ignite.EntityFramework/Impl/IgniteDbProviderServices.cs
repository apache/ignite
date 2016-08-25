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
    using System.Data.Common;
    using System.Data.Entity.Core.Common;
    using System.Data.Entity.Core.Common.CommandTrees;

    internal class IgniteDbProviderServices : DbProviderServices
    {
        private readonly IgniteEntityFrameworkCachingPolicy _policy;

        public IgniteDbProviderServices(DbProviderServices services, TransactionInterceptor transactionInterceptor, IgniteEntityFrameworkCachingPolicy policy)
        {
            _policy = policy;
        }

        protected override DbCommandDefinition CreateDbCommandDefinition(DbProviderManifest providerManifest, DbCommandTree commandTree)
        {
            throw new System.NotImplementedException();
        }

        protected override string GetDbProviderManifestToken(DbConnection connection)
        {
            throw new System.NotImplementedException();
        }

        protected override DbProviderManifest GetDbProviderManifest(string manifestToken)
        {
            throw new System.NotImplementedException();
        }
    }
}
