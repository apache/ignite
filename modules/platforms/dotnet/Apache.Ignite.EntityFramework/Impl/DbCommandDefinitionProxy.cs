/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
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
    using System.Diagnostics;

    internal class DbCommandDefinitionProxy : DbCommandDefinition
    {
        /** */
        private readonly DbCommandDefinition _definition;

        /** */
        private readonly DbCommandInfo _info;

        /// <summary>
        /// Initializes a new instance of the <see cref="DbCommandDefinitionProxy"/> class.
        /// </summary>
        public DbCommandDefinitionProxy(DbCommandDefinition definition, DbCommandInfo info)
        {
            Debug.Assert(definition != null);

            var proxy = definition as DbCommandDefinitionProxy;
            _definition = proxy != null ? proxy._definition : definition;

            _info = info;
        }

        /** <inheritDoc /> */
        public override DbCommand CreateCommand()
        {
            return new DbCommandProxy(_definition.CreateCommand(), _info);
        }
    }
}
