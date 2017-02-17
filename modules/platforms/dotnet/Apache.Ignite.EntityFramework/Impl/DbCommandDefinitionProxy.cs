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
