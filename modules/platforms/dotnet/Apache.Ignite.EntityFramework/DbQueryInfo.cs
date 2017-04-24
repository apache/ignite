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

namespace Apache.Ignite.EntityFramework
{
    using System.Collections.Generic;
    using System.Data.Common;
    using System.Data.Entity.Core.Metadata.Edm;
    using System.Diagnostics;

    /// <summary>
    /// Query info.
    /// </summary>
    public class DbQueryInfo
    {
        /** */
        private readonly ICollection<EntitySetBase> _affectedEntitySets;

        /** */
        private readonly string _commandText;
        
        /** */
        private readonly DbParameterCollection _parameters;

        /// <summary>
        /// Initializes a new instance of the <see cref="DbQueryInfo"/> class.
        /// </summary>
        internal DbQueryInfo(ICollection<EntitySetBase> affectedEntitySets, string commandText, 
            DbParameterCollection parameters)
        {
            Debug.Assert(affectedEntitySets != null);
            Debug.Assert(commandText != null);
            Debug.Assert(parameters != null);

            _affectedEntitySets = affectedEntitySets;
            _commandText = commandText;
            _parameters = parameters;
        }

        /// <summary>
        /// Gets the affected entity sets.
        /// </summary>
        public ICollection<EntitySetBase> AffectedEntitySets
        {
            get { return _affectedEntitySets; }
        }

        /// <summary>
        /// Gets the command text.
        /// </summary>
        public string CommandText
        {
            get { return _commandText; }
        }

        /// <summary>
        /// Gets the parameters.
        /// </summary>
        public DbParameterCollection Parameters
        {
            get { return _parameters; }
        }
    }
}