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

namespace Apache.Ignite.Linq.Impl
{
    using System.Collections.Generic;

    /// <summary>
    /// Query data DTO.
    /// </summary>
    internal class QueryData
    {
        /** */
        private readonly ICollection<object> _parameters;
        
        /** */
        private readonly string _queryText;
        
        /** */
        private readonly bool _isFieldsQuery;

        /// <summary>
        /// Initializes a new instance of the <see cref="QueryData"/> class.
        /// </summary>
        /// <param name="queryText">The query text.</param>
        /// <param name="parameters">The parameters.</param>
        /// <param name="isFieldsQuery">Fields query flag.</param>
        public QueryData(string queryText, ICollection<object> parameters, bool isFieldsQuery = false)
        {
            _queryText = queryText;
            _parameters = parameters;
            _isFieldsQuery = isFieldsQuery;
        }

        /// <summary>
        /// Gets the parameters.
        /// </summary>
        public ICollection<object> Parameters
        {
            get { return _parameters; }
        }

        /// <summary>
        /// Gets the query text.
        /// </summary>
        public string QueryText
        {
            get { return _queryText; }
        }

        /// <summary>
        /// Gets a value indicating whether this instance is fields query.
        /// </summary>
        public bool IsFieldsQuery
        {
            get { return _isFieldsQuery; }
        }
    }
}