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
    using System.Diagnostics;
    using System.Linq;

    /// <summary>
    /// Query data holder.
    /// </summary>
    internal class QueryData
    {
        /** */
        private readonly ICollection<object> _parameters;
        
        /** */
        private readonly string _queryText;

        /// <summary>
        /// Initializes a new instance of the <see cref="QueryData"/> class.
        /// </summary>
        /// <param name="queryText">The query text.</param>
        /// <param name="parameters">The parameters.</param>
        public QueryData(string queryText, ICollection<object> parameters)
        {
            Debug.Assert(queryText != null);
            Debug.Assert(parameters != null);

            _queryText = queryText;
            _parameters = parameters;
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
        /// Returns a <see cref="string" /> that represents this instance.
        /// </summary>
        /// <returns>
        /// A <see cref="string" /> that represents this instance.
        /// </returns>
        public override string ToString()
        {
            return string.Format("SQL Query [Text={0}, Parameters={1}]", QueryText,
                string.Join(", ", Parameters.Select(x => x == null ? "null" : x.ToString())));
        }
    }
}