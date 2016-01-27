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
    using Remotion.Linq;
    using Remotion.Linq.Clauses;

    /// <summary>
    /// Fields query visitor, respects "SELECT" clause.
    /// </summary>
    internal class CacheFieldsQueryModelVisitor : CacheQueryModelVisitor
    {
        /// <summary>
        /// Generates the query.
        /// </summary>
        public new static QueryData GenerateQuery(QueryModel queryModel)
        {
            var visitor = new CacheFieldsQueryModelVisitor();

            visitor.VisitQueryModel(queryModel);

            return visitor.GetQuery();
        }

        /** <inheritdoc /> */
        public override void VisitSelectClause(SelectClause selectClause, QueryModel queryModel)
        {
            base.VisitSelectClause(selectClause, queryModel);

            // Only queries starting with 'SELECT *' are supported or use SqlFieldsQuery instead: select _key, _val from LinqPerson where (age1 < ?)

            var selectSql = GetSqlExpression(selectClause.Selector);

            Builder.Insert(0, string.Format("select {0} ", selectSql.QueryText));
        }
    }
}