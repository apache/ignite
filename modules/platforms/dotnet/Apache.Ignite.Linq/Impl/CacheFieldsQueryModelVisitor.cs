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
    using System;
    using System.Text;
    using Remotion.Linq;
    using Remotion.Linq.Clauses.ResultOperators;

    /// <summary>
    /// Fields query visitor, respects "SELECT" clause.
    /// </summary>
    internal class CacheFieldsQueryModelVisitor : CacheQueryModelVisitor
    {
        /// <summary>
        /// Generates the query.
        /// </summary>
        public new static QueryData GenerateQuery(QueryModel queryModel, string tableName)
        {
            var visitor = new CacheFieldsQueryModelVisitor(tableName);

            visitor.VisitQueryModel(queryModel);

            var resultBuilder = new StringBuilder("select ");
            int parenCount = 0;

            foreach (var resultOperator in queryModel.ResultOperators)  // TODO: Reverse?
            {
                if (resultOperator is CountResultOperator)
                {
                    resultBuilder.Append("count (");
                    parenCount++;
                }
                else if (resultOperator is SumResultOperator)
                {
                    resultBuilder.Append("sum (");
                    parenCount++;
                }
                else if (resultOperator is MinResultOperator)
                {
                    resultBuilder.Append("min (");
                    parenCount++;
                }
                else if (resultOperator is MaxResultOperator)
                {
                    resultBuilder.Append("max (");
                    parenCount++;
                }
                else if (resultOperator is FirstResultOperator)
                    visitor.Builder.Append("limit 1 ");
                else
                    throw new NotSupportedException("TODO: " + resultOperator); // Min, max, etc
            }

            resultBuilder.Append(GetSqlExpression(queryModel.SelectClause.Selector).QueryText);

            resultBuilder.Append(')', parenCount);

            var queryData = visitor.GetQuery();

            var queryText = resultBuilder.Append(" ").Append(queryData.QueryText).ToString();

            return new QueryData(queryText, queryData.Parameters, true);
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="CacheFieldsQueryModelVisitor"/> class.
        /// </summary>
        /// <param name="tableName">Name of the table.</param>
        private CacheFieldsQueryModelVisitor(string tableName) : base(tableName)
        {
            // No-op.
        }
    }
}