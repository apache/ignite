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
    using System.Diagnostics;
    using System.Linq;
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
        public new static QueryData GenerateQuery(QueryModel queryModel, string schemaName)
        {
            Debug.Assert(queryModel != null);
            Debug.Assert(!string.IsNullOrEmpty(schemaName));

            var visitor = new CacheFieldsQueryModelVisitor(schemaName);

            visitor.VisitQueryModel(queryModel);

            var resultBuilder = new StringBuilder("select ");
            int parenCount = 0;

            foreach (var op in queryModel.ResultOperators.Reverse())
            {
                if (op is CountResultOperator)
                {
                    resultBuilder.Append("count (");
                    parenCount++;
                }
                else if (op is SumResultOperator)
                {
                    resultBuilder.Append("sum (");
                    parenCount++;
                }
                else if (op is MinResultOperator)
                {
                    resultBuilder.Append("min (");
                    parenCount++;
                }
                else if (op is MaxResultOperator)
                {
                    resultBuilder.Append("max (");
                    parenCount++;
                }
                else if (op is DistinctResultOperator)
                    resultBuilder.Append("distinct ");
                else if (op is FirstResultOperator || op is SingleResultOperator)
                    resultBuilder.Append("top 1 ");
                else if (op is TakeResultOperator)
                    resultBuilder.AppendFormat("top {0} ", ((TakeResultOperator) op).Count);
                else
                    throw new NotSupportedException("Operator is not supported: " + op);
            }

            resultBuilder.Append(visitor.GetSqlExpression(queryModel.SelectClause.Selector, parenCount > 0).QueryText);

            resultBuilder.Append(')', parenCount);

            var queryData = visitor.GetQuery();

            var queryText = resultBuilder.Append(" ").Append(queryData.QueryText).ToString();

            return new QueryData(queryText, queryData.Parameters, true);
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="CacheFieldsQueryModelVisitor"/> class.
        /// </summary>
        /// <param name="schemaName">Name of the schema.</param>
        private CacheFieldsQueryModelVisitor(string schemaName) : base(schemaName)
        {
            // No-op.
        }
    }
}