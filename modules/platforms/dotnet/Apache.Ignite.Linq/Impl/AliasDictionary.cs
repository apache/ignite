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
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Linq.Expressions;
    using System.Text;
    using Remotion.Linq.Clauses;
    using Remotion.Linq.Clauses.Expressions;

    /// <summary>
    /// Alias dictionary.
    /// </summary>
    internal class AliasDictionary
    {
        /** */
        private int _tableAliasIndex;

        /** */
        private Dictionary<IQuerySource, string> _tableAliases = new Dictionary<IQuerySource, string>();

        /** */
        private int _fieldAliasIndex;

        /** */
        private readonly Dictionary<Expression, string> _fieldAliases = new Dictionary<Expression, string>();

        /** */
        private readonly Stack<Dictionary<IQuerySource, string>> _stack 
            = new Stack<Dictionary<IQuerySource, string>>();

        /// <summary>
        /// Pushes current aliases to stack.
        /// </summary>
        /// <param name="copyAliases">Flag indicating that current aliases should be copied</param>
        public void Push(bool copyAliases)
        {
            _stack.Push(_tableAliases);

            _tableAliases = copyAliases
                ? _tableAliases.ToDictionary(p => p.Key, p => p.Value)
                : new Dictionary<IQuerySource, string>();
        }

        /// <summary>
        /// Pops current aliases from stack.
        /// </summary>
        public void Pop()
        {
            _tableAliases = _stack.Pop();
        }

        /// <summary>
        /// Gets the table alias.
        /// </summary>
        public string GetTableAlias(Expression expression)
        {
            Debug.Assert(expression != null);

            return GetTableAlias(GetQuerySource(expression));
        }

        /// <summary>
        /// Gets the table alias.
        /// </summary>
        public string GetTableAlias(IFromClause fromClause)
        {
            return GetTableAlias(GetQuerySource(fromClause.FromExpression) ?? fromClause);
        }

        /// <summary>
        /// Gets the table alias.
        /// </summary>
        public string GetTableAlias(JoinClause joinClause)
        {
            return GetTableAlias(GetQuerySource(joinClause.InnerSequence) ?? joinClause);
        }

        /// <summary>
        /// Gets the table alias.
        /// </summary>
        private string GetTableAlias(IQuerySource querySource)
        {
            Debug.Assert(querySource != null);

            string alias;

            if (!_tableAliases.TryGetValue(querySource, out alias))
            {
                alias = "_T" + _tableAliasIndex++;

                _tableAliases[querySource] = alias;
            }

            return alias;
        }

        /// <summary>
        /// Gets the fields alias.
        /// </summary>
        public string GetFieldAlias(Expression expression)
        {
            Debug.Assert(expression != null);

            var referenceExpression = ExpressionWalker.GetQuerySourceReference(expression);

            return GetFieldAlias(referenceExpression);
        }

        /// <summary>
        /// Gets the fields alias.
        /// </summary>
        private string GetFieldAlias(QuerySourceReferenceExpression querySource)
        {
            Debug.Assert(querySource != null);

            string alias;

            if (!_fieldAliases.TryGetValue(querySource, out alias))
            {
                alias = "F" + _fieldAliasIndex++;

                _fieldAliases[querySource] = alias;
            }

            return alias;
        }

        /// <summary>
        /// Appends as clause.
        /// </summary>
        public StringBuilder AppendAsClause(StringBuilder builder, IFromClause clause)
        {
            Debug.Assert(builder != null);
            Debug.Assert(clause != null);

            var queryable = ExpressionWalker.GetCacheQueryable(clause);
            var tableName = ExpressionWalker.GetTableNameWithSchema(queryable);

            builder.AppendFormat("{0} as {1}", tableName, GetTableAlias(clause));

            return builder;
        }

        /// <summary>
        /// Gets the query source.
        /// <para />
        /// TODO: This logic is very similar to <see cref="ExpressionWalker.GetProjectedMember"/>
        /// We should probably reuse the traversal logic.
        /// </summary>
        private static IQuerySource GetQuerySource(Expression expression, MemberExpression memberHint = null)
        {
            if (memberHint != null)
            {
                var newExpr = expression as NewExpression;

                if (newExpr != null)
                {
                    for (var i = 0; i < newExpr.Members.Count; i++)
                    {
                        var member = newExpr.Members[i];

                        if (member == memberHint.Member)
                        {
                            return GetQuerySource(newExpr.Arguments[i]);
                        }
                    }
                }
            }

            var subQueryExp = expression as SubQueryExpression;

            if (subQueryExp != null)
            {
                var source = GetQuerySource(subQueryExp.QueryModel.SelectClause.Selector, memberHint);
                if (source != null)
                {
                    return source;
                }

                // TODO: Can we have a single exit point to generify this method?
                return subQueryExp.QueryModel.MainFromClause;
            }

            var srcRefExp = expression as QuerySourceReferenceExpression;

            if (srcRefExp != null)
            {
                var fromSource = srcRefExp.ReferencedQuerySource as IFromClause;

                if (fromSource != null)
                {
                    var source = GetQuerySource(fromSource.FromExpression, memberHint);
                    if (source != null)
                    {
                        return source;
                    }

                    return fromSource;
                }

                var joinSource = srcRefExp.ReferencedQuerySource as JoinClause;

                if (joinSource != null)
                    return GetQuerySource(joinSource.InnerSequence, memberHint) ?? joinSource;

                throw new NotSupportedException("Unexpected query source: " + srcRefExp.ReferencedQuerySource);
            }

            var memberExpr = expression as MemberExpression;

            if (memberExpr != null)
                return GetQuerySource(memberExpr.Expression, memberExpr);

            return null;
        }
    }
}
