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

namespace Apache.Ignite.Linq.Impl
{
    using System;
    using System.Collections;
    using System.Linq;
    using System.Linq.Expressions;
    using System.Reflection;
    using Remotion.Linq.Parsing;

    /// <summary>
    /// Transforms JoinClause with parameterised inner sequence to .Join(innerSequence ?? new T[0] ...
    /// </summary>
    internal class JoinInnerSequenceParameterNotNullExpressionVisitor : RelinqExpressionVisitor
    {
        /** */
        private static readonly MethodInfo[] JoinMethods = typeof(Queryable).GetMethods()
            .Where(info => info.Name == "Join")
            .ToArray();

        /** */
        private static readonly Type EnumerableType = typeof(IEnumerable);

        /** */
        private bool _inJoin;

        /** <inheritdoc /> */
        protected override Expression VisitMethodCall(MethodCallExpression node)
        {
            if (node.Method.IsGenericMethod)
            {
                var genericMethodDefinition = node.Method.GetGenericMethodDefinition();

                _inJoin = JoinMethods.Any(mi => mi == genericMethodDefinition);
            }

            var result = base.VisitMethodCall(node);

            _inJoin = false;

            return result;
        }

        /** <inheritdoc /> */
        protected override Expression VisitParameter(ParameterExpression node)
        {
            if (_inJoin && EnumerableType.IsAssignableFrom(node.Type))
            {
                var itemType = EnumerableHelper.GetIEnumerableItemType(node.Type);
                return Expression.Coalesce(node, Expression.NewArrayBounds(itemType, Expression.Constant(0)));
            }

            return node;
        }
    }
}