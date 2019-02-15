/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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