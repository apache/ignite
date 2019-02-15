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

namespace Apache.Ignite.Linq.Impl.Dml
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics.CodeAnalysis;
    using System.Linq;
    using System.Linq.Expressions;
    using System.Reflection;
    using Remotion.Linq.Clauses;
    using Remotion.Linq.Clauses.Expressions;
    using Remotion.Linq.Parsing.ExpressionVisitors;
    using Remotion.Linq.Parsing.Structure.IntermediateModel;

    /// <summary>
    /// Represents a <see cref="MethodCallExpression" /> for
    /// <see cref="CacheLinqExtensions.UpdateAll{TKey,TValue}" />.
    /// When user calls UpdateAll, this node is generated.
    /// </summary>
    internal sealed class UpdateAllExpressionNode : ResultOperatorExpressionNodeBase
    {
        /// <summary>
        /// The UpdateAll method.
        /// </summary>
        public static readonly MethodInfo UpdateAllMethodInfo = typeof(CacheLinqExtensions)
            .GetMethods()
            .Single(x => x.Name == "UpdateAll");

        //** */
        private static readonly MethodInfo[] SupportedMethods = {UpdateAllMethodInfo};

        //** */
        private readonly LambdaExpression _updateDescription;

        /// <summary>
        /// Initializes a new instance of the <see cref="UpdateAllExpressionNode" /> class.
        /// </summary>
        /// <param name="parseInfo">The parse information.</param>
        /// <param name="updateDescription">Expression with update description info</param>
        public UpdateAllExpressionNode(MethodCallExpressionParseInfo parseInfo,
            LambdaExpression updateDescription)
            : base(parseInfo, null, null)
        {
            _updateDescription = updateDescription;
        }

        /** <inheritdoc /> */
        [ExcludeFromCodeCoverage]
        public override Expression Resolve(ParameterExpression inputParameter, Expression expressionToBeResolved,
            ClauseGenerationContext clauseGenerationContext)
        {
            throw CreateResolveNotSupportedException();
        }

        /** <inheritdoc /> */
        protected override ResultOperatorBase CreateResultOperator(ClauseGenerationContext clauseGenerationContext)
        {
            if (_updateDescription.Parameters.Count != 1)
                throw new NotSupportedException("Expression is not supported for UpdateAll: " +
                                                _updateDescription);

            var querySourceRefExpression = (QuerySourceReferenceExpression) Source.Resolve(
                _updateDescription.Parameters[0],
                _updateDescription.Parameters[0],
                clauseGenerationContext);

            var cacheEntryType = querySourceRefExpression.Type;
            var querySourceAccessValue =
                Expression.MakeMemberAccess(querySourceRefExpression, cacheEntryType.GetMember("Value").First());

            if (!(_updateDescription.Body is MethodCallExpression))
                throw new NotSupportedException("Expression is not supported for UpdateAll: " +
                                                _updateDescription.Body);

            var updates = new List<MemberUpdateContainer>();

            var methodCall = (MethodCallExpression) _updateDescription.Body;
            while (methodCall != null)
            {
                if (methodCall.Arguments.Count != 2)
                    throw new NotSupportedException("Method is not supported for UpdateAll: " + methodCall);

                var selectorLambda = (LambdaExpression) methodCall.Arguments[0];
                var selector = ReplacingExpressionVisitor.Replace(selectorLambda.Parameters[0], querySourceAccessValue,
                    selectorLambda.Body);

                var newValue = methodCall.Arguments[1];
                switch (newValue.NodeType)
                {
                    case ExpressionType.Constant:
                        break;
                    case ExpressionType.Lambda:
                        var newValueLambda = (LambdaExpression) newValue;
                        newValue = ReplacingExpressionVisitor.Replace(newValueLambda.Parameters[0],
                            querySourceRefExpression, newValueLambda.Body);
                        break;
                    default:
                        throw new NotSupportedException("Value expression is not supported for UpdateAll: "
                                                        + newValue);
                }

                updates.Add(new MemberUpdateContainer
                {
                    Selector = selector,
                    Value = newValue
                });

                methodCall = methodCall.Object as MethodCallExpression;
            }

            return new UpdateAllResultOperator(updates);
        }

        /// <summary>
        /// Gets the supported methods.
        /// </summary>
        public static IEnumerable<MethodInfo> GetSupportedMethods()
        {
            return SupportedMethods;
        }
    }
}