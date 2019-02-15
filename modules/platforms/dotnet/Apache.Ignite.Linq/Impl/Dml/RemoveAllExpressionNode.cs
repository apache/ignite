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
    using System.Collections.Generic;
    using System.Diagnostics.CodeAnalysis;
    using System.Linq;
    using System.Linq.Expressions;
    using System.Reflection;
    using Apache.Ignite.Core.Cache;
    using Remotion.Linq.Clauses;
    using Remotion.Linq.Parsing.Structure.IntermediateModel;

    /// <summary>
    /// Represents a <see cref="MethodCallExpression"/> for 
    /// <see cref="CacheLinqExtensions.RemoveAll{TKey,TValue}(IQueryable{ICacheEntry{TKey,TValue}})"/>.
    /// When user calls RemoveAll, this node is generated.
    /// </summary>
    internal sealed class RemoveAllExpressionNode : ResultOperatorExpressionNodeBase
    {
        /** */
        private static readonly MethodInfo[] RemoveAllMethodInfos = typeof(CacheLinqExtensions)
            .GetMethods().Where(x => x.Name == "RemoveAll").ToArray();

        /// <summary>
        /// The RemoveAll() method.
        /// </summary>
        public static readonly MethodInfo RemoveAllMethodInfo =
            RemoveAllMethodInfos.Single(x => x.GetParameters().Length == 1);

        /// <summary>
        /// The RemoveAll(pred) method.
        /// </summary>
        public static readonly MethodInfo RemoveAllPredicateMethodInfo =
            RemoveAllMethodInfos.Single(x => x.GetParameters().Length == 2);

        /// <summary>
        /// Initializes a new instance of the <see cref="RemoveAllExpressionNode"/> class.
        /// </summary>
        /// <param name="parseInfo">The parse information.</param>
        /// <param name="optionalPredicate">The optional predicate.</param>
        /// <param name="optionalSelector">The optional selector.</param>
        public RemoveAllExpressionNode(MethodCallExpressionParseInfo parseInfo,
            LambdaExpression optionalPredicate, LambdaExpression optionalSelector)
            : base(parseInfo, optionalPredicate, optionalSelector)
        {
            // No-op.
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
            return new RemoveAllResultOperator();
        }

        /// <summary>
        /// Gets the supported methods.
        /// </summary>
        public static IEnumerable<MethodInfo> GetSupportedMethods()
        {
            yield return RemoveAllMethodInfo;
            yield return RemoveAllPredicateMethodInfo;
        }
    }
}
