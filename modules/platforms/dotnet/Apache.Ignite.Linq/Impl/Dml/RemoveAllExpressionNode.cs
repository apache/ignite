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
