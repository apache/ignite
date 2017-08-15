﻿/*
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
    /// <see cref="CacheLinqExtensions.UpdateAll{TKey,TValue}(System.Linq.IQueryable{Apache.Ignite.Core.Cache.ICacheEntry{TKey,TValue}},System.Linq.Expressions.Expression{System.Func{Apache.Ignite.Linq.IUpdateDescriptor{TValue},Apache.Ignite.Linq.IUpdateDescriptor{TValue}}})"/>.
    /// When user calls UpdateAll, this node is generated.
    /// </summary>
    internal sealed class UpdateAllExpressionNode : ResultOperatorExpressionNodeBase
    {
        /** */
        private static readonly MethodInfo[] UpdateAllMethodInfos = typeof(CacheLinqExtensions)
            .GetMethods()
            .Where(x => x.Name == "UpdateAll")
            .ToArray();

        /// <summary>
        /// The UpdateAll(pred) method.
        /// </summary>
        public static readonly MethodInfo UpdateAllDescriptorMethodInfo =
            UpdateAllMethodInfos.Single(x => x.GetParameters().Length == 2 && x.GetParameters()[1].ParameterType.IsGenericType);


        //public static readonly MethodInfo UpdateAllString =
        //    UpdateAllMethodInfos.Single(x => x.GetParameters().Length == 2);

        /// <summary>
        /// Initializes a new instance of the <see cref="UpdateAllExpressionNode"/> class.
        /// </summary>
        /// <param name="parseInfo">The parse information.</param>
        /// <param name="optionalPredicate">The optional predicate.</param>
        /// <param name="optionalSelector">The optional selector.</param>
        public UpdateAllExpressionNode(MethodCallExpressionParseInfo parseInfo,
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
            return new UpdateAllResultOperator();
        }

        /// <summary>
        /// Gets the supported methods.
        /// </summary>
        public static IEnumerable<MethodInfo> GetSupportedMethods()
        {
            yield return UpdateAllDescriptorMethodInfo;
            //yield return UpdateAllString;
        }
    }
}
