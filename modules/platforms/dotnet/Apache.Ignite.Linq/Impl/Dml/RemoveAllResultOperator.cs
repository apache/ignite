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

namespace Apache.Ignite.Linq.Impl.Dml
{
    using System;
    using System.Diagnostics.CodeAnalysis;
    using System.Linq;
    using System.Linq.Expressions;
    using Apache.Ignite.Core.Cache;
    using Remotion.Linq.Clauses;
    using Remotion.Linq.Clauses.ResultOperators;
    using Remotion.Linq.Clauses.StreamedData;

    /// <summary>
    /// Represents an operator for <see cref="CacheLinqExtensions.RemoveAll{TK,TV}(IQueryable{ICacheEntry{TK,TV}})"/>.
    /// </summary>
    internal sealed class RemoveAllResultOperator : ValueFromSequenceResultOperatorBase
    {
        /** <inheritdoc /> */
        public override IStreamedDataInfo GetOutputDataInfo(IStreamedDataInfo inputInfo)
        {
            return new StreamedScalarValueInfo(typeof(int));
        }

        /** <inheritdoc /> */
        [ExcludeFromCodeCoverage]
        public override ResultOperatorBase Clone(CloneContext cloneContext)
        {
            return new RemoveAllResultOperator();
        }

        /** <inheritdoc /> */
        [ExcludeFromCodeCoverage]
        public override void TransformExpressions(Func<Expression, Expression> transformation)
        {
            // No-op.
        }

        /** <inheritdoc /> */
        [ExcludeFromCodeCoverage]
        public override StreamedValue ExecuteInMemory<T>(StreamedSequence sequence)
        {
            throw new NotSupportedException("RemoveAll is not supported for in-memory sequences.");
        }
    }
}