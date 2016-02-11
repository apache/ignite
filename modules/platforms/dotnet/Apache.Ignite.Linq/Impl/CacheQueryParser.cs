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
    using System.Threading;
    using Remotion.Linq.Parsing.ExpressionVisitors.Transformation;
    using Remotion.Linq.Parsing.ExpressionVisitors.TreeEvaluation;
    using Remotion.Linq.Parsing.Structure;
    using Remotion.Linq.Parsing.Structure.ExpressionTreeProcessors;

    /// <summary>
    /// Cache query parser.
    /// </summary>
    internal static class CacheQueryParser
    {
        /** */
        private static readonly ThreadLocal<QueryParser> ThreadLocalInstance =
            new ThreadLocal<QueryParser>(CreateParser);

        /// <summary>
        /// Gets the default instance for current thread.
        /// </summary>
        public static QueryParser Instance
        {
            get { return ThreadLocalInstance.Value; }
        }

        private static QueryParser CreateParser()
        {
            var transformerRegistry = ExpressionTransformerRegistry.CreateDefault();
            var evaluatableExpressionFilter = new NullEvaluatableExpressionFilter();
            var expressionTreeParser = new ExpressionTreeParser(
                ExpressionTreeParser.CreateDefaultNodeTypeProvider(),
                CreateProcessor(transformerRegistry, evaluatableExpressionFilter));
            return new QueryParser(expressionTreeParser);
        }

        private static CompoundExpressionTreeProcessor CreateProcessor(
            IExpressionTranformationProvider tranformationProvider,
            IEvaluatableExpressionFilter evaluatableExpressionFilter)
        {
            return new CompoundExpressionTreeProcessor(
                new IExpressionTreeProcessor[]
                {
                    new PartialEvaluatingExpressionTreeProcessor(evaluatableExpressionFilter),
                    new TransformingExpressionTreeProcessor(tranformationProvider)
                });
        }


        private sealed class NullEvaluatableExpressionFilter : EvaluatableExpressionFilterBase
        {
            // No-op.
        }
    }
}