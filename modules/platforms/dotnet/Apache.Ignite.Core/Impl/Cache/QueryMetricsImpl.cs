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

namespace Apache.Ignite.Core.Impl.Cache
{
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Cache;

    /// <summary>
    /// Cache query metrics used to obtain statistics on query.
    /// </summary>
    internal class QueryMetricsImpl : IQueryMetrics
    {
        /** */
        private readonly long _minimumTime;

        /** */
        private readonly long _maximumTime;

        /** */
        private readonly double _averageTime;

        /** */
        private readonly int _executions;

        /** */
        private readonly int _fails;

        /// <summary>
        /// Initializes a new instance of the <see cref="QueryMetricsImpl"/> class.
        /// </summary>
        /// <param name="reader">The reader.</param>
        public QueryMetricsImpl(IBinaryRawReader reader)
        {
            _minimumTime = reader.ReadLong();
            _maximumTime = reader.ReadLong();
            _averageTime = reader.ReadDouble();
            _executions = reader.ReadInt();
            _fails = reader.ReadInt();
        }

        /** <inheritDoc /> */
        public long MinimumTime { get { return _minimumTime; } }

        /** <inheritDoc /> */
        public long MaximumTime { get { return _maximumTime; } }

        /** <inheritDoc /> */
        public double AverageTime { get { return _averageTime; } }

        /** <inheritDoc /> */
        public int Executions { get { return _executions; } }

        /** <inheritDoc /> */
        public int Fails { get { return _fails; } }
    }
}