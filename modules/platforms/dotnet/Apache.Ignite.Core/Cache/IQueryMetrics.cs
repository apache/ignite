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

namespace Apache.Ignite.Core.Cache
{
    /// <summary>
    /// Cache query metrics used to obtain statistics on query.
    /// </summary>
    public interface IQueryMetrics
    {
        /// <summary>
        /// Gets minimum execution time of query.
        /// </summary>
        /// <returns>
        /// Minimum execution time of query.
        /// </returns>
        long MinimumTime { get; }

        /// <summary>
        /// Gets maximum execution time of query.
        /// </summary>
        /// <returns>
        /// Maximum execution time of query.
        /// </returns>
        long MaximumTime { get; }

        /// <summary>
        /// Gets average execution time of query.
        /// </summary>
        /// <returns>
        /// Average execution time of query.
        /// </returns>
        double AverageTime { get; }

        /// <summary>
        /// Gets total number execution of query.
        /// </summary>
        /// <returns>
        /// Number of executions.
        /// </returns>
        int Executions { get; }

        /// <summary>
        /// Gets total number of times a query execution failed.
        /// </summary>
        /// <returns>
        /// Total number of times a query execution failed.
        /// </returns>
        int Fails { get; }
    }
}