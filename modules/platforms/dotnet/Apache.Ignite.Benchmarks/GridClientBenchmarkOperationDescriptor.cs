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

namespace GridGain.Client.Benchmark
{
    using System;

    /// <summary>
    /// Benchmark operation.
    /// </summary>
    /// <param name="state">Benchmark state.</param>
    public delegate void GridClientBenchmarkOperation(GridClientBenchmarkState state);

    /// <summary>
    /// Benchmark operation descriptor.
    /// </summary>
    internal class GridClientBenchmarkOperationDescriptor
    {
        /// <summary>
        /// Create new operation descriptor.
        /// </summary>
        /// <param name="name">Name.</param>
        /// <param name="opertaion">Operation.</param>
        /// <param name="weight">Weight.</param>
        /// <returns>Operation descriptor.</returns>
        public static GridClientBenchmarkOperationDescriptor Create(string name, GridClientBenchmarkOperation opertaion, int weight)
        {
            if (name == null || name.Length == 0)
                throw new Exception("Operation name cannot be null or empty.");

            if (opertaion == null)
                throw new Exception("Operation cannot be null: " + name);

            if (weight <= 0)
                throw new Exception("Operation weight cannot be negative [name=" + name + ", weight=" + weight + ']');

            GridClientBenchmarkOperationDescriptor desc = new GridClientBenchmarkOperationDescriptor();

            desc.Name = name;
            desc.Operation = opertaion;
            desc.Weight = weight;

            return desc;
        }

        /// <summary>
        /// Unique operation name.
        /// </summary>
        public string Name
        {
            get;
            private set;
        }

        /// <summary>
        /// Operation delegate.
        /// </summary>
        public GridClientBenchmarkOperation Operation
        {
            get;
            private set;
        }

        /// <summary>
        /// Weight.
        /// </summary>
        public int Weight
        {
            get;
            private set;
        }
    }
}
