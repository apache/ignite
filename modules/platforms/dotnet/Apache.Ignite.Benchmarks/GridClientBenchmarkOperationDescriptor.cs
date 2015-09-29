/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
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
