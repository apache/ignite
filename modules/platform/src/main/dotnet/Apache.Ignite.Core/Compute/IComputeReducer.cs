/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace Apache.Ignite.Core.Compute
{
    /// <summary>
    /// Compute reducer which is capable of result collecting and reducing.
    /// </summary>
    public interface IComputeReducer<in R1, out R2>
    {
        /// <summary>
        /// Collect closure execution result.
        /// </summary>
        /// <param name="res">Result.</param>
        /// <returns><c>True</c> to continue collecting results until all closures are finished, 
        /// <c>false</c> to start reducing.</returns>
        bool Collect(R1 res);

        /// <summary>
        /// Reduce closure execution results collected earlier.
        /// </summary>
        /// <returns>Reduce result.</returns>
        R2 Reduce();
    }
}
