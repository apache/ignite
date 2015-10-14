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

namespace Apache.Ignite.Core.Compute
{
    /// <summary>
    /// Compute reducer which is capable of result collecting and reducing.
    /// </summary>
    /// <typeparam name="TRes">Type of results passed for reducing.</typeparam>
    /// <typeparam name="TReduceRes">Type of reduced result.</typeparam>
    public interface IComputeReducer<in TRes, out TReduceRes>
    {
        /// <summary>
        /// Collect closure execution result.
        /// </summary>
        /// <param name="res">Result.</param>
        /// <returns><c>True</c> to continue collecting results until all closures are finished, 
        /// <c>false</c> to start reducing.</returns>
        bool Collect(TRes res);

        /// <summary>
        /// Reduce closure execution results collected earlier.
        /// </summary>
        /// <returns>Reduce result.</returns>
        TReduceRes Reduce();
    }
}
