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

namespace Apache.Ignite.Core.DataStructures
{
    /// <summary>
    /// Represents a distributed atomic sequence of numbers.
    /// </summary>
    public interface IAtomicSequence
    {
        /// <summary>
        /// Gets the name of this atomic sequence.
        /// </summary>
        /// <value>
        /// Name of this atomic sequence.
        /// </value>
        string Name { get; }

        /// <summary>
        /// Returns current value.
        /// </summary>
        /// <returns>Current value of the atomic sequence.</returns>
        long Read();

        /// <summary>
        /// Increments current value and returns result.
        /// </summary>
        /// <returns>The new value of the atomic sequence.</returns>
        long Increment();

        /// <summary>
        /// Adds specified value to the current value and returns result.
        /// </summary>
        /// <param name="value">The value to add.</param>
        /// <returns>The new value of the atomic sequence.</returns>
        long Add(long value);

        /// <summary>
        /// Gets local batch size for this atomic sequence.
        /// </summary>
        /// <returns>Sequence batch size.</returns>
        int BatchSize { get; set; }

        /// <summary>
        /// Determines whether this instance was removed from cache.
        /// </summary>
        /// <returns>True if this atomic was removed from cache; otherwise, false.</returns>
        bool IsClosed { get; }

        /// <summary>
        /// Closes this instance.
        /// </summary>
        void Close();
    }
}
