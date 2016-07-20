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
    /// Represents a named value in the distributed cache.
    /// </summary>
    public interface IAtomicReference<T>
    {
        /// <summary>
        /// Gets the name of this atomic reference.
        /// </summary>
        /// <value>
        /// Name of this atomic reference.
        /// </value>
        string Name { get; }

        /// <summary>
        /// Reads current value of an atomic reference.
        /// </summary>
        /// <returns>Current value of an atomic reference.</returns>
        T Read();

        /// <summary>
        /// Writes current value of an atomic reference.
        /// </summary>
        /// <param name="value">The value to set.</param>
        void Write(T value);

        /// <summary>
        /// Compares current value with specified value for equality and, if they are equal, replaces current value.
        /// </summary>
        /// <param name="value">The value to set.</param>
        /// <param name="comparand">The value that is compared to the current value.</param>
        /// <returns>Original value of the atomic reference.</returns>
        T CompareExchange(T value, T comparand);

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
