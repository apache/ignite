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
    using System;

    /// <summary>
    /// Represents a distributed atomic long value.
    /// </summary>
    public interface IAtomicLong : IDisposable
    {
        /// <summary>
        /// Gets the name of this atomic long.
        /// </summary>
        string Name { get; }

        /// <summary>
        /// Gets the value of this atomic long.
        /// </summary>
        /// <returns>Current value of atomic long.</returns>
        long Read();

        long Increment();

        long ReadAndIncrement();

        long Add(long value);
        
        long ReadAndAdd(long value);

        long Decrement();

        long ReadAndDecrement();

        long Exchange(long value);

        long CompareExchange(long value, long comparand);

        bool IsRemoved();
    }
}
