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
    /// Represents a result of processing <see cref="ICacheEntry{K, V}"/> 
    /// by <see cref="ICacheEntryProcessor{K, V, A, R}"/>.
    /// </summary>
    /// <typeparam name="T">Processor result type.</typeparam>
    public interface ICacheEntryProcessorResult<out T>
    {
        /// <summary>
        /// Gets the result of processing an entry.
        /// <para />
        /// If an exception was thrown during the processing of an entry, 
        /// either by the <see cref="ICacheEntryProcessor{K, V, A, R}"/> itself 
        /// or by the Caching implementation, the exceptions will be wrapped and re-thrown as a 
        /// <see cref="CacheEntryProcessorException"/> when calling this property.
        /// </summary>
        /// <value>
        /// The result.
        /// </value>
        T Result { get; }
    }
}