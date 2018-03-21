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

namespace Apache.Ignite.Linq
{
    using System;
    using Apache.Ignite.Core.Cache;

    /// <summary>
    /// Dummy interface to provide update description for
    /// <see
    ///     cref="CacheLinqExtensions.UpdateAll{TKey,TValue}" />
    /// </summary>
    /// <typeparam name="TKey">Key type.</typeparam>
    /// <typeparam name="TValue">Value type.</typeparam>
    public interface IUpdateDescriptor<out TKey, out TValue>
    {
        /// <summary>
        /// Specifies member update with constant
        /// </summary>
        /// <typeparam name="TProp">Member type</typeparam>
        /// <param name="selector">Member selector</param>
        /// <param name="value">New value</param>
        /// <returns></returns>
        IUpdateDescriptor<TKey, TValue> Set<TProp>(Func<TValue, TProp> selector, TProp value);

        /// <summary>
        /// Specifies member update with expression
        /// </summary>
        /// <typeparam name="TProp">Member type</typeparam>
        /// <param name="selector">Member selector</param>
        /// <param name="valueBuilder">New value generator</param>
        /// <returns></returns>
        IUpdateDescriptor<TKey, TValue> Set<TProp>(Func<TValue, TProp> selector,
            Func<ICacheEntry<TKey, TValue>, TProp> valueBuilder);
    }
}