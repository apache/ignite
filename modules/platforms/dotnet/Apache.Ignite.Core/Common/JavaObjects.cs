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

namespace Apache.Ignite.Core.Common
{
    using Apache.Ignite.Core.Cache.Event;
    using Apache.Ignite.Core.Impl.Common.JavaObjects;

    /// <summary>
    /// Java object factory.
    /// </summary>
    public static class JavaObjects
    {
        /// <summary>
        /// Creates the cache event filter that delegates to specified Java class.
        /// </summary>
        /// <typeparam name="TK">Key type.</typeparam>
        /// <typeparam name="TV">Value type.</typeparam>
        /// <param name="className">Name of the class.</param>
        /// <returns>Cache event filter that delegates to specified Java class.</returns>
        public static ICacheEntryEventFilter<TK, TV> CreateCacheEventFilter<TK, TV>(string className)
        {
            return new JavaCacheEntryEventFilter<TK, TV>(className);
        }
    }
}
