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

namespace Apache.Ignite.Core.Cache.Query.Continuous
{
    using Apache.Ignite.Core.Cache.Event;
    using Apache.Ignite.Core.Impl.Cache.Event;
    using Apache.Ignite.Core.Impl.Common;
    using Apache.Ignite.Core.Interop;

    /// <summary>
    /// Extensions for continuous queries.
    /// </summary>
    public static class ContinuousQueryExtensions
    {
        /// <summary>
        /// Creates the cache event filter that delegates to the corresponding Java object.
        /// </summary>
        /// <typeparam name="TK">Key type.</typeparam>
        /// <typeparam name="TV">Value type.</typeparam>
        public static ICacheEntryEventFilter<TK, TV> ToCacheEntryEventFilter<TK, TV>(this JavaObject javaObject)
        {
            IgniteArgumentCheck.NotNull(javaObject, "javaObject");

            return new JavaCacheEntryEventFilter<TK, TV>(javaObject.ClassName, javaObject.Properties);
        }
    }
}
