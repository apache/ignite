/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

namespace Apache.Ignite.Core.Impl.Cache.Event
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics.CodeAnalysis;
    using Apache.Ignite.Core.Cache.Event;
    using Apache.Ignite.Core.Impl.Common;

    /// <summary>
    /// Cache entry event filter that delegates to Java.
    /// </summary>
    /// <typeparam name="TK">Key type.</typeparam>
    /// <typeparam name="TV">Value type.</typeparam>
    internal class JavaCacheEntryEventFilter<TK, TV> : PlatformJavaObjectFactoryProxy, ICacheEntryEventFilter<TK, TV>
    {
        /** <inheritdoc /> */
        [ExcludeFromCodeCoverage]
        public bool Evaluate(ICacheEntryEvent<TK, TV> evt)
        {
            throw new InvalidOperationException(GetType() + " cannot be invoked directly.");
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="JavaCacheEntryEventFilter{TK, TV}"/> class.
        /// </summary>
        /// <param name="factoryClassName">Name of the factory class.</param>
        /// <param name="properties">The properties.</param>
        public JavaCacheEntryEventFilter(string factoryClassName, IDictionary<string, object> properties) 
            : base(FactoryType.User, factoryClassName, null, properties)
        {
            // No-op.
        }
    }
}
