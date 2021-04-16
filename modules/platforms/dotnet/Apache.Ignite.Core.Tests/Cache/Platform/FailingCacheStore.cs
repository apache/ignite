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

namespace Apache.Ignite.Core.Tests.Cache.Platform
{
    using System;
    using Apache.Ignite.Core.Cache.Store;
    using Apache.Ignite.Core.Common;

    /// <summary>
    /// Cache store that throws exception for certain value.
    /// </summary>
    public class FailingCacheStore : CacheStoreAdapter<int, Foo>, IFactory<ICacheStore<int, Foo>>
    {
        /// <summary>
        /// Value that causes failure.
        /// </summary>
        public const int FailingValue = -1;
        
        /// <summary>
        /// Value to be loaded from store.
        /// </summary>
        public static readonly Foo Foo = new Foo(768);

        /** <inheritdoc /> */
        public ICacheStore<int, Foo> CreateInstance()
        {
            return new FailingCacheStore();
        }
            
        /** <inheritdoc /> */
        public override Foo Load(int key)
        {
            return null;
        }

        /** <inheritdoc /> */
        public override void LoadCache(Action<int, Foo> act, params object[] args)
        {
            act(Foo.Bar, Foo);
        }

        /** <inheritdoc /> */
        public override void Write(int key, Foo val)
        {
            if (val != null && val.Bar == FailingValue)
            {
                throw new Exception("Fail");
            }
        }

        /** <inheritdoc /> */
        public override void Delete(int key)
        {
            // No-op.
        }
    }
}