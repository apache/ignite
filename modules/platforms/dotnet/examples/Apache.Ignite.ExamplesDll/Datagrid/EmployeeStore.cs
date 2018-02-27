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

namespace Apache.Ignite.ExamplesDll.Datagrid
{
    using System;
    using System.Collections;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Cache.Store;
    using Apache.Ignite.ExamplesDll.Binary;

    /// <summary>
    /// Example cache store implementation.
    /// </summary>
    public class EmployeeStore : CacheStoreAdapter
    {
        /// <summary>
        /// Dictionary representing the store.
        /// </summary>
        private readonly ConcurrentDictionary<object, object> _db = new ConcurrentDictionary<object, object>(
            new List<KeyValuePair<object, object>>
            {
                new KeyValuePair<object, object>(1, new Employee(
                    "Allison Mathis",
                    25300,
                    new Address("2702 Freedom Lane, San Francisco, CA", 94109),
                    new List<string> {"Development"}
                    )),

                new KeyValuePair<object, object>(2, new Employee(
                    "Breana Robbin",
                    6500,
                    new Address("3960 Sundown Lane, Austin, TX", 78130),
                    new List<string> {"Sales"}
                    ))
            });

        /// <summary>
        /// Loads all values from underlying persistent storage.
        /// This method gets called as a result of <see cref="ICache{TK,TV}.LoadCache"/> call.
        /// </summary>
        /// <param name="act">Action that loads a cache entry.</param>
        /// <param name="args">Optional arguments.</param>
        public override void LoadCache(Action<object, object> act, params object[] args)
        {
            // Iterate over whole underlying store and call act on each entry to load it into the cache.
            foreach (var entry in _db)
                act(entry.Key, entry.Value);
        }

        /// <summary>
        /// Loads multiple objects from the cache store.
        /// This method gets called as a result of <see cref="ICache{K,V}.GetAll"/> call.
        /// </summary>
        /// <param name="keys">Keys to load.</param>
        /// <returns>
        /// A map of key, values to be stored in the cache.
        /// </returns>
        public override IDictionary LoadAll(ICollection keys)
        {
            var result = new Dictionary<object, object>();

            foreach (var key in keys)
                result[key] = Load(key);

            return result;
        }

        /// <summary>
        /// Loads an object from the cache store.
        /// This method gets called as a result of <see cref="ICache{K,V}.Get"/> call.
        /// </summary>
        /// <param name="key">Key to load.</param>
        /// <returns>Loaded value</returns>
        public override object Load(object key)
        {
            object val;

            _db.TryGetValue(key, out val);

            return val;
        }

        /// <summary>
        /// Write key-value pair to store.
        /// </summary>
        /// <param name="key">Key to write.</param>
        /// <param name="val">Value to write.</param>
        public override void Write(object key, object val)
        {
            _db[key] = val;
        }

        /// <summary>
        /// Delete cache entry form store.
        /// </summary>
        /// <param name="key">Key to delete.</param>
        public override void Delete(object key)
        {
            object val;

            _db.TryRemove(key, out val);
        }
    }
}
