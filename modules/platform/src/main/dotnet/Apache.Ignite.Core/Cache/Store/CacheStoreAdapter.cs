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

namespace Apache.Ignite.Core.Cache.Store
{
    using System;
    using System.Collections;
    using System.Linq;

    /// <summary>
    /// Cache storage convenience adapter. It provides default implementation for 
    /// bulk operations, such as <code>LoadAll</code>, <code>PutAll</code> and
    /// <code>RemoveAll</code> by sequentially calling corresponding <code>Load</code>,
    /// <code>Put</code> and <code>Remove</code> operations. Use this adapter whenever 
    /// such behaviour is acceptable. However in many cases it maybe more preferable 
    /// to take advantage of database batch update functionality, and therefore default 
    /// adapter implementation may not be the best option.
    /// <para/>
    /// Note that <code>LoadCache</code> method has empty implementation because it is 
    /// essentially up to the user to invoke it with specific arguments.
    /// </summary>
    public abstract class CacheStoreAdapter : ICacheStore
    {
        /// <summary>
        /// Loads all values from underlying persistent storage. Note that keys are
        /// not passed, so it is up to implementation to figure out what to load.
        /// This method is called whenever <see cref="ICache{K,V}.LocalLoadCache" />
        /// method is invoked which is usually to preload the cache from persistent storage.
        /// <para />
        /// This method is optional, and cache implementation
        /// does not depend on this method to do anything.
        /// <para />
        /// For every loaded value method provided action should be called.
        /// The action will then make sure that the loaded value is stored in cache.
        /// </summary>
        /// <param name="act">Action for loaded values.</param>
        /// <param name="args">Optional arguemnts passed to <see cref="ICache{K,V}.LocalLoadCache" /> method.</param>
        public virtual void LoadCache(Action<object, object> act, params object[] args)
        {
            // No-op.
        }
        
        /// <summary>
        /// Loads multiple objects. Application developers should implement this method to customize
        /// the loading of cache entries. This method is called when the requested object is not in the cache.
        /// If an object can't be loaded, it is not returned in the resulting map.
        /// </summary>
        /// <param name="keys">Keys identifying the values to be loaded.</param>
        /// <returns>
        /// A map of key, values to be stored in the cache.
        /// </returns>
        public virtual IDictionary LoadAll(ICollection keys)
        {
            return keys.OfType<object>().ToDictionary(key => key, Load);
        }
        
        /// <summary>
        /// Writes all.
        /// </summary>
        /// <param name="entries">The map.</param>
        public virtual void WriteAll(IDictionary entries)
        {
            foreach (DictionaryEntry entry in entries)
                Write(entry.Key, entry.Value);
        }
        
        /// <summary>
        /// Remove data and keys from the external resource for the given collection of keys, if present.
        /// <para />
        /// The order that individual deletes occur is undefined.
        /// <para />
        /// If this operation fails (by throwing an exception) after a partial success,
        /// the writer must remove any successfully written entries from the entries collection
        /// so that the caching implementation knows what succeeded and can mutate the cache.
        /// <para />
        /// Expiry of a cache entry is not a delete hence will not cause this method to be invoked.
        /// <para />
        /// This method may include keys even if there is no mapping for that key,
        /// in which case the data represented by that key should be removed from the underlying resource.
        /// </summary>
        /// <param name="keys">a mutable collection of keys for entries to delete. Upon invocation,
        /// it contains the keys to delete for write-through. Upon return the collection must only contain
        /// the keys that were not successfully deleted.</param>
        public virtual void DeleteAll(ICollection keys)
        {
            foreach (object key in keys)
                Delete(key);
        }
        
        /// <summary>
        /// Tells store to commit or rollback a transaction depending on the value of the
        /// <c>commit</c> parameter.
        /// </summary>
        /// <param name="commit"><c>True</c> if transaction should commit, <c>false</c> for rollback.</param>
        public virtual void SessionEnd(bool commit)
        {
            // No-op.
        }
        
        /// <summary>
        /// Loads an object. Application developers should implement this method to customize the loading
        /// of a value for a cache entry.
        /// This method is called by a cache when a requested entry is not in the cache.
        /// If the object can't be loaded <code>null</code> should be returned.
        /// </summary>
        /// <param name="key">The key identifying the object being loaded.</param>
        /// <returns>
        /// The value for the entry that is to be stored in the cache
        /// or <code>null</code> if the object can't be loaded
        /// </returns>
        public abstract object Load(object key);

        /// <summary>
        /// Write the specified value under the specified key to the external resource.
        /// <para />
        /// This method is intended to support both key/value creation and value update.
        /// </summary>
        /// <param name="key">Key to write.</param>
        /// <param name="val">Value to write.</param>
        public abstract void Write(object key, object val);
        
        /// <summary>
        /// Delete the cache entry from the external resource.
        /// <para />
        /// Expiry of a cache entry is not a delete hence will not cause this method to be invoked.
        /// <para />
        /// This method is invoked even if no mapping for the key exists.
        /// </summary>
        /// <param name="key">The key that is used for the delete operation.</param>
        public abstract void Delete(object key);
    }
}
