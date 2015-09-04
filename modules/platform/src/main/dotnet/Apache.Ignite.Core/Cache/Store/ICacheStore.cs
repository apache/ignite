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
    using Apache.Ignite.Core.Transactions;

    /// <summary>
    /// API for cache persistent storage for read-through and write-through behavior.
    ///
    /// Persistent store is configured in Ignite's Spring XML configuration file via
    /// <c>CacheConfiguration.setStore()</c> property. If you have an implementation
    /// of cache store in .NET, you should use special Java wrapper which accepts assembly name and
    /// class name of .NET store implementation (both properties are mandatory).
    /// 
    /// Optionally, you may specify "properies" property to set any property values on an instance of your store.
    /// <example>
    /// Here is an example:
    /// <code>
    /// <bean class="org.apache.ignite.configuration.CacheConfiguration">
    ///     ...
    ///     <property name="cacheStoreFactory">
    ///         <bean class="org.apache.ignite.platform.dotnet.PlatformDotNetCacheStoreFactory">
    ///             <property name="assemblyName" value="MyAssembly"/>
    ///             <property name="className" value="MyApp.MyCacheStore"/>
    ///             <property name="properties">
    ///                 <map>
    ///                     <entry key="IntProperty">
    ///                         <value type="java.lang.Integer">42</value>
    ///                     </entry>
    ///                     <entry key="StringProperty" value="String value"/>
    ///                 </map>
    ///             </property>
    ///         </bean>
    ///     </property>
    ///     ...
    /// </bean>
    /// </code>
    /// </example>
    /// Assemply name and class name are passed to <a target="_blank" href="http://msdn.microsoft.com/en-us/library/d133hta4.aspx"><b>System.Activator.CreateInstance(String, String)</b></a>
    /// method during node startup to create an instance of cache store. Refer to its documentation for details.
    /// <para/>
    /// All transactional operations of this API are provided with ongoing <see cref="ITransaction"/>,
    /// if any. You can attach any metadata to transaction, e.g. to recognize if several operations 
    /// belong to the same transaction or not.
    /// <example>
    /// Here is an example of how attach a ODBC connection as transaction metadata:
    /// <code>
    /// OdbcConnection conn = tx.Meta("some.name");
    ///
    /// if (conn == null)
    /// {
    ///     conn = ...; // Create or get connection.
    ///
    ///     // Store connection in transaction metadata, so it can be accessed
    ///     // for other operations on the same transaction.
    ///     tx.AddMeta("some.name", conn);
    /// }
    /// </code>
    /// </example>
    /// </summary>
    public interface ICacheStore
    {
        /// <summary>
        /// Loads all values from underlying persistent storage. Note that keys are
        /// not passed, so it is up to implementation to figure out what to load.
        /// This method is called whenever <see cref="ICache{K,V}.LocalLoadCache"/>
        /// method is invoked which is usually to preload the cache from persistent storage.
        /// <para/>
        /// This method is optional, and cache implementation
        /// does not depend on this method to do anything.
        /// <para/>
        /// For every loaded value method provided action should be called.
        /// The action will then make sure that the loaded value is stored in cache.
        /// </summary>
        /// <param name="act">Action for loaded values.</param>
        /// <param name="args">Optional arguemnts passed to <see cref="ICache{K,V}.LocalLoadCache"/> method.</param>
        /// <exception cref="CacheStoreException" />
        void LoadCache(Action<object, object> act, params object[] args);

        /// <summary>
        /// Loads an object. Application developers should implement this method to customize the loading 
        /// of a value for a cache entry. 
        /// This method is called by a cache when a requested entry is not in the cache. 
        /// If the object can't be loaded <code>null</code> should be returned.
        /// </summary>
        /// <param name="key">The key identifying the object being loaded.</param>
        /// <returns>The value for the entry that is to be stored in the cache 
        /// or <code>null</code> if the object can't be loaded</returns>
        /// <exception cref="CacheStoreException" />
        object Load(object key);

        /// <summary>
        /// Loads multiple objects. Application developers should implement this method to customize 
        /// the loading of cache entries. This method is called when the requested object is not in the cache. 
        /// If an object can't be loaded, it is not returned in the resulting map.
        /// </summary>
        /// <param name="keys">Keys identifying the values to be loaded.</param>
        /// <returns>A map of key, values to be stored in the cache.</returns>
        /// <exception cref="CacheStoreException" />
        IDictionary LoadAll(ICollection keys);

        /// <summary>
        /// Write the specified value under the specified key to the external resource.
        /// <para />
        /// This method is intended to support both key/value creation and value update.
        /// </summary>
        /// <param name="key">Key to write.</param>
        /// <param name="val">Value to write.</param>
        /// <exception cref="CacheStoreException" />
        void Write(object key, object val);

        /// <summary>
        /// Write the specified entries to the external resource. 
        /// This method is intended to support both insert and update.
        /// <para />
        /// The order that individual writes occur is undefined.
        /// <para />
        /// If this operation fails (by throwing an exception) after a partial success, 
        /// the writer must remove any successfully written entries from the entries collection 
        /// so that the caching implementation knows what succeeded and can mutate the cache.
        /// </summary>
        /// <param name="entries">a mutable collection to write. Upon invocation,  it contains the entries 
        /// to write for write-through. Upon return the collection must only contain entries 
        /// that were not successfully written. (see partial success above).</param>
        /// <exception cref="CacheStoreException" />
        void WriteAll(IDictionary entries);

        /// <summary>
        /// Delete the cache entry from the external resource.
        /// <para />
        /// Expiry of a cache entry is not a delete hence will not cause this method to be invoked.
        /// <para />
        /// This method is invoked even if no mapping for the key exists.
        /// </summary>
        /// <param name="key">The key that is used for the delete operation.</param>
        /// <exception cref="CacheStoreException" />
        void Delete(object key);

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
        /// <exception cref="CacheStoreException" />
        void DeleteAll(ICollection keys);

        /// <summary>
        /// Tells store to commit or rollback a transaction depending on the value of the
        /// <c>commit</c> parameter.
        /// </summary>
        /// <param name="commit"><c>True</c> if transaction should commit, <c>false</c> for rollback.</param>
        /// <exception cref="CacheStoreException" />
        void SessionEnd(bool commit);
    }
}
