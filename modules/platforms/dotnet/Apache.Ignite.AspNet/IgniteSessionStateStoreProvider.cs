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

namespace Apache.Ignite.AspNet
{
    using System;
    using System.Collections.Specialized;
    using System.Diagnostics.CodeAnalysis;
    using System.Web;
    using System.Web.SessionState;
    using Apache.Ignite.AspNet.Impl;
    using Apache.Ignite.Core.Cache;

    /// <summary>
    /// ASP.NET Session-State Store Provider that use Ignite distributed cache as an underlying storage.
    /// </summary>
    public class IgniteSessionStateStoreProvider : SessionStateStoreProviderBase
    {
        /** */
        private volatile ICache<string, object> _cache;

        // TODO: See 
        /*
           cacheName="myPartionReplicaCache" 
           sessionAppId="NCacheApp1"
           exceptionsEnabled="true" 
           writeExceptionsToEventLog="false"
           enableLogs="false" 
           enableSessionLocking="true"
           sessionLockingRetry="-1"
           emptySessionWhenLocked="false" 
         */

        /// <summary>
        /// Initializes the provider.
        /// </summary>
        /// <param name="name">The friendly name of the provider.</param>
        /// <param name="config">A collection of the name/value pairs representing the provider-specific attributes 
        /// specified in the configuration for this provider.</param>
        [SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods")]
        public override void Initialize(string name, NameValueCollection config)
        {
            base.Initialize(name, config);

            _cache = ConfigUtil.InitializeCache<string, object>(config, GetType());
        }


        /// <summary>
        /// Releases all resources used by the <see cref="T:System.Web.SessionState.SessionStateStoreProviderBase" /> 
        /// implementation.
        /// </summary>
        public override void Dispose()
        {
            // TODO
        }

        /// <summary>
        /// Sets a reference to the <see cref="T:System.Web.SessionState.SessionStateItemExpireCallback" /> 
        /// delegate for the Session_OnEnd event defined in the Global.asax file.
        /// </summary>
        /// <param name="expireCallback">The <see cref="T:System.Web.SessionState.SessionStateItemExpireCallback" />  
        /// delegate for the Session_OnEnd event defined in the Global.asax file.</param>
        /// <returns>
        /// true if the session-state store provider supports calling the Session_OnEnd event; otherwise, false.
        /// </returns>
        public override bool SetItemExpireCallback(SessionStateItemExpireCallback expireCallback)
        {
            // TODO
            return false;
        }

        public override void InitializeRequest(HttpContext context)
        {
            // TODO
            Cache["1"] = 1;
        }

        /// <summary>
        /// Returns read-only session-state data from the session data store.
        /// </summary>
        /// <param name="context">The <see cref="T:System.Web.HttpContext" /> for the current request.</param>
        /// <param name="id">The <see cref="P:System.Web.SessionState.HttpSessionState.SessionID" /> for the 
        /// current request.</param>
        /// <param name="locked">When this method returns, contains a Boolean value that is set to true if the 
        /// requested session item is locked at the session data store; otherwise, false.</param>
        /// <param name="lockAge">When this method returns, contains a <see cref="T:System.TimeSpan" /> object that 
        /// is set to the amount of time that an item in the session data store has been locked.</param>
        /// <param name="lockId">When this method returns, contains an object that is set to the lock identifier 
        /// for the current request. For details on the lock identifier, see "Locking Session-Store Data" 
        /// in the <see cref="T:System.Web.SessionState.SessionStateStoreProviderBase" /> class summary.</param>
        /// <param name="actions">When this method returns, contains one of the 
        /// <see cref="T:System.Web.SessionState.SessionStateActions" /> values, indicating whether the current 
        /// session is an uninitialized, cookieless session.</param>
        /// <returns>
        /// A <see cref="T:System.Web.SessionState.SessionStateStoreData" /> populated with session values and 
        /// information from the session data store.
        /// </returns>
        public override SessionStateStoreData GetItem(HttpContext context, string id, out bool locked,
            out TimeSpan lockAge, out object lockId,
            out SessionStateActions actions)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Returns read-only session-state data from the session data store.
        /// </summary>
        /// <param name="context">The <see cref="T:System.Web.HttpContext" /> for the current request.</param>
        /// <param name="id">The <see cref="P:System.Web.SessionState.HttpSessionState.SessionID" /> for the current 
        /// request.</param>
        /// <param name="locked">When this method returns, contains a Boolean value that is set to true if a lock 
        /// is successfully obtained; otherwise, false.</param>
        /// <param name="lockAge">When this method returns, contains a <see cref="T:System.TimeSpan" /> object that 
        /// is set to the amount of time that an item in the session data store has been locked.</param>
        /// <param name="lockId">When this method returns, contains an object that is set to the lock identifier 
        /// for the current request. For details on the lock identifier, see "Locking Session-Store Data" in 
        /// the <see cref="T:System.Web.SessionState.SessionStateStoreProviderBase" /> class summary.</param>
        /// <param name="actions">When this method returns, contains one of the 
        /// <see cref="T:System.Web.SessionState.SessionStateActions" /> values, indicating whether the current 
        /// session is an uninitialized, cookieless session.</param>
        /// <returns>
        /// A <see cref="T:System.Web.SessionState.SessionStateStoreData" /> populated with session values 
        /// and information from the session data store.
        /// </returns>
        public override SessionStateStoreData GetItemExclusive(HttpContext context, string id, out bool locked,
            out TimeSpan lockAge,
            out object lockId, out SessionStateActions actions)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Releases a lock on an item in the session data store.
        /// </summary>
        /// <param name="context">The <see cref="T:System.Web.HttpContext" /> for the current request.</param>
        /// <param name="id">The session identifier for the current request.</param>
        /// <param name="lockId">The lock identifier for the current request.</param>
        public override void ReleaseItemExclusive(HttpContext context, string id, object lockId)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Updates the session-item information in the session-state data store with values from the current request, 
        /// and clears the lock on the data.
        /// </summary>
        /// <param name="context">The <see cref="T:System.Web.HttpContext" /> for the current request.</param>
        /// <param name="id">The session identifier for the current request.</param>
        /// <param name="item">The <see cref="T:System.Web.SessionState.SessionStateStoreData" /> object that 
        /// contains the current session values to be stored.</param>
        /// <param name="lockId">The lock identifier for the current request.</param>
        /// <param name="newItem">true to identify the session item as a new item; false to identify the session 
        /// item as an existing item.</param>
        public override void SetAndReleaseItemExclusive(HttpContext context, string id, SessionStateStoreData item,
            object lockId, bool newItem)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Deletes item data from the session data store.
        /// </summary>
        /// <param name="context">The <see cref="T:System.Web.HttpContext" /> for the current request.</param>
        /// <param name="id">The session identifier for the current request.</param>
        /// <param name="lockId">The lock identifier for the current request.</param>
        /// <param name="item">The <see cref="T:System.Web.SessionState.SessionStateStoreData" /> that represents 
        /// the item to delete from the data store.</param>
        public override void RemoveItem(HttpContext context, string id, object lockId, SessionStateStoreData item)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Updates the expiration date and time of an item in the session data store.
        /// </summary>
        /// <param name="context">The <see cref="T:System.Web.HttpContext" /> for the current request.</param>
        /// <param name="id">The session identifier for the current request.</param>
        public override void ResetItemTimeout(HttpContext context, string id)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Creates a new <see cref="T:System.Web.SessionState.SessionStateStoreData" /> object to be used 
        /// for the current request.
        /// </summary>
        /// <param name="context">The <see cref="T:System.Web.HttpContext" /> for the current request.</param>
        /// <param name="timeout">The session-state <see cref="P:System.Web.SessionState.HttpSessionState.Timeout" /> 
        /// value for the new <see cref="T:System.Web.SessionState.SessionStateStoreData" />.</param>
        /// <returns>
        /// A new <see cref="T:System.Web.SessionState.SessionStateStoreData" /> for the current request.
        /// </returns>
        public override SessionStateStoreData CreateNewStoreData(HttpContext context, int timeout)
        {
            return new SessionStateStoreData(new SessionStateItemCollection(),
                SessionStateUtility.GetSessionStaticObjects(context), timeout);
        }

        /// <summary>
        /// Adds a new session-state item to the data store.
        /// </summary>
        /// <param name="context">The <see cref="T:System.Web.HttpContext" /> for the current request.</param>
        /// <param name="id">The <see cref="P:System.Web.SessionState.HttpSessionState.SessionID" /> 
        /// for the current request.</param>
        /// <param name="timeout">The session <see cref="P:System.Web.SessionState.HttpSessionState.Timeout" /> 
        /// for the current request.</param>
        public override void CreateUninitializedItem(HttpContext context, string id, int timeout)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Called by the <see cref="T:System.Web.SessionState.SessionStateModule" /> object at the end of a request.
        /// </summary>
        /// <param name="context">The <see cref="T:System.Web.HttpContext" /> for the current request.</param>
        public override void EndRequest(HttpContext context)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Gets the cache.
        /// </summary>
        private ICache<string, object> Cache
        {
            get
            {
                var cache = _cache;

                if (cache == null)
                    throw new InvalidOperationException(GetType() + " has not been initialized.");

                return cache;
            }}


    }
}