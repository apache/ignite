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
    using System.Web;
    using System.Web.SessionState;

    /// <summary>
    /// ASP.NET Session-State Store Provider that use Ignite distributed cache as an underlying storage.
    /// </summary>
    public class IgniteSessionStateStoreProvider : SessionStateStoreProviderBase
    {
        public override void Dispose()
        {
            throw new NotImplementedException();
        }

        public override bool SetItemExpireCallback(SessionStateItemExpireCallback expireCallback)
        {
            throw new NotImplementedException();
        }

        public override void InitializeRequest(HttpContext context)
        {
            throw new NotImplementedException();
        }

        public override SessionStateStoreData GetItem(HttpContext context, string id, out bool locked, out TimeSpan lockAge, out object lockId,
            out SessionStateActions actions)
        {
            throw new NotImplementedException();
        }

        public override SessionStateStoreData GetItemExclusive(HttpContext context, string id, out bool locked, out TimeSpan lockAge,
            out object lockId, out SessionStateActions actions)
        {
            throw new NotImplementedException();
        }

        public override void ReleaseItemExclusive(HttpContext context, string id, object lockId)
        {
            throw new NotImplementedException();
        }

        public override void SetAndReleaseItemExclusive(HttpContext context, string id, SessionStateStoreData item, object lockId, bool newItem)
        {
            throw new NotImplementedException();
        }

        public override void RemoveItem(HttpContext context, string id, object lockId, SessionStateStoreData item)
        {
            throw new NotImplementedException();
        }

        public override void ResetItemTimeout(HttpContext context, string id)
        {
            throw new NotImplementedException();
        }

        public override SessionStateStoreData CreateNewStoreData(HttpContext context, int timeout)
        {
            throw new NotImplementedException();
        }

        public override void CreateUninitializedItem(HttpContext context, string id, int timeout)
        {
            throw new NotImplementedException();
        }

        public override void EndRequest(HttpContext context)
        {
            throw new NotImplementedException();
        }
    }
}
