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

namespace Apache.Ignite.Core.Impl.Services
{
    using System.Collections;
    using System.Threading;
    using Apache.Ignite.Core.Services;

    internal class ServiceProxyContextImpl : ServiceProxyContext
    {
        private readonly Hashtable _attrs;
        
        internal ServiceProxyContextImpl(Hashtable attrs)
        {
            _attrs = attrs;
        }
        
        public override object Attribute(string name)
        {
            return _attrs[name];
        }

        internal static void Current(Hashtable attrs)
        {
            int threadId = Thread.CurrentThread.ManagedThreadId;

            if (attrs == null)
                ((IDictionary) ProxyCtxs).Remove(threadId);
            else
                ProxyCtxs.AddOrUpdate(threadId, attrs, (k, v) => attrs);
        }

        internal Hashtable Values()
        {
            return _attrs;
        }
    }
}