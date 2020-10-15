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

using System;
using Apache.Ignite.Core.Cache.Configuration;
using Apache.Ignite.Core.Cache.Expiry;
using Apache.Ignite.Core.Common;

namespace dotnet_helloworld
{
    class ExpiryPolicies
    {

        // tag::cfg[]
        class ExpiryPolicyFactoryImpl : IFactory<IExpiryPolicy>
        {
            public IExpiryPolicy CreateInstance()
            {
                return new ExpiryPolicy(TimeSpan.FromMilliseconds(100), TimeSpan.FromMilliseconds(100),
                    TimeSpan.FromMilliseconds(100));
            }
        }

        public static void Example()
        {
            var cfg = new CacheConfiguration
            {
                Name = "cache_name",
                ExpiryPolicyFactory = new ExpiryPolicyFactoryImpl()
            };
            // end::cfg[]
        }

        public static void EagerTtl()
        {
            // tag::eagerTTL[]
            var cfg = new CacheConfiguration
            {
                Name = "cache_name",
                EagerTtl = true
            };
            // end::eagerTTL[]
        }

    }
}
