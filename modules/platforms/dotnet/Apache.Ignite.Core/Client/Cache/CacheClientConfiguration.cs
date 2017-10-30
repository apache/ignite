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

namespace Apache.Ignite.Core.Client.Cache
{
    using Apache.Ignite.Core.Cache.Configuration;

    /// <summary>
    /// Ignite client cache configuration.
    /// Same thing as <see cref="CacheConfiguration"/>, but with a subset of properties that can be accessed from
    /// Ignite thin client (see <see cref="IIgniteClient"/>).
    /// <para />
    /// Note that caches created from server nodes can be accessed from thin client, and vice versa.
    /// The only difference is that thin client can not read or write certain <see cref="CacheConfiguration"/>
    /// properties, so a separate class exists to make it clear which properties can be used.
    /// </summary>
    public class CacheClientConfiguration
    {
    }
}
