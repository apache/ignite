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

namespace Apache.Ignite.Core.Client.Datastream
{
    using System;
    using System.Collections.Generic;
    using System.Threading.Tasks;

    /// <summary>
    ///
    /// </summary>
    public interface IDataStreamerClient<TK, TV> : IDisposable
    {
        string CacheName { get; }

        DataStreamerClientOptions<TK, TV> Options { get; }

        // TODO: We don't need async overloads - flushing should only happen in the background
        void Add(TK key, TV val);

        void Add(IEnumerable<KeyValuePair<TK, TV>> entries);

        void Remove(TK key);

        void Remove(IEnumerable<TK> keys);

        void Flush();

        Task FlushAsync();
    }
}
