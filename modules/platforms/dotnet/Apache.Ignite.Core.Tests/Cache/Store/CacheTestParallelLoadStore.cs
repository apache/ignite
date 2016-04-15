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

namespace Apache.Ignite.Core.Tests.Cache.Store
{
    using System.Collections;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;
    using Apache.Ignite.Core.Cache.Store;

    /// <summary>
    /// Test cache store with parallel load.
    /// </summary>
    public class CacheTestParallelLoadStore : CacheParallelLoadStoreAdapter
    {
        /** Length of input data sequence */
        public const int InputDataLength = 10000;

        /** list of thread ids where Parse has been executed */
        private static readonly ConcurrentDictionary<int, int> ThreadIds = new ConcurrentDictionary<int, int>();

        /// <summary>
        /// Gets the count of unique threads that entered Parse method.
        /// </summary>
        public static int UniqueThreadCount
        {
            get { return ThreadIds.Count; }
        }

        /// <summary>
        /// Resets the test counters.
        /// </summary>
        public static void ResetCounters()
        {
            ThreadIds.Clear();
        }

        /** <inheritdoc /> */
        protected override IEnumerable GetInputData()
        {
            return Enumerable.Range(0, InputDataLength).Select(x => new Record {Id = x, Name = "Test Record " + x});
        }

        /** <inheritdoc /> */
        protected override KeyValuePair<object, object>? Parse(object inputRecord, params object[] args)
        {
            var threadId = Thread.CurrentThread.ManagedThreadId;
            ThreadIds.GetOrAdd(threadId, threadId);

            var minId = (int)args[0];

            var rec = (Record)inputRecord;

            return rec.Id >= minId
                ? new KeyValuePair<object, object>(rec.Id, rec)
                : (KeyValuePair<object, object>?) null;
        }

        /// <summary>
        /// Test store record.
        /// </summary>
        public class Record
        {
            /// <summary>
            /// Gets or sets the identifier.
            /// </summary>
            public int Id { get; set; }

            /// <summary>
            /// Gets or sets the name.
            /// </summary>
            public string Name { get; set; }
        }
    }
}