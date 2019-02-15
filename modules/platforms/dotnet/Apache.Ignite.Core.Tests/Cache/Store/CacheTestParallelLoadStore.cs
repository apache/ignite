/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

namespace Apache.Ignite.Core.Tests.Cache.Store
{
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;
    using Apache.Ignite.Core.Cache.Store;

    /// <summary>
    /// Test cache store with parallel load.
    /// </summary>
    public class CacheTestParallelLoadStore : 
        CacheParallelLoadStoreAdapter<object, object, CacheTestParallelLoadStore.Record>
    {
        /** Length of input data sequence */
        public const int InputDataLength = 10000;

        /** list of thread ids where Parse has been executed */
        private static readonly ConcurrentDictionary<int, int> ThreadIds = new ConcurrentDictionary<int, int>();

        /// <summary>
        /// Initializes a new instance of the <see cref="CacheTestParallelLoadStore"/> class.
        /// </summary>
        public CacheTestParallelLoadStore()
        {
            MaxDegreeOfParallelism -= 1;
        }

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
        protected override IEnumerable<Record> GetInputData()
        {
            return Enumerable.Range(0, InputDataLength).Select(x => new Record {Id = x, Name = "Test Record " + x});
        }

        /** <inheritdoc /> */
        protected override KeyValuePair<object, object>? Parse(Record inputRecord, params object[] args)
        {
            var threadId = Thread.CurrentThread.ManagedThreadId;
            ThreadIds.GetOrAdd(threadId, threadId);

            var minId = (int)args[0];

            return inputRecord.Id >= minId
                ? new KeyValuePair<object, object>(inputRecord.Id, inputRecord)
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
            // ReSharper disable once UnusedAutoPropertyAccessor.Global
            public string Name { get; set; }
        }
    }
}