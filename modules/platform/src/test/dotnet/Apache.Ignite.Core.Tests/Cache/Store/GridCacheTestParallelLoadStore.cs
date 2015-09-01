/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Cache.Store
{
    using System.Collections;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;

    /// <summary>
    /// Test cache store with parallel load.
    /// </summary>
    public class GridCacheTestParallelLoadStore : CacheParallelLoadStoreAdapter
    {
        /** Length of input data sequence */
        public const int INPUT_DATA_LENGTH = 10000;

        /** list of thread ids where Parse has been executed */
        private static readonly ConcurrentDictionary<int, int> THREAD_IDS = new ConcurrentDictionary<int, int>();

        /// <summary>
        /// Gets the count of unique threads that entered Parse method.
        /// </summary>
        public static int UniqueThreadCount
        {
            get { return THREAD_IDS.Count; }
        }

        /// <summary>
        /// Resets the test counters.
        /// </summary>
        public static void ResetCounters()
        {
            THREAD_IDS.Clear();
        }

        /** <inheritdoc /> */
        protected override IEnumerable GetInputData()
        {
            return Enumerable.Range(0, INPUT_DATA_LENGTH).Select(x => new Record {Id = x, Name = "Test Record " + x});
        }

        /** <inheritdoc /> */
        protected override KeyValuePair<object, object>? Parse(object inputRecord, params object[] args)
        {
            var threadId = Thread.CurrentThread.ManagedThreadId;
            THREAD_IDS.GetOrAdd(threadId, threadId);

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