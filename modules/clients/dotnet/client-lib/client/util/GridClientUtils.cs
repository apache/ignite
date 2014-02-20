// @csharp.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Client.Util {
    using System;
    using System.Threading;
    using System.Collections;
    using System.Collections.Generic;

    using A = GridGain.Client.Util.GridClientArgumentCheck;
    using GridGain.Client.Impl;

    /** <summary>Java client utils.</summary> */
    internal static class GridClientUtils {
        /** <summary>Conversion from Java UUID format into C# Guid.</summary> */
        //          0 1 2 3  4 5  6 7  8 9  a b c d e f
        //   orig: 2ec84557-f7c4-4a2e-aea8-251eb13acff3
        //          3 2 1 0  5 4  7 6  8 9  a b c d e f
        // parsed: 5745c82e-c4f7-2e4a-aea8-251eb13acff3
        private static readonly byte[] JAVA_GUID_CONV = new byte[] { 3, 2, 1, 0, 5, 4, 7, 6, 8, 9, 10, 11, 12, 13, 14, 15 };

        /** <summary>Background task execution threads counter.</summary> */
        private static long asyncThreadsCounter;

        /**
         * <summary>
         * Converts Guid to a bytes array in Java format.</summary>
         *
         * <param name="id">Guid to convert.</param>
         * <returns>Converted Guid bytes in Java format.</returns>
         */
        public static byte[] ToBytes(Guid id) {
            return ToBytes(id, JAVA_GUID_CONV);
        }

        /**
         * <summary>
         * Converts Guid to a bytes array in specified format.</summary>
         *
         * <param name="id">Guid to convert.</param>
         * <param name="format">UUID bytes format.</param>
         * <returns>Converted Guid bytes in Java format.</returns>
         */
        internal static byte[] ToBytes(Guid id, byte[] format) {
            byte[] cs = id.ToByteArray();
            byte[] java = new byte[format.Length];

            // Convert to Java format
            for (int i = 0; i < format.Length; i++)
                java[i] = cs[format[i]];

            return java;
        }

        /**
         * <summary>
         * Converts bytes array representation of Java UUID into C# Guid.</summary>
         *
         * <param name="java">Java UUID as bytes array.</param>
         * <param name="off">Offset in the bytes array.</param>
         * <returns>C# Guid.</returns>
         */
        public static Guid BytesToGuid(byte[] java, int off) {
            return BytesToGuid(java, off, JAVA_GUID_CONV);
        }

        /**
         * <summary>
         * Converts bytes array representation of specified format UUID into C# Guid.</summary>
         *
         * <param name="java">UUID as bytes array.</param>
         * <param name="off">Offset in the bytes array.</param>
         * <param name="format">UUID bytes format.</param>
         * <returns>C# Guid.</returns>
         */
        internal static Guid BytesToGuid(byte[] java, int off, byte[] format) {
            byte[] cs = new byte[format.Length];

            // Convert from Java format
            for (int i = 0; i < format.Length; i++)
                cs[format[i]] = java[i + off];

            return new Guid(cs);
        }

        /**
         * <summary>
         * Converts primitive <c>short</c> type to byte array.</summary>
         *
         * <param name="val">Short value.</param>
         * <returns>Array of bytes.</returns>
         */
        public static byte[] ToBytes(short val) {
            return Invert(BitConverter.GetBytes(val));
        }

        /**
         * <summary>
         * Converts primitive <c>int</c> type to byte array.</summary>
         *
         * <param name="val">Integer value.</param>
         * <returns>Array of bytes.</returns>
         */
        public static byte[] ToBytes(int val) {
            return Invert(BitConverter.GetBytes(val));
        }

        /**
         * <summary>
         * Converts primitive <c>long</c> type to byte array.</summary>
         *
         * <param name="val">Long value.</param>
         * <returns>Array of bytes.</returns>
         */
        public static byte[] ToBytes(long val) {
            return Invert(BitConverter.GetBytes(val));
        }

        /**
         * <summary>
         * Converts primitive <c>float</c> type to byte array.</summary>
         *
         * <param name="val">Float value.</param>
         * <returns>Array of bytes.</returns>
         */
        public static byte[] ToBytes(float val) {
            return Invert(BitConverter.GetBytes(val));
        }

        /**
         * <summary>
         * Converts primitive <c>double</c> type to byte array.</summary>
         *
         * <param name="val">Double value.</param>
         * <returns>Array of bytes.</returns>
         */
        public static byte[] ToBytes(double val) {
            return Invert(BitConverter.GetBytes(val));
        }

        /**
         * <summary>
         * Constructs <c>short</c> from byte array.</summary>
         *
         * <param name="buf">Array of bytes.</param>
         * <param name="off">Offset in <c>bytes</c> array.</param>
         * <returns>Short value.</returns>
         */
        public static short BytesToInt16(byte[] buf, int off) {
            return BitConverter.ToInt16(Invert(buf, off, 2), 0);
        }

        /**
         * <summary>
         * Constructs <c>int</c> from byte array.</summary>
         *
         * <param name="buf">Array of bytes.</param>
         * <param name="off">Offset in <c>bytes</c> array.</param>
         * <returns>Integer value.</returns>
         */
        public static int BytesToInt32(byte[] buf, int off) {
            return BitConverter.ToInt32(Invert(buf, off, 4), 0);
        }

        /**
         * <summary>
         * Constructs <c>long</c> from byte array.</summary>
         *
         * <param name="buf">Array of bytes.</param>
         * <param name="off">Offset in <c>bytes</c> array.</param>
         * <returns>Long value.</returns>
         */
        public static long BytesToInt64(byte[] buf, int off) {
            return BitConverter.ToInt64(Invert(buf, off, 8), 0);
        }

        /**
         * <summary>
         * Constructs <c>float</c> from byte array.</summary>
         *
         * <param name="buf">Array of bytes.</param>
         * <param name="off">Offset in <c>bytes</c> array.</param>
         * <returns>Float value.</returns>
         */
        public static float BytesToSingle(byte[] buf, int off) {
            return BitConverter.ToSingle(Invert(buf, off, 4), 0);
        }

        /**
         * <summary>
         * Constructs <c>double</c> from byte array.</summary>
         *
         * <param name="buf">Array of bytes.</param>
         * <param name="off">Offset in <c>bytes</c> array.</param>
         * <returns>Double value.</returns>
         */
        public static double BytesToDouble(byte[] buf, int off) {
            return BitConverter.ToDouble(Invert(buf, off, 8), 0);
        }

        /**
         * <summary>
         * Invert part of the bytes array.</summary>
         *
         * <param name="buf">Bytes buffer to cut an inverted part from.</param>
         * <param name="off">Start offset in the source buffer.</param>
         * <param name="limit">The number of bytes to cut from the source buffer.</param>
         * <returns>Inverted part of the source buffer.</returns>
         */
        private static byte[] Invert(byte[] buf, int off, int limit) {
            byte[] bin = new byte[limit];

            for (int i = 0; i < limit; i++)
                bin[limit - i - 1] = buf[off + i];

            return bin;
        }

        /**
         * <summary>
         * Invert array direction.</summary>
         *
         * <param name="buf">Bytes array to invert.</param>
         * <returns>Inverted bytes array.</returns>
         */
        private static byte[] Invert(byte[] buf) {
            if (!BitConverter.IsLittleEndian)
                return buf;

            byte[] res = new byte[buf.Length];

            for (int i = 0, s = buf.Length; i < s; i++)
                res[s - i - 1] = buf[i];

            return res;
        }

        /**
         * <summary>
         * Applies filter and returns filtered collection of nodes.</summary>
         *
         * <param name="elements">Nodes to be filtered.</param>
         * <param name="filter">Filter to apply</param>
         * <returns>Filtered collection.</returns>
         */
        public static IList<T> ApplyFilter<T>(IEnumerable<T> elements, Predicate<T> filter) where T : class {
            A.NotNull(filter, "filter");

            IList<T> res = new List<T>();

            foreach (T e in elements)
                if (filter(e))
                    res.Add(e);

            return res;
        }

        /**
         * <summary>
         * All predicate.</summary>
         *
         * <returns>Always true-result predicate.</returns>
         */
        public static Predicate<T> All<T>() {
            return delegate(T t) {
                return true;
            };
        }

        /**
         * <summary>
         * AND predicate. Passes if and only if both provided filters accept the node.
         * This filter uses short-term condition evaluation, i.e. second filter would not
         * be invoked if first filter returned <c>false</c>.</summary>
         *
         * <param name="first">First filter to check.</param>
         * <param name="second">Second filter to check.</param>
         * <returns>Conjunction predicate.</returns>
         */
        public static Predicate<T> And<T>(Predicate<T> first, Predicate<T> second) {
            A.NotNull(first, "first");
            A.NotNull(second, "second");

            return delegate(T t) {
                return first(t) && second(t);
            };
        }

        /** <summary>Filter predicate.</summary> */
        public static Predicate<T> Filter<T>(ICollection<T> inc, ICollection<T> exc) {
            return delegate(T t) {
                return (inc == null || inc.Contains(t)) && (exc == null || !exc.Contains(t));
            };
        }

        /**
         * <summary>
         * Creates a predicates that checks if given value is contained in the items collection.</summary>
         *
         * <param name="items">Collection of valid items.</param>
         * <returns>Predicate.</returns>
         */
        public static Predicate<T> Contains<T>(ICollection<T> items) {
            return delegate(T t) {
                return items != null && items.Count > 0 && items.Contains(t);
            };
        }

        /**
         * <summary>
         * Do job and hide exception if happens.</summary>
         *
         * <param name="job">Job to execute.</param>
         * <param name="handler">Exception handler.</param>
         */
        public static void DoSilent<TException>(Action job, Action<TException> handler) where TException : Exception {
            try {
                job();
            } catch(TException e) {
                if (handler != null)
                    handler(e);
            }
        }

        /** <summary>Gets current date and time on this computer, expressed as the local time.</summary> */
        public static DateTime Now {
            get {
                return System.DateTime.Now;
            }
        }

        /** <summary>Unix epoch start point "1970-01-01 00:00:00.0 UTC" as the local time.</summary> */
        public static DateTime Epoch {
            get {
                return new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc).ToLocalTime();
            }
        }

        /**
         * <summary>
         * Convert unix timestamp (ms) into the DateTime object.</summary>
         *
         * <returns>DateTime object representing passed unix timestamp (ms).</returns>
         */
        public static DateTime Timestamp(long timestamp) {
            return Epoch.AddMilliseconds(timestamp);
        }

        /**
         * <summary>
         * Blocks the current thread for a specified time.</summary>
         *
         * <param name="delay">Amount of time for which the thread is blocked.</param>
         * <returns>true if thread sleep was finished successfully, false if thread sleep was interrupted.</returns>
         */
        public static bool Sleep(TimeSpan delay) {
            try {
                Thread.Sleep(delay);

                return true;
            }
            catch (ThreadInterruptedException) {
                return false;
            }
        }

        /**
         * <summary>
         * Convert list of arguments into the list.</summary>
         *
         * <param name="array">Variable arguments collection of items to add into list.</param>
         * <returns>List of the passed items.</returns>
         */
        public static IList<T> List<T>(params T[] array) {
            if (array == null)
                throw new ArgumentException("No values to add to the list (possibly null argument not casted to item type?).");

            IList<T> list = new List<T>();

            foreach (T item in array)
                list.Add(item);

            return list;
        }

        /**
         * <summary>
         * Execute function in background thread.</summary>
         *
         * <param name="func">Function to execute in background thread.</param>
         * <returns>Future to get function result.</returns>
         */
        public static IGridClientFuture<T> Async<T>(Func<T> func) {
            var fut = new GridClientFuture<T>();

            var thread = new Thread(() => {
                try {
                    fut.Done(func());
                }
                catch (Exception e) {
                    fut.Fail(() => {
                        throw new GridClientException(e.Message, e);
                    });
                }
            });

            thread.Name = "grid-client--async-worker-" + Interlocked.Increment(ref asyncThreadsCounter);

            thread.Start();

            return fut;
        }

        /**
         * <summary>
         * Execute action in background thread.</summary>
         *
         * <param name="action">Action to execute in background thread.</param>
         * <returns>Future to get action result.</returns>
         */
        public static IGridClientFuture Async(Action action) {
            return Async<int>(() => {
                action();

                return 0;
            });
        }

        //
        // Extensions
        //

        /**
         * <summary>
         * Add all items from the passed collection to this one.</summary>
         *
         * <param name="collection">This collection to add new items to.</param>
         * <param name="newItems">Collection of items to add to this one.</param>
         */
        public static void AddAll<T>(this ICollection<T> collection, IEnumerable<T> newItems) {
            foreach (T item in newItems)
                collection.Add(item);
        }

        /**
         * <summary>
         * Extend generic maps with conversions into not-generic ones.</summary>
         *
         * <param name="source">Generic map to convert into not-generic one.</param>
         * <returns>Not-generic map.</returns>
         */
        public static IDictionary ToMap<TKey, TVal>(this IDictionary<TKey, TVal> source) {
            IDictionary map = new Hashtable();

            foreach (KeyValuePair<TKey, TVal> pair in source)
                map.Add(pair.Key, pair.Value);

            return map;
        }

        /**
         * <summary>
         * Extend base maps with conversions into generic ones.</summary>
         *
         * <param name="source">Not-generic map to convert into generic one.</param>
         * <returns>Generic map.</returns>
         */
        public static IDictionary<TKey, TVal> ToMap<TKey, TVal>(this IDictionary source) {
            IDictionary<TKey, TVal> map = new Dictionary<TKey, TVal>();

            foreach (DictionaryEntry entry in source)
                map.Add((TKey)entry.Key, (TVal)entry.Value);

            return map;
        }

        /**
         * <summary>
         * Wrap specified map with null-key allowed dictionary (if required).</summary>
         *
         * <param name="source">Map to wrap with null-key allowed one.</param>
         * <returns>Null-key allowed map.</returns>
         */
        public static IDictionary<TKey, TVal> ToNullable<TKey, TVal>(this IDictionary<TKey, TVal> source) {
            if (source is GridClientNullDictionary<TKey, TVal>)
                return source;

            return new GridClientNullDictionary<TKey, TVal>(source);
        }
    }
}
