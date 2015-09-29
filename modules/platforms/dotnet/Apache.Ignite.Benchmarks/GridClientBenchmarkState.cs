/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Client.Benchmark
{
    using System;

    /// <summary>
    /// Benchmark state.
    /// </summary>
    public class GridClientBenchmarkState
    {
        /** Warmup flag. */
        private bool warmup = true;

        /** Counter within the batch. */
        private int ctr;

        /** Array of attached objects. */
        private object[] arr;

        /// <summary>
        /// Reset state.
        /// </summary>
        public void Reset()
        {
            ctr = 0;
            arr = null;
        }

        /// <summary>
        /// Clear warmup flag.
        /// </summary>
        public void StopWarmup()
        {
            warmup = false;
        }

        /// <summary>
        /// Increment counter.
        /// </summary>
        public void IncrementCounter()
        {
            ctr++;
        }

        /// <summary>
        /// Warmup flag.
        /// </summary>
        public bool Warmup
        {
            get { return warmup; }
        }

        /// <summary>
        /// Counter within the batch.
        /// </summary>
        public int Counter
        {
            get { return ctr; }
        }

        /// <summary>
        /// Get/set attached object.
        /// </summary>
        /// <param name="idx">Index.</param>
        /// <returns>Attahced object.</returns>
        public object this[int idx]
        {
            get
            {
                return (arr == null || idx >= arr.Length) ? null : arr[idx];
            }

            set
            {
                if (arr == null)
                    arr = new object[idx + 1];
                else if (idx >= arr.Length)
                {
                    object[] arr0 = new object[idx + 1];

                    Array.Copy(arr, 0, arr0, 0, arr0.Length);

                    arr = arr0;
                }

                arr[idx] = value;
            }
        }
    }
}
