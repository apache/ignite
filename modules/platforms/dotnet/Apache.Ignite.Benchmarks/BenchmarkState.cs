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

namespace Apache.Ignite.Benchmarks
{
    using System;

    /// <summary>
    /// Benchmark state.
    /// </summary>
    public class BenchmarkState
    {
        /** Warmup flag. */
        private bool _warmup = true;

        /** Counter within the batch. */
        private int _ctr;

        /** Array of attached objects. */
        private object[] _arr;

        /// <summary>
        /// Reset state.
        /// </summary>
        public void Reset()
        {
            _ctr = 0;
            _arr = null;
        }

        /// <summary>
        /// Clear warmup flag.
        /// </summary>
        public void StopWarmup()
        {
            _warmup = false;
        }

        /// <summary>
        /// Increment counter.
        /// </summary>
        public void IncrementCounter()
        {
            _ctr++;
        }

        /// <summary>
        /// Warmup flag.
        /// </summary>
        public bool Warmup
        {
            get { return _warmup; }
        }

        /// <summary>
        /// Counter within the batch.
        /// </summary>
        public int Counter
        {
            get { return _ctr; }
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
                return (_arr == null || idx >= _arr.Length) ? null : _arr[idx];
            }

            set
            {
                if (_arr == null)
                    _arr = new object[idx + 1];
                else if (idx >= _arr.Length)
                {
                    object[] arr0 = new object[idx + 1];

                    Array.Copy(_arr, 0, arr0, 0, arr0.Length);

                    _arr = arr0;
                }

                _arr[idx] = value;
            }
        }
    }
}
