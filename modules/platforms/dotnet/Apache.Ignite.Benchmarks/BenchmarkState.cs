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
    internal class BenchmarkState
    {
        /** Warmup flag. */
        private bool _warmup = true;

        /** Counter within the batch. */
        private int _counter;

        /** Array of attached objects. */
        private object[] _attachedObjects;

        /// <summary>
        /// Reset state.
        /// </summary>
        public void Reset()
        {
            _counter = 0;
            _attachedObjects = null;
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
            _counter++;
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
            get { return _counter; }
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
                return (_attachedObjects == null || idx >= _attachedObjects.Length) ? null : _attachedObjects[idx];
            }

            set
            {
                if (_attachedObjects == null)
                    _attachedObjects = new object[idx + 1];
                else if (idx >= _attachedObjects.Length)
                {
                    var arr0 = new object[idx + 1];

                    Array.Copy(_attachedObjects, 0, arr0, 0, arr0.Length);

                    _attachedObjects = arr0;
                }

                _attachedObjects[idx] = value;
            }
        }
    }
}
