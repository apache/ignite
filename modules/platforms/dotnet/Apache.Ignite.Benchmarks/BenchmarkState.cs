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
