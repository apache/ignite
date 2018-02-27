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

namespace Apache.Ignite.ExamplesDll.Compute
{
    using Apache.Ignite.Core.Compute;

    /// <summary>
    /// Character count reducer which collects individual string lengths and aggregate them.
    /// </summary>
    public class CharacterCountReducer : IComputeReducer<int, int>
    {
        /// <summary> Total length. </summary>
        private int _length;

        /// <summary>
        /// Collect character counts of distinct words.
        /// </summary>
        /// <param name="res">Character count of a distinct word.</param>
        /// <returns><c>True</c> to continue collecting results until all closures are finished.</returns>
        public bool Collect(int res)
        {
            _length += res;

            return true;
        }

        /// <summary>
        /// Reduce all collected results.
        /// </summary>
        /// <returns>Total character count.</returns>
        public int Reduce()
        {
            return _length;
        }
    }
}
