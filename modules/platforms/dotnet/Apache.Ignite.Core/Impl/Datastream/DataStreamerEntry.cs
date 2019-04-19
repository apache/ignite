/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 * 
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

namespace Apache.Ignite.Core.Impl.Datastream
{
    /// <summary>
    /// Data streamer entry.
    /// </summary>
    internal class DataStreamerEntry<TK, TV>
    {
        /** Key. */
        private readonly TK _key;

        /** Value. */
        private readonly TV _val;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="key">Key.</param>
        /// <param name="val">Value.</param>
        public DataStreamerEntry(TK key, TV val)
        {
            _key = key;
            _val = val;
        }

        /// <summary>
        /// Key.
        /// </summary>
        public TK Key
        {
            get
            {
                return _key;
            }
        }

        /// <summary>
        /// Value.
        /// </summary>
        public TV Value
        {
            get
            {
                return _val;
            }
        }
    }
}
