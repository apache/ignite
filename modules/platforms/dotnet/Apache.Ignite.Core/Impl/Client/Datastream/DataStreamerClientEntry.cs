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

namespace Apache.Ignite.Core.Impl.Client.Datastream
{
    /// <summary>
    /// Streamer entry.
    /// </summary>
    internal struct DataStreamerClientEntry<TK, TV>
    {
        /** */
        private const byte StatusEmpty = 0;
        
        /** */
        private const byte StatusAdd = 1;
        
        /** */
        private const byte StatusRemove = 2;

        /** */
        private readonly TK _key;

        /** */
        private readonly TV _val;

        /** */
        private readonly byte _status;

        /// <summary>
        /// Initializes a new instance of <see cref="DataStreamerClientEntry{TK,TV}"/> struct.
        /// </summary>
        public DataStreamerClientEntry(TK key, TV val)
        {
            _key = key;
            _val = val;
            _status = StatusAdd;
        }

        /// <summary>
        /// Initializes a new instance of <see cref="DataStreamerClientEntry{TK,TV}"/> struct.
        /// </summary>
        public DataStreamerClientEntry(TK key)
        {
            _key = key;
            _val = default(TV);
            _status = StatusRemove;
        }

        /// <summary>
        /// Gets the key.
        /// </summary>
        public TK Key
        {
            get { return _key; }
        }

        /// <summary>
        /// Gets the value.
        /// </summary>
        public TV Val
        {
            get { return _val; }
        }

        /// <summary>
        /// Gets a value indicating whether this entry is marked for removal
        /// </summary>
        public bool Remove
        {
            get { return _status == StatusRemove; }
        }

        /// <summary>
        /// Gets a value indicating whether this entry is empty.
        /// </summary>
        public bool IsEmpty
        {
            get { return _status == StatusEmpty; }
        }

        /** <inheritdoc /> */
        public override string ToString()
        {
            return string.Format("DataStreamerClientEntry [Key={0}, Val={1}, Remove={2}, IsEmpty={3}]", Key, Val,
                Remove, IsEmpty);
        }
    }
}
