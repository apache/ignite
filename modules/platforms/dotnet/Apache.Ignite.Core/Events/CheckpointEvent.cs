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

namespace Apache.Ignite.Core.Events
{
    using System.Globalization;
    using Apache.Ignite.Core.Binary;

    /// <summary>
    /// Grid checkpoint event.
    /// </summary>
    public sealed class CheckpointEvent : EventBase
	{
        /** */
        private readonly string _key;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="r">The reader to read data from.</param>
        internal CheckpointEvent(IBinaryRawReader r) : base(r)
        {
            _key = r.ReadString();
        }
		
        /// <summary>
        /// Gets checkpoint key associated with this event. 
        /// </summary>
        public string Key { get { return _key; } }

        /// <summary>
        /// Gets shortened version of ToString result.
        /// </summary>
	    public override string ToShortString()
	    {
            return string.Format(CultureInfo.InvariantCulture, "{0}: Key={1}", Name, Key);
	    }
    }
}
