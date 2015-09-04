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

namespace Apache.Ignite.Core.Events
{
    using Apache.Ignite.Core.Portable;

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
        internal CheckpointEvent(IPortableRawReader r) : base(r)
        {
            _key = r.ReadString();
        }
		
        /// <summary>
        /// Gets checkpoint key associated with this event. 
        /// </summary>
        public string Key { get { return _key; } }

        /** <inheritDoc /> */
	    public override string ToShortString()
	    {
	        return string.Format("{0}: Key={1}", Name, Key);
	    }
    }
}
