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
    /// Grid swap space event.
    /// </summary>
    public sealed class SwapSpaceEvent : EventBase
	{
        /** */
        private readonly string _space;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="r">The reader to read data from.</param>
        internal SwapSpaceEvent(IPortableRawReader r) : base(r)
        {
            _space = r.ReadString();
        }
		
        /// <summary>
        /// Gets swap space name. 
        /// </summary>
        public string Space { get { return _space; } }

        /** <inheritDoc /> */
	    public override string ToShortString()
	    {
	        return string.Format("{0}: Space={1}", Name, Space);
	    }
    }
}
