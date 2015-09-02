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
    using System;
    using Apache.Ignite.Core.Portable;

    /// <summary>
    /// Cache query read event.
    /// </summary>
    public sealed class CacheQueryReadEvent : EventBase
	{
        /** */
        private readonly string _queryType;

        /** */
        private readonly string _cacheName;

        /** */
        private readonly string _className;

        /** */
        private readonly string _clause;

        /** */
        private readonly Guid _subjectId;

        /** */
        private readonly string _taskName;

        /** */
        private readonly object _key;

        /** */
        private readonly object _value;

        /** */
        private readonly object _oldValue;

        /** */
        private readonly object _row;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="r">The reader to read data from.</param>
        internal CacheQueryReadEvent(IPortableRawReader r) : base(r)
        {
            _queryType = r.ReadString();
            _cacheName = r.ReadString();
            _className = r.ReadString();
            _clause = r.ReadString();
            _subjectId = r.ReadGuid() ?? Guid.Empty;
            _taskName = r.ReadString();
            _key = r.ReadObject<object>();
            _value = r.ReadObject<object>();
            _oldValue = r.ReadObject<object>();
            _row = r.ReadObject<object>();
        }
		
        /// <summary>
        /// Gets query type. 
        /// </summary>
        public string QueryType { get { return _queryType; } }

        /// <summary>
        /// Gets cache name on which query was executed. 
        /// </summary>
        public string CacheName { get { return _cacheName; } }

        /// <summary>
        /// Gets queried class name. Applicable for SQL and full text queries. 
        /// </summary>
        public string ClassName { get { return _className; } }

        /// <summary>
        /// Gets query clause. Applicable for SQL, SQL fields and full text queries. 
        /// </summary>
        public string Clause { get { return _clause; } }

        /// <summary>
        /// Gets security subject ID. 
        /// </summary>
        public Guid SubjectId { get { return _subjectId; } }

        /// <summary>
        /// Gets the name of the task that executed the query (if any). 
        /// </summary>
        public string TaskName { get { return _taskName; } }

        /// <summary>
        /// Gets read entry key. 
        /// </summary>
        public object Key { get { return _key; } }

        /// <summary>
        /// Gets read entry value. 
        /// </summary>
        public object Value { get { return _value; } }

        /// <summary>
        /// Gets read entry old value (applicable for continuous queries). 
        /// </summary>
        public object OldValue { get { return _oldValue; } }

        /// <summary>
        /// Gets read results set row. 
        /// </summary>
        public object Row { get { return _row; } }

        /** <inheritDoc /> */
	    public override string ToShortString()
	    {
	        return string.Format("{0}: QueryType={1}, CacheName={2}, ClassName={3}, Clause={4}, SubjectId={5}, " +
	                             "TaskName={6}, Key={7}, Value={8}, OldValue={9}, Row={10}", Name, QueryType, 
                                 CacheName, ClassName, Clause, SubjectId, TaskName, Key, Value, OldValue, Row);
	    }
    }
}
