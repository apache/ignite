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
    using Apache.Ignite.Core.Cluster;
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Portable;

    /// <summary>
    /// In-memory database (cache) event.
    /// </summary>
    public sealed class CacheEvent : EventBase
	{
        /** */
        private readonly string _cacheName;

        /** */
        private readonly int _partition;

        /** */
        private readonly bool _isNear;

        /** */
        private readonly IClusterNode _eventNode;

        /** */
        private readonly object _key;

        /** */
        private readonly IgniteGuid _xid;

        /** */
        private readonly object _lockId;

        /** */
        private readonly object _newValue;

        /** */
        private readonly object _oldValue;

        /** */
        private readonly bool _hasOldValue;

        /** */
        private readonly bool _hasNewValue;

        /** */
        private readonly Guid _subjectId;

        /** */
        private readonly string _closureClassName;

        /** */
        private readonly string _taskName;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="r">The reader to read data from.</param>
        internal CacheEvent(IPortableRawReader r) : base(r)
        {
            _cacheName = r.ReadString();
            _partition = r.ReadInt();
            _isNear = r.ReadBoolean();
            _eventNode = ReadNode(r);
            _key = r.ReadObject<object>();
            _xid = IgniteGuid.ReadPortable(r);
            _lockId = r.ReadObject<object>();
            _newValue = r.ReadObject<object>();
            _oldValue = r.ReadObject<object>();
            _hasOldValue = r.ReadBoolean();
            _hasNewValue = r.ReadBoolean();
            _subjectId = r.ReadGuid() ?? Guid.Empty;
            _closureClassName = r.ReadString();
            _taskName = r.ReadString();
        }
		
        /// <summary>
        /// Gets cache name. 
        /// </summary>
        public string CacheName { get { return _cacheName; } }

        /// <summary>
        /// Gets partition for the event which is the partition the key belongs to. 
        /// </summary>
        public int Partition { get { return _partition; } }

        /// <summary>
        /// Gets flag indicating whether event happened on near or partitioned cache. 
        /// </summary>
        public bool IsNear { get { return _isNear; } }

        /// <summary>
        /// Gets node which initiated cache operation or null if that node is not available. 
        /// </summary>
        public IClusterNode EventNode { get { return _eventNode; } }

        /// <summary>
        /// Gets cache entry associated with event. 
        /// </summary>
        public object Key { get { return _key; } }

        /// <summary>
        /// ID of surrounding cache cache transaction or null if there is no surrounding transaction. 
        /// </summary>
        public IgniteGuid Xid { get { return _xid; } }

        /// <summary>
        /// ID of the lock if held or null if no lock held. 
        /// </summary>
        public object LockId { get { return _lockId; } }

        /// <summary>
        /// Gets new value for this event. 
        /// </summary>
        public object NewValue { get { return _newValue; } }

        /// <summary>
        /// Gets old value associated with this event. 
        /// </summary>
        public object OldValue { get { return _oldValue; } }

        /// <summary>
        /// Gets flag indicating whether cache entry has old value in case if we only have old value in serialized form 
        /// in which case <see cref="OldValue" /> will return null. 
        /// </summary>
        public bool HasOldValue { get { return _hasOldValue; } }

        /// <summary>
        /// Gets flag indicating whether cache entry has new value in case if we only have new value in serialized form 
        /// in which case <see cref="NewValue" /> will return null. 
        /// </summary>
        public bool HasNewValue { get { return _hasNewValue; } }

        /// <summary>
        /// Gets security subject ID initiated this cache event, if available. This property is available only for <see 
        /// cref="EventType.EvtCacheObjectPut" />, <see cref="EventType.EvtCacheObjectRemoved" /> and <see 
        /// cref="EventType.EvtCacheObjectRead" /> cache events. Subject ID will be set either to nodeId initiated 
        /// cache update or read or client ID initiated cache update or read. 
        /// </summary>
        public Guid SubjectId { get { return _subjectId; } }

        /// <summary>
        /// Gets closure class name (applicable only for TRANSFORM operations). 
        /// </summary>
        public string ClosureClassName { get { return _closureClassName; } }

        /// <summary>
        /// Gets task name if cache event was caused by an operation initiated within task execution. 
        /// </summary>
        public string TaskName { get { return _taskName; } }
        
        /** <inheritDoc /> */
	    public override string ToShortString()
	    {
	        return string.Format("{0}: IsNear={1}, Key={2}, HasNewValue={3}, HasOldValue={4}, NodeId={5}", Name, 
                _isNear, _key, HasNewValue, HasOldValue, Node.Id);
	    }
    }
}