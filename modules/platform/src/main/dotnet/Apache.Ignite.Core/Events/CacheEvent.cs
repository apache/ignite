/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
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
        private readonly string cacheName;

        /** */
        private readonly int partition;

        /** */
        private readonly bool isNear;

        /** */
        private readonly IClusterNode eventNode;

        /** */
        private readonly object key;

        /** */
        private readonly GridGuid xid;

        /** */
        private readonly object lockId;

        /** */
        private readonly object newValue;

        /** */
        private readonly object oldValue;

        /** */
        private readonly bool hasOldValue;

        /** */
        private readonly bool hasNewValue;

        /** */
        private readonly Guid subjectId;

        /** */
        private readonly string closureClassName;

        /** */
        private readonly string taskName;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="r">The reader to read data from.</param>
        internal CacheEvent(IPortableRawReader r) : base(r)
        {
            cacheName = r.ReadString();
            partition = r.ReadInt();
            isNear = r.ReadBoolean();
            eventNode = ReadNode(r);
            key = r.ReadObject<object>();
            xid = GridGuid.ReadPortable(r);
            lockId = r.ReadObject<object>();
            newValue = r.ReadObject<object>();
            oldValue = r.ReadObject<object>();
            hasOldValue = r.ReadBoolean();
            hasNewValue = r.ReadBoolean();
            subjectId = r.ReadGuid() ?? Guid.Empty;
            closureClassName = r.ReadString();
            taskName = r.ReadString();
        }
		
        /// <summary>
        /// Gets cache name. 
        /// </summary>
        public string CacheName { get { return cacheName; } }

        /// <summary>
        /// Gets partition for the event which is the partition the key belongs to. 
        /// </summary>
        public int Partition { get { return partition; } }

        /// <summary>
        /// Gets flag indicating whether event happened on near or partitioned cache. 
        /// </summary>
        public bool IsNear { get { return isNear; } }

        /// <summary>
        /// Gets node which initiated cache operation or null if that node is not available. 
        /// </summary>
        public IClusterNode EventNode { get { return eventNode; } }

        /// <summary>
        /// Gets cache entry associated with event. 
        /// </summary>
        public object Key { get { return key; } }

        /// <summary>
        /// ID of surrounding cache cache transaction or null if there is no surrounding transaction. 
        /// </summary>
        public GridGuid Xid { get { return xid; } }

        /// <summary>
        /// ID of the lock if held or null if no lock held. 
        /// </summary>
        public object LockId { get { return lockId; } }

        /// <summary>
        /// Gets new value for this event. 
        /// </summary>
        public object NewValue { get { return newValue; } }

        /// <summary>
        /// Gets old value associated with this event. 
        /// </summary>
        public object OldValue { get { return oldValue; } }

        /// <summary>
        /// Gets flag indicating whether cache entry has old value in case if we only have old value in serialized form 
        /// in which case <see cref="OldValue" /> will return null. 
        /// </summary>
        public bool HasOldValue { get { return hasOldValue; } }

        /// <summary>
        /// Gets flag indicating whether cache entry has new value in case if we only have new value in serialized form 
        /// in which case <see cref="NewValue" /> will return null. 
        /// </summary>
        public bool HasNewValue { get { return hasNewValue; } }

        /// <summary>
        /// Gets security subject ID initiated this cache event, if available. This property is available only for <see 
        /// cref="EventType.EVT_CACHE_OBJECT_PUT" />, <see cref="EventType.EVT_CACHE_OBJECT_REMOVED" /> and <see 
        /// cref="EventType.EVT_CACHE_OBJECT_READ" /> cache events. Subject ID will be set either to nodeId initiated 
        /// cache update or read or client ID initiated cache update or read. 
        /// </summary>
        public Guid SubjectId { get { return subjectId; } }

        /// <summary>
        /// Gets closure class name (applicable only for TRANSFORM operations). 
        /// </summary>
        public string ClosureClassName { get { return closureClassName; } }

        /// <summary>
        /// Gets task name if cache event was caused by an operation initiated within task execution. 
        /// </summary>
        public string TaskName { get { return taskName; } }
        
        /** <inheritDoc /> */
	    public override string ToShortString()
	    {
	        return string.Format("{0}: IsNear={1}, Key={2}, HasNewValue={3}, HasOldValue={4}, NodeId={5}", Name, 
                isNear, key, HasNewValue, HasOldValue, Node.Id);
	    }
    }
}