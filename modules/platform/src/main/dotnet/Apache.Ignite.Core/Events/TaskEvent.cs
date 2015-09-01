/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Events
{
    using System;

    using GridGain.Common;
    using GridGain.Portable;

	/// <summary>
    /// Grid task event.
    /// </summary>
    public sealed class TaskEvent : EventBase
	{
        /** */
        private readonly string taskName;

        /** */
        private readonly string taskClassName;

        /** */
        private readonly GridGuid taskSessionId;

        /** */
        private readonly bool @internal;

        /** */
        private readonly Guid subjectId;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="r">The reader to read data from.</param>
        internal TaskEvent(IPortableRawReader r) : base(r)
        {
            taskName = r.ReadString();
            taskClassName = r.ReadString();
            taskSessionId = GridGuid.ReadPortable(r);
            @internal = r.ReadBoolean();
            subjectId = r.ReadGuid() ?? Guid.Empty;
        }
		
        /// <summary>
        /// Gets name of the task that triggered the event. 
        /// </summary>
        public string TaskName { get { return taskName; } }

        /// <summary>
        /// Gets name of task class that triggered this event. 
        /// </summary>
        public string TaskClassName { get { return taskClassName; } }

        /// <summary>
        /// Gets session ID of the task that triggered the event. 
        /// </summary>
        public GridGuid TaskSessionId { get { return taskSessionId; } }

        /// <summary>
        /// Returns true if task is created by Ignite and is used for system needs. 
        /// </summary>
        public bool Internal { get { return @internal; } }

        /// <summary>
        /// Gets security subject ID initiated this task event, if available. This property is not available for 
        /// GridEventType#EVT_TASK_SESSION_ATTR_SET task event. Subject ID will be set either to node ID or client ID 
        /// initiated task execution. 
        /// </summary>
        public Guid SubjectId { get { return subjectId; } }

        /** <inheritDoc /> */
	    public override string ToShortString()
	    {
	        return string.Format("{0}: TaskName={1}, TaskClassName={2}, TaskSessionId={3}, Internal={4}, " +
	                             "SubjectId={5}", Name, TaskName, TaskClassName, TaskSessionId, Internal, SubjectId);
	    }
    }
}
