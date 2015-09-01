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

    using GridGain.Cluster;
    using GridGain.Common;
    using GridGain.Portable;

	/// <summary>
    /// Grid job event.
    /// </summary>
    public sealed class JobEvent : EventBase
	{
        /** */
        private readonly string taskName;

        /** */
        private readonly string taskClassName;

        /** */
        private readonly GridGuid taskSessionId;

        /** */
        private readonly GridGuid jobId;

        /** */
        private readonly IClusterNode taskNode;

        /** */
        private readonly Guid taskSubjectId;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="r">The reader to read data from.</param>
        internal JobEvent(IPortableRawReader r) : base(r)
        {
            taskName = r.ReadString();
            taskClassName = r.ReadString();
            taskSessionId = GridGuid.ReadPortable(r);
            jobId = GridGuid.ReadPortable(r);
            taskNode = ReadNode(r);
            taskSubjectId = r.ReadGuid() ?? Guid.Empty;
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
        /// Gets task session ID of the task that triggered this event. 
        /// </summary>
        public GridGuid TaskSessionId { get { return taskSessionId; } }

        /// <summary>
        /// Gets job ID. 
        /// </summary>
        public GridGuid JobId { get { return jobId; } }

        /// <summary>
        /// Get node where parent task of the job has originated. 
        /// </summary>
        public IClusterNode TaskNode { get { return taskNode; } }

        /// <summary>
        /// Gets task subject ID. 
        /// </summary>
        public Guid TaskSubjectId { get { return taskSubjectId; } }

        /** <inheritDoc /> */
	    public override string ToShortString()
	    {
	        return string.Format("{0}: TaskName={1}, TaskClassName={2}, TaskSessionId={3}, JobId={4}, TaskNode={5}, " +
	                             "TaskSubjectId={6}", Name, TaskName, TaskClassName, TaskSessionId, JobId, TaskNode, 
                                 TaskSubjectId);
	    }
    }
}
