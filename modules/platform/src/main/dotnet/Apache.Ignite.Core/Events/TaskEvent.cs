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
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Portable;

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
