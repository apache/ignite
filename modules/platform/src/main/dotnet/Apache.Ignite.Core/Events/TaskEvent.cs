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
    /// Ignite task event.
    /// </summary>
    public sealed class TaskEvent : EventBase
	{
        /** */
        private readonly string _taskName;

        /** */
        private readonly string _taskClassName;

        /** */
        private readonly IgniteGuid _taskSessionId;

        /** */
        private readonly bool _internal;

        /** */
        private readonly Guid _subjectId;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="r">The reader to read data from.</param>
        internal TaskEvent(IPortableRawReader r) : base(r)
        {
            _taskName = r.ReadString();
            _taskClassName = r.ReadString();
            _taskSessionId = IgniteGuid.ReadPortable(r);
            _internal = r.ReadBoolean();
            _subjectId = r.ReadGuid() ?? Guid.Empty;
        }
		
        /// <summary>
        /// Gets name of the task that triggered the event. 
        /// </summary>
        public string TaskName { get { return _taskName; } }

        /// <summary>
        /// Gets name of task class that triggered this event. 
        /// </summary>
        public string TaskClassName { get { return _taskClassName; } }

        /// <summary>
        /// Gets session ID of the task that triggered the event. 
        /// </summary>
        public IgniteGuid TaskSessionId { get { return _taskSessionId; } }

        /// <summary>
        /// Returns true if task is created by Ignite and is used for system needs. 
        /// </summary>
        public bool Internal { get { return _internal; } }

        /// <summary>
        /// Gets security subject ID initiated this task event, if available. This property is not available for 
        /// <see cref="EventType.EvtTaskSessionAttrSet" /> task event. 
        /// Subject ID will be set either to node ID or client ID initiated task execution. 
        /// </summary>
        public Guid SubjectId { get { return _subjectId; } }

        /** <inheritDoc /> */
	    public override string ToShortString()
	    {
	        return string.Format("{0}: TaskName={1}, TaskClassName={2}, TaskSessionId={3}, Internal={4}, " +
	                             "SubjectId={5}", Name, TaskName, TaskClassName, TaskSessionId, Internal, SubjectId);
	    }
    }
}
