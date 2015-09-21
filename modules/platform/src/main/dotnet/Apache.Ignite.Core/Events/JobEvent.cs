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
    /// Ignite job event.
    /// </summary>
    public sealed class JobEvent : EventBase
	{
        /** */
        private readonly string _taskName;

        /** */
        private readonly string _taskClassName;

        /** */
        private readonly IgniteGuid _taskSessionId;

        /** */
        private readonly IgniteGuid _jobId;

        /** */
        private readonly IClusterNode _taskNode;

        /** */
        private readonly Guid _taskSubjectId;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="r">The reader to read data from.</param>
        internal JobEvent(IPortableRawReader r) : base(r)
        {
            _taskName = r.ReadString();
            _taskClassName = r.ReadString();
            _taskSessionId = IgniteGuid.ReadPortable(r);
            _jobId = IgniteGuid.ReadPortable(r);
            _taskNode = ReadNode(r);
            _taskSubjectId = r.ReadGuid() ?? Guid.Empty;
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
        /// Gets task session ID of the task that triggered this event. 
        /// </summary>
        public IgniteGuid TaskSessionId { get { return _taskSessionId; } }

        /// <summary>
        /// Gets job ID. 
        /// </summary>
        public IgniteGuid JobId { get { return _jobId; } }

        /// <summary>
        /// Get node where parent task of the job has originated. 
        /// </summary>
        public IClusterNode TaskNode { get { return _taskNode; } }

        /// <summary>
        /// Gets task subject ID. 
        /// </summary>
        public Guid TaskSubjectId { get { return _taskSubjectId; } }

        /** <inheritDoc /> */
	    public override string ToShortString()
	    {
	        return string.Format("{0}: TaskName={1}, TaskClassName={2}, TaskSessionId={3}, JobId={4}, TaskNode={5}, " +
	                             "TaskSubjectId={6}", Name, TaskName, TaskClassName, TaskSessionId, JobId, TaskNode, 
                                 TaskSubjectId);
	    }
    }
}
