/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

namespace Apache.Ignite.Core.Events
{
    using System;
    using System.Globalization;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Cluster;
    using Apache.Ignite.Core.Common;

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
        private readonly IgniteGuid? _taskSessionId;

        /** */
        private readonly IgniteGuid? _jobId;

        /** */
        private readonly IClusterNode _taskNode;

        /** */
        private readonly Guid? _taskSubjectId;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="r">The reader to read data from.</param>
        internal JobEvent(IBinaryRawReader r) : base(r)
        {
            _taskName = r.ReadString();
            _taskClassName = r.ReadString();
            _taskSessionId = r.ReadObject<IgniteGuid?>();
            _jobId = r.ReadObject<IgniteGuid?>();
            _taskNode = ReadNode(r);
            _taskSubjectId = r.ReadGuid();
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
        public IgniteGuid? TaskSessionId { get { return _taskSessionId; } }

        /// <summary>
        /// Gets job ID. 
        /// </summary>
        public IgniteGuid? JobId { get { return _jobId; } }

        /// <summary>
        /// Get node where parent task of the job has originated. 
        /// </summary>
        public IClusterNode TaskNode { get { return _taskNode; } }

        /// <summary>
        /// Gets task subject ID. 
        /// </summary>
        public Guid? TaskSubjectId { get { return _taskSubjectId; } }

        /// <summary>
        /// Gets shortened version of ToString result.
        /// </summary>
        public override string ToShortString()
	    {
	        return string.Format(CultureInfo.InvariantCulture,
	            "{0}: TaskName={1}, TaskClassName={2}, TaskSessionId={3}, JobId={4}, TaskNode={5}, " +
	            "TaskSubjectId={6}", Name, TaskName, TaskClassName, TaskSessionId, JobId, TaskNode,
	            TaskSubjectId);
	    }
    }
}
