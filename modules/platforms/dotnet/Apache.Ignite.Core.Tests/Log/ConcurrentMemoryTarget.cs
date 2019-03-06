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

namespace Apache.Ignite.Core.Tests.Log
{
    using System.Collections.Generic;
    using System.Linq;
    using global::NLog;
    using global::NLog.Targets;

    /// <summary>
    /// NLog target which supports logging from multiple threads.
    /// </summary>
    public class ConcurrentMemoryTarget : TargetWithLayout
    {
        /// <summary> Object used for locking. </summary>
        private readonly object _locker = new object();

        /// <summary> Logs. </summary>
        private readonly IList<string> _logs;

        /// <summary>
        /// Initializes a new instance of the <see cref="ConcurrentMemoryTarget" /> class.
        /// </summary>
        public ConcurrentMemoryTarget()
        {
            _logs = new List<string>();
            Name = "ConcurrentMemoryTarget";
        }

        /// <summary>
        /// Gets the collection of logs gathered in the <see cref="ConcurrentMemoryTarget" />.
        /// </summary>
        public IEnumerable<string> Logs
        {
            get
            {
                lock (_locker)
                {
                    return _logs.ToList();
                }
            }
        }

        /// <summary>
        /// Renders the logging event message and adds it to the internal ArrayList of log messages.
        /// </summary>
        /// <param name="logEvent">The logging event.</param>
        protected override void Write(LogEventInfo logEvent)
        {
            lock (_locker)
            {
                var msg = Layout.Render(logEvent);

                _logs.Add(msg);
            }
        }
    }
}