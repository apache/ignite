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

namespace Apache.Ignite.AspNet.Impl
{
    using System;
    using System.Diagnostics;
    using Apache.Ignite.Core.Binary;

    /// <summary>
    /// Result of the session state lock processor.
    /// </summary>
    internal class SessionStateLockResult
    {
        /** Success flag. */
        private readonly bool _success;

        /** Session state data. */
        private readonly IgniteSessionStateStoreData _data;

        /** Lock time. */
        private readonly DateTime? _lockTime;

        /** Lock id. */
        private readonly long _lockId;

        /// <summary>
        /// Initializes a new instance of the <see cref="SessionStateLockResult"/> class.
        /// </summary>
        /// <param name="reader">The reader.</param>
        public SessionStateLockResult(IBinaryRawReader reader)
        {
            _success = reader.ReadBoolean();

            if (_success)
                _data = new IgniteSessionStateStoreData(reader);

            _lockTime = reader.ReadTimestamp();
            _lockId = reader.ReadLong();

            Debug.Assert(_success ^ (_data == null));
            Debug.Assert(_success ^ (_lockTime != null));
        }

        /// <summary>
        /// Gets a value indicating whether lock succeeded.
        /// </summary>
        public bool Success
        {
            get { return _success; }
        }

        /// <summary>
        /// Gets the data. Null when <see cref="Success"/> is <c>false</c>.
        /// </summary>
        public IgniteSessionStateStoreData Data
        {
            get { return _data; }
        }

        /// <summary>
        /// Gets the lock time. Null when <see cref="Success"/> is <c>true</c>.
        /// </summary>
        public DateTime? LockTime
        {
            get { return _lockTime; }
        }

        /// <summary>
        /// Gets the lock identifier.
        /// </summary>
        public long LockId
        {
            get { return _lockId; }
        }
    }
}
