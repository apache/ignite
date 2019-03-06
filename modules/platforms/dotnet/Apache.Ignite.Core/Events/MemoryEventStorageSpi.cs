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
    using System.ComponentModel;
    using System.Diagnostics;
    using Apache.Ignite.Core.Binary;

    /// <summary>
    /// In-memory event storage.
    /// </summary>
    public class MemoryEventStorageSpi : IEventStorageSpi
    {
        /// <summary>
        /// Default event count limit.
        /// </summary>
        public const long DefaultMaxEventCount = 10000;

        /// <summary>
        /// The default expiration timeout.
        /// </summary>
        public static readonly TimeSpan DefaultExpirationTimeout = TimeSpan.FromSeconds(-1);

        /// <summary>
        /// Gets or sets the expiration timeout for stored events.
        /// Negative value means no expiration.
        /// Defaults to -1 second.
        /// </summary>
        [DefaultValue(typeof(TimeSpan), "-0:0:1")]
        public TimeSpan ExpirationTimeout { get; set; }

        /// <summary>
        /// Gets or sets the maximum event count to store. When this limit is reached, older events are removed.
        /// Defaults to <see cref="DefaultMaxEventCount"/>.
        /// </summary>
        [DefaultValue(DefaultMaxEventCount)]
        public long MaxEventCount { get; set; }

        /// <summary>
        /// Initializes a new instance of the <see cref="MemoryEventStorageSpi"/> class.
        /// </summary>
        public MemoryEventStorageSpi()
        {
            ExpirationTimeout = DefaultExpirationTimeout;
            MaxEventCount = DefaultMaxEventCount;
        }

        /// <summary>
        /// Reads instance.
        /// </summary>
        internal static MemoryEventStorageSpi Read(IBinaryRawReader reader)
        {
            Debug.Assert(reader != null);

            var eventCount = reader.ReadLong();
            var timeout = reader.ReadLong();

            return new MemoryEventStorageSpi
            {
                MaxEventCount = eventCount,
                ExpirationTimeout = timeout < 0 || timeout > TimeSpan.MaxValue.TotalMilliseconds
                    ? DefaultExpirationTimeout
                    : TimeSpan.FromMilliseconds(timeout)
            };
        }

        /// <summary>
        /// Writes this instance.
        /// </summary>
        internal void Write(IBinaryRawWriter writer)
        {
            writer.WriteLong(MaxEventCount);

            if (ExpirationTimeout == TimeSpan.MaxValue || ExpirationTimeout < TimeSpan.Zero)
            {
                writer.WriteLong(long.MaxValue);
            }
            else
            {
                writer.WriteLong((long) ExpirationTimeout.TotalMilliseconds);
            }
        }
    }
}