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

namespace Apache.Ignite.Core.DataStructures.Configuration
{
    using System.ComponentModel;
    using Apache.Ignite.Core.Cache.Configuration;

    /// <summary>
    /// Configuration for atomic data structures.
    /// </summary>
    public class AtomicConfiguration
    {
        /// <summary> Default number of backups. </summary>
        public const int DefaultBackups = 0;

        /// <summary> Default caching mode. </summary>
        public const CacheMode DefaultCacheMode = CacheMode.Partitioned;

        /// <summary> Default atomic sequence reservation size. </summary>
        public const int DefaultAtomicSequenceReserveSize = 1000;

        /// <summary>
        /// Gets or sets number of nodes used to back up single partition for 
        /// <see cref="Cache.Configuration.CacheMode.Partitioned"/> cache.
        /// </summary>
        [DefaultValue(DefaultBackups)]
        public int Backups { get; set; }

        /// <summary>
        /// Gets or sets caching mode to use.
        /// </summary>
        [DefaultValue(DefaultCacheMode)]
        public CacheMode CacheMode { get; set; }

        /// <summary>
        /// Gets or sets the default number of sequence values reserved for <see cref="IAtomicSequence"/> instances. 
        /// After a certain number has been reserved, consequent increments of sequence will happen locally,
        /// without communication with other nodes, until the next reservation has to be made. 
        /// </summary>
        [DefaultValue(DefaultAtomicSequenceReserveSize)]
        public int AtomicSequenceReserveSize { get; set; }

        /// <summary>
        /// Initializes a new instance of the <see cref="AtomicConfiguration" /> class.
        /// </summary>
        public AtomicConfiguration()
        {
            CacheMode = DefaultCacheMode;
            AtomicSequenceReserveSize = DefaultAtomicSequenceReserveSize;
        }
    }
}
