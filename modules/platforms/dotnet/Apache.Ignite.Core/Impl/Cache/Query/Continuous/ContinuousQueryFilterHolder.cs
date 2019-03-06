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

namespace Apache.Ignite.Core.Impl.Cache.Query.Continuous
{
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Impl.Binary;

    /// <summary>
    /// Continuous query remote filter holder. Wraps real filter into binary object,
    /// so that it can be passed over wire to another node.
    /// </summary>
    internal class ContinuousQueryFilterHolder : IBinaryWriteAware
    {
        /** Filter object. */
        private readonly object _filter;

        /** Keep binary flag. */
        private readonly bool _keepBinary;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="filter">Filter.</param>
        /// <param name="keepBinary">Keep binary flag.</param>
        public ContinuousQueryFilterHolder(object filter, bool keepBinary)
        {
            _filter = filter;
            _keepBinary = keepBinary;
        }

        /// <summary>
        /// Filter.
        /// </summary>
        internal object Filter
        {
            get { return _filter; }
        }

        /// <summary>
        /// Keep binary flag.
        /// </summary>
        internal bool KeepBinary
        {
            get { return _keepBinary; }
        }

        /// <summary>
        /// Writes this object to the given writer.
        /// </summary>
        /// <param name="writer">Writer.</param>
        public void WriteBinary(IBinaryWriter writer)
        {
            var rawWriter = (BinaryWriter) writer.GetRawWriter();

            rawWriter.WriteObject(_filter);
            rawWriter.WriteBoolean(_keepBinary);
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ContinuousQueryFilterHolder"/> class.
        /// </summary>
        /// <param name="reader">The reader.</param>
        public ContinuousQueryFilterHolder(IBinaryRawReader reader)
        {
            _filter = reader.ReadObject<object>();
            _keepBinary = reader.ReadBoolean();
        }
    }
}
