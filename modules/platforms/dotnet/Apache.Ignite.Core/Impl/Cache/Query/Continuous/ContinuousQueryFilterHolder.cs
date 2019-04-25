/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
