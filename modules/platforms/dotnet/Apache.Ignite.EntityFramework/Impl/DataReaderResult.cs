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

namespace Apache.Ignite.EntityFramework.Impl
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Data.Common;
    using System.Diagnostics.CodeAnalysis;
    using System.Linq;

    /// <summary>
    /// Cacheable result of a DbDataReader.
    /// </summary>
    [Serializable]
    internal class DataReaderResult
    {
        /** */
        private readonly object[][] _data;

        /** */
        private readonly DataReaderField[] _schema;

        /// <summary>
        /// Initializes a new instance of the <see cref="DataReaderResult"/> class.
        /// </summary>
        [SuppressMessage("Microsoft.Usage", "CA2202: Do not call Dispose more than one time on an object")]
        public DataReaderResult(IDataReader reader)
        {
            try
            {
                _data = ReadAll(reader).ToArray();

                _schema = new DataReaderField[reader.FieldCount];

                for (int i = 0; i < reader.FieldCount; i++)
                {
                    _schema[i] = new DataReaderField(reader.GetName(i), reader.GetFieldType(i), 
                        reader.GetDataTypeName(i));
                }
            }
            finally 
            {
                reader.Close();
                reader.Dispose();
            }
        }

        /// <summary>
        /// Creates the reader over this instance.
        /// </summary>
        public DbDataReader CreateReader()
        {
            return new ArrayDbDataReader(_data, _schema);
        }

        /// <summary>
        /// Gets the row count.
        /// </summary>
        public int RowCount
        {
            get { return _data.Length; }
        }

        /// <summary>
        /// Reads all data from the reader.
        /// </summary>
        private static IEnumerable<object[]> ReadAll(IDataReader reader)
        {
            while (reader.Read())
            {
                var vals = new object[reader.FieldCount];

                reader.GetValues(vals);

                yield return vals;
            }
        }
    }
}
