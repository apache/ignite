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
