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

namespace Apache.Ignite.Core.Impl.Client.Cache.Query
{
    using System.Collections.Generic;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Cache.Query;
    using Apache.Ignite.Core.Impl.Binary.IO;

    /// <summary>
    /// Client fields cursor.
    /// </summary>
    internal class ClientFieldsQueryCursor : ClientQueryCursorBase<IList<object>>, IFieldsQueryCursor
    {
        /** */
        private string[] _fieldNames;

        /// <summary>
        /// Initializes a new instance of the <see cref="ClientQueryCursor{TK, TV}" /> class.
        /// </summary>
        /// <param name="ignite">The ignite.</param>
        /// <param name="cursorId">The cursor identifier.</param>
        /// <param name="keepBinary">Keep binary flag.</param>
        /// <param name="initialBatchStream">Optional stream with initial batch.</param>
        /// <param name="getPageOp">The get page op.</param>
        /// <param name="columnCount">The column count.</param>
        public ClientFieldsQueryCursor(IgniteClient ignite, long cursorId, bool keepBinary,
            IBinaryStream initialBatchStream, ClientOp getPageOp, int columnCount)
            : base(ignite, cursorId, keepBinary, initialBatchStream, getPageOp,
                r =>
                {
                    var res = new List<object>(columnCount);

                    for (var i = 0; i < columnCount; i++)
                    {
                        res.Add(r.ReadObject<object>());
                    }

                    return res;
                })
        {
            // No-op.
        }

        /** <inheritdoc /> */
        public IList<string> FieldNames
        {
            get { return _fieldNames ?? (_fieldNames = GetFieldNames()); }
        }

        /// <summary>
        /// Gets the field names.
        /// </summary>
        private string[] GetFieldNames()
        {
            return Ignite.Socket.DoOutInOp(ClientOp.QuerySqlFieldsCursorGetFieldNames,
                w => w.WriteLong(CursorId), s =>
                {
                    IBinaryRawReader r = Ignite.Marshaller.StartUnmarshal(s);
                    var res = new string[r.ReadInt()];

                    for (var i = 0; i < res.Length; i++)
                    {
                        res[i] = r.ReadString();
                    }

                    return res;
                });
        }
    }
}
