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

namespace Apache.Ignite.Core.Cache.Query
{
    using System;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Impl.Binary;
    using Apache.Ignite.Core.Impl.Cache;

    /// <summary>
    /// Base class for all Ignite cache entry queries.
    /// </summary>
    public abstract class QueryBase
    {
        /// <summary> Default page size. </summary>
        public const int DefaultPageSize = 1024;

        /// <summary>
        /// Initializes a new instance of the <see cref="QueryBase"/> class.
        /// </summary>
        protected internal QueryBase()
        {
            PageSize = DefaultPageSize;
        }

        /// <summary>
        /// Local flag. When set query will be executed only on local node, so only local 
        /// entries will be returned as query result.
        /// <para />
        /// Defaults to <c>false</c>.
        /// </summary>
        public bool Local { get; set; }

        /// <summary>
        /// Optional page size. If set to <c>0</c>, then <c>CacheQueryConfiguration.pageSize</c> is used.
        /// </summary>
        public int PageSize { get; set; }

        /// <summary>
        /// Writes this instance to a stream created with a specified delegate.
        /// </summary>
        /// <param name="writer">Writer.</param>
        /// <param name="keepBinary">Keep binary flag.</param>
        internal abstract void Write(BinaryWriter writer, bool keepBinary);

        /// <summary>
        /// Gets the interop opcode.
        /// </summary>
        internal abstract CacheOp OpId { get; }

        /// <summary>
        /// Write query arguments.
        /// </summary>
        /// <param name="writer">Writer.</param>
        /// <param name="args">Arguments.</param>
        internal static void WriteQueryArgs(IBinaryRawWriter writer, object[] args)
        {
            if (args == null)
                writer.WriteInt(0);
            else
            {
                writer.WriteInt(args.Length);

                foreach (var arg in args)
                {
                    // Write DateTime as TimeStamp always, otherwise it does not make sense
                    // Wrapped DateTime comparison does not work in SQL
                    var dt = arg as DateTime?;  // Works with DateTime also

                    if (dt != null)
                        writer.WriteTimestamp(dt);
                    else
                        writer.WriteObject(arg);
                }
            }
        }
    }
}
