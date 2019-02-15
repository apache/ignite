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
    using Apache.Ignite.Core.Impl.Binary;
    using Apache.Ignite.Core.Impl.Cache;
    using Apache.Ignite.Core.Impl.Common;

    /// <summary>
    /// Text query.
    /// </summary>
    public class TextQuery : QueryBase
    {
        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="queryType">Type.</param>
        /// <param name="text">Text.</param>
        public TextQuery(Type queryType, string text) : this(queryType, text, false)
        {
            // No-op.
        }

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="queryType">Type.</param>
        /// <param name="text">Text.</param>
        /// <param name="local">Whether query should be executed locally.</param>
        public TextQuery(Type queryType, string text, bool local)
            : this(queryType == null ? null : queryType.Name, text, local)
        {
            // No-op.
        }

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="queryType">Type.</param>
        /// <param name="text">Text.</param>
        public TextQuery(string queryType, string text) : this(queryType, text, false)
        {
            // No-op.
        }

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="queryType">Type.</param>
        /// <param name="text">Text.</param>
        /// <param name="local">Whether query should be executed locally.</param>
        public TextQuery(string queryType, string text, bool local)
        {
            IgniteArgumentCheck.NotNullOrEmpty("queryType", queryType);
            IgniteArgumentCheck.NotNullOrEmpty("text", text);

            QueryType = queryType;
            Text = text;
            Local = local;
        }

        /// <summary>
        /// Type.
        /// </summary>
        public string QueryType { get; set; }

        /// <summary>
        /// Text.
        /// </summary>
        public string Text { get; set; }

        /** <inheritDoc /> */
        internal override void Write(BinaryWriter writer, bool keepBinary)
        {
            if (string.IsNullOrEmpty(Text))
                throw new ArgumentException("Text cannot be null or empty");

            if (string.IsNullOrEmpty(QueryType))
                throw new ArgumentException("QueryType cannot be null or empty");

            writer.WriteBoolean(Local);
            writer.WriteString(Text);
            writer.WriteString(QueryType);
            writer.WriteInt(PageSize);
        }

        /** <inheritDoc /> */
        internal override CacheOp OpId
        {
            get { return CacheOp.QryTxt; }
        }
    }
}
