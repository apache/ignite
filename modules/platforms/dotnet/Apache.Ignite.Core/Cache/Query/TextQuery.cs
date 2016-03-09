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
