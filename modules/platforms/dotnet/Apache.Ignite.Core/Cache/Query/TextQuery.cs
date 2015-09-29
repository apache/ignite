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
    using Apache.Ignite.Core.Impl.Cache;
    using Apache.Ignite.Core.Impl.Portable;

    /// <summary>
    /// Text query.
    /// </summary>
    public class TextQuery : QueryBase
    {
        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="typ">Type.</param>
        /// <param name="txt">Text.</param>
        public TextQuery(Type typ, string txt) : this(typ, txt, false)
        {
            // No-op.
        }

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="typ">Type.</param>
        /// <param name="txt">Text.</param>
        /// <param name="loc">Whether query should be executed locally.</param>
        public TextQuery(Type typ, string txt, bool loc) : this(typ.Name, txt, loc)
        {
            // No-op.
        }

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="typ">Type.</param>
        /// <param name="txt">Text.</param>
        public TextQuery(string typ, string txt) : this(typ, txt, false)
        {
            // No-op.
        }

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="typ">Type.</param>
        /// <param name="txt">Text.</param>
        /// <param name="loc">Whether query should be executed locally.</param>
        public TextQuery(string typ, string txt, bool loc)
        {
            Type = typ;
            Text = txt;
            Local = loc;
        }

        /// <summary>
        /// Type.
        /// </summary>
        public string Type { get; set; }

        /// <summary>
        /// Text.
        /// </summary>
        public string Text { get; set; }

        /** <inheritDoc /> */
        internal override void Write(PortableWriterImpl writer, bool keepPortable)
        {
            if (string.IsNullOrEmpty(Text))
                throw new ArgumentException("Text cannot be null or empty");

            if (string.IsNullOrEmpty(Type))
                throw new ArgumentException("Type cannot be null or empty");

            writer.WriteBoolean(Local);
            writer.WriteString(Text);
            writer.WriteString(Type);
            writer.WriteInt(PageSize);
        }

        /** <inheritDoc /> */
        internal override CacheOp OpId
        {
            get { return CacheOp.QryTxt; }
        }
    }
}
