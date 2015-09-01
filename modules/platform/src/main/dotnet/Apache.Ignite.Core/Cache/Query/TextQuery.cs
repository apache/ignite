/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Cache.Query
{
    using System;

    using GridGain.Impl.Cache;
    using GridGain.Impl.Portable;

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
            get { return CacheOp.QRY_TXT; }
        }
    }
}
