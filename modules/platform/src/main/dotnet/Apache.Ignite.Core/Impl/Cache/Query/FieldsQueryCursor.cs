/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Impl.Cache.Query
{
    using System.Collections;

    using GridGain.Impl.Portable;
    using GridGain.Impl.Unmanaged;

    /// <summary>
    /// Cursor for entry-based queries.
    /// </summary>
    internal class FieldsQueryCursor : AbstractQueryCursor<IList>
    {
        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="target">Target.</param>
        /// <param name="marsh">Marshaler.</param>
        /// <param name="keepPortable">Keep poratble flag.</param>
        public FieldsQueryCursor(IUnmanagedTarget target, PortableMarshaller marsh, bool keepPortable)
            : base(target, marsh, keepPortable)
        {
            // No-op.
        }

        /** <inheritdoc /> */
        protected override IList Read(PortableReaderImpl reader)
        {
            int cnt = reader.ReadInt();

            var res = new ArrayList(cnt);

            for (int i = 0; i < cnt; i++)
                res.Add(reader.ReadObject<object>());

            return res;
        }
    }
}
