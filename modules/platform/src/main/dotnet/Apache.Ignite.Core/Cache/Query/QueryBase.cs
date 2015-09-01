/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace Apache.Ignite.Core.Cache.Query
{
    using Apache.Ignite.Core.Impl.Cache;
    using Apache.Ignite.Core.Impl.Portable;

    /// <summary>
    /// Base class for all GridGain cache entry queries.
    /// </summary>
    public abstract class QueryBase
    {
        /** Default page size. */
        public const int DFLT_PAGE_SIZE = 1024;

        /// <summary>
        /// Initializes a new instance of the <see cref="QueryBase"/> class.
        /// </summary>
        protected internal QueryBase()
        {
            PageSize = DFLT_PAGE_SIZE;
        }

        /// <summary>
        /// Local flag. When set query will be executed only on local node, so only local 
        /// entries will be returned as query result.
        /// <para />
        /// Defaults to <c>false</c>.
        /// </summary>
        public bool Local { get; set; }

        /// <summary>
        /// Optional page size. If set to <code>0</code>, then <code>CacheQueryConfiguration.pageSize</code> is used.
        /// </summary>
        public int PageSize { get; set; }

        /// <summary>
        /// Writes this instance to a stream created with a specified delegate.
        /// </summary>
        /// <param name="writer">Writer.</param>
        /// <param name="keepPortable">Keep portable flag.</param>
        internal abstract void Write(PortableWriterImpl writer, bool keepPortable);

        /// <summary>
        /// Gets the interop opcode.
        /// </summary>
        internal abstract CacheOp OpId { get; }

        /// <summary>
        /// Write query arguments.
        /// </summary>
        /// <param name="writer">Writer.</param>
        /// <param name="args">Arguments.</param>
        internal static void WriteQueryArgs(PortableWriterImpl writer, object[] args)
        {
            if (args == null)
                writer.WriteInt(0);
            else
            {
                writer.WriteInt(args.Length);

                foreach (var arg in args)
                    writer.WriteObject(arg);
            }
        }
    }
}
