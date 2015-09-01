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
    using System.Diagnostics.CodeAnalysis;

    /// <summary>
    /// SQL fields query.
    /// </summary>
    public class SqlFieldsQuery
    {
        /** Default page size. */
        public const int DFLT_PAGE_SIZE = 1024;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="sql">SQL.</param>
        /// <param name="args">Arguments.</param>
        public SqlFieldsQuery(string sql, params object[] args) : this(sql, false, args)
        {
            // No-op.
        }

        /// <summary>
        /// Constructor,
        /// </summary>
        /// <param name="sql">SQL.</param>
        /// <param name="loc">Whether query should be executed locally.</param>
        /// <param name="args">Arguments.</param>
        public SqlFieldsQuery(string sql, bool loc, params object[] args)
        {
            Sql = sql;
            Local = loc;
            Arguments = args;

            PageSize = DFLT_PAGE_SIZE;
        }

        /// <summary>
        /// SQL.
        /// </summary>
        public string Sql { get; set; }
        
        /// <summary>
        /// Arguments.
        /// </summary>
        [SuppressMessage("Microsoft.Performance", "CA1819:PropertiesShouldNotReturnArrays")]
        public object[] Arguments { get; set; }

        /// <summary>
        /// Local flag. When set query will be executed only on local node, so only local 
        /// entries will be returned as query result.
        /// <para />
        /// Defaults to <c>false</c>.
        /// </summary>
        public bool Local { get; set; }

        /// <summary>
        /// Optional page size.
        /// <para />
        /// Defautls to <see cref="DFLT_PAGE_SIZE"/>.
        /// </summary>
        public int PageSize { get; set; }
    }
}
