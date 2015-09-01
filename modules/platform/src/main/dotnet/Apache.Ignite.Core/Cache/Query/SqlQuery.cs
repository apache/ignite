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
    using System;
    using System.Diagnostics.CodeAnalysis;
    using Apache.Ignite.Core.Impl.Cache;
    using Apache.Ignite.Core.Impl.Portable;

    /// <summary>
    /// SQL Query.
    /// </summary>
    public class SqlQuery : QueryBase
    {
        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="typ">Type.</param>
        /// <param name="sql">SQL.</param>
        /// <param name="args">Arguments.</param>
        public SqlQuery(Type typ, string sql, params object[] args) : this(typ, sql, false, args)
        {
            // No-op.
        }

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="typ">Type.</param>
        /// <param name="sql">SQL.</param>
        /// <param name="loc">Whether query should be executed locally.</param>
        /// <param name="args">Arguments.</param>
        public SqlQuery(Type typ, string sql, bool loc, params object[] args) : this(typ.Name, sql, loc, args)
        {
            // No-op.
        }
        
        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="typ">Type.</param>
        /// <param name="sql">SQL.</param>
        /// <param name="args">Arguments.</param>
        public SqlQuery(string typ, string sql, params object[] args) : this(typ, sql, false, args)
        {
            // No-op.
        }

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="typ">Type.</param>
        /// <param name="sql">SQL.</param>
        /// <param name="loc">Whether query should be executed locally.</param>
        /// <param name="args">Arguments.</param>
        public SqlQuery(string typ, string sql, bool loc, params object[] args)
        {
            Type = typ;
            Sql = sql;
            Local = loc;
            Arguments = args;
        }

        /// <summary>
        /// Type.
        /// </summary>
        public string Type { get; set; }

        /// <summary>
        /// SQL.
        /// </summary>
        public string Sql { get; set; }

        /// <summary>
        /// Arguments.
        /// </summary>
        [SuppressMessage("Microsoft.Performance", "CA1819:PropertiesShouldNotReturnArrays")]
        public object[] Arguments { get; set; }

        /** <inheritDoc /> */
        internal override void Write(PortableWriterImpl writer, bool keepPortable)
        {
            if (string.IsNullOrEmpty(Sql))
                throw new ArgumentException("Sql cannot be null or empty");

            if (string.IsNullOrEmpty(Type))
                throw new ArgumentException("Type cannot be null or empty");

            // 2. Prepare.
            writer.WriteBoolean(Local);
            writer.WriteString(Sql);
            writer.WriteString(Type);
            writer.WriteInt(PageSize);

            WriteQueryArgs(writer, Arguments);
        }

        /** <inheritDoc /> */
        internal override CacheOp OpId
        {
            get { return CacheOp.QRY_SQL; }
        }
    }
}
