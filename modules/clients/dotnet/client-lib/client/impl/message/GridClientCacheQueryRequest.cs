/* @csharp.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

using System.Collections.Generic;

namespace GridGain.Client.Impl.Message
{
    using System;
    using System.Text;
    using System.Collections;
    using GridGain.Client.Portable;
    using GridGain.Client.Impl.Query;

    using A = GridGain.Client.Util.GridClientArgumentCheck;

    /** <summary>Generic cache request.</summary> */
    internal class GridClientCacheQueryRequest : GridClientRequest {
        /**
         * <summary>
         * Creates grid cache query request.</summary>
         *
         * <param name="op">Requested operation.</param>
         * <param name="destNodeId">Node ID to route request to.</param>
         */
        public GridClientCacheQueryRequest(GridClientCacheQueryRequestOperation op, Guid destNodeId)
            : base(destNodeId) {
            this.Operation = op;
        }

        /**
         * <summary>Query ID.</summary>
         */
        public long QueryId {
            get;
            set;
        }

        /** 
         * <summary>Requested cache operation.</summary> 
         */
        public GridClientCacheQueryRequestOperation Operation {
            get;
            set;
        }

        /**
         * <summary>Query type</summary>
         */
        public GridClientDataQueryType Type {
            get;
            set;
        }

        /** 
         * <summary>Cache name.</summary> 
         */
        public String CacheName {
            get;
            set;
        }

        /** 
         * <summary>Query clause.</summary> 
         */
        public String Clause {
            get;
            set;
        }

        /** 
         * <summary>Page size.</summary> 
         */
        public int PageSize {
            get;
            set;
        }

        /** 
         * <summary>Query timeout.</summary> 
         */
        public long Timeout {
            get;
            set;
        }

        /** 
         * <summary>Include backups flag.</summary> 
         */
        public bool IncludeBackups {
            get;
            set;
        }

        /** 
         * <summary>Enable de-duplication flag.</summary> 
         */
        public bool EnableDedup {
            get;
            set;
        }

        /** 
         * <summary>Class name.</summary> 
         */
        public String ClassName {
            get;
            set;
        }

        /** 
         * <summary>Remote reducer class name.</summary> 
         */
        public String RemoteReducerClassName {
            get;
            set;
        }

        /** 
         * <summary>Remote transformer class name.</summary> 
         */
        public String RemoteTransformerClassName {
            get;
            set;
        }

        /** 
         * <summary>Class arguments (reducer, transformer or full scan closure).</summary> 
         */
        public Object[] ClassArguments {
            get;
            set;
        }

        /** 
         * <summary>Query arguments.</summary> 
         */
        public Object[] Arguments {
            get;
            set;
        }

        /** <inheritdoc /> */
        public override void WritePortable(IGridClientPortableWriter writer) {
            base.WritePortable(writer);

            writer.WriteLong("queryId", QueryId);

            writer.WriteInt("op", (int)Operation);
            writer.WriteInt("type", (int)Type);

            writer.WriteString("cacheName", CacheName);
            writer.WriteString("clause", Clause);
            writer.WriteInt("pageSize", PageSize);
            writer.WriteLong("timeout", Timeout);
            writer.WriteBoolean("includeBackups", IncludeBackups);
            writer.WriteBoolean("enableDedup", EnableDedup);
            writer.WriteString("className", ClassName);
            writer.WriteString("remoteReducerClassName", RemoteReducerClassName);
            writer.WriteString("remoteTransformerClassName", RemoteTransformerClassName);
            writer.WriteObjectArray("classArguments", ClassArguments);
            writer.WriteObjectArray("arguments", Arguments);
        }

        /** <inheritdoc /> */
        public override void ReadPortable(IGridClientPortableReader reader) {
            base.ReadPortable(reader);

            QueryId = reader.ReadLong("queryId");

            Operation = (GridClientCacheQueryRequestOperation)reader.ReadInt("op");
            Type = (GridClientDataQueryType)reader.ReadInt("type");

            CacheName = reader.ReadString("cacheName");
            Clause = reader.ReadString("clause");
            PageSize = reader.ReadInt("pageSize");
            Timeout = reader.ReadLong("timeout");
            IncludeBackups = reader.ReadBoolean("includeBackups");
            EnableDedup = reader.ReadBoolean("enableDedup");
            ClassName = reader.ReadString("className");
            RemoteReducerClassName = reader.ReadString("remoteReducerClassName");
            RemoteTransformerClassName = reader.ReadString("remoteTransformerClassName");
            ClassArguments = reader.ReadObjectArray<Object>("classArguments");
            Arguments = reader.ReadObjectArray<Object>("arguments");
        }
    }
}
