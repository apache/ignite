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
    using PU = GridGain.Client.Impl.Portable.GridClientPortableUilts;

    /** <summary>Generic cache request.</summary> */
    [GridClientPortableId(PU.TYPE_CACHE_QUERY_REQ)]
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

            IGridClientPortableRawWriter rawWriter = writer.RawWriter();

            rawWriter.WriteLong(QueryId);

            rawWriter.WriteInt((int)Operation);
            rawWriter.WriteInt((int)Type);

            rawWriter.WriteString(CacheName);
            rawWriter.WriteString(Clause);
            rawWriter.WriteInt(PageSize);
            rawWriter.WriteLong(Timeout);
            rawWriter.WriteBoolean(IncludeBackups);
            rawWriter.WriteBoolean(EnableDedup);
            rawWriter.WriteString(ClassName);
            rawWriter.WriteString(RemoteReducerClassName);
            rawWriter.WriteString(RemoteTransformerClassName);
            rawWriter.WriteObjectArray(ClassArguments);
            rawWriter.WriteObjectArray(Arguments);
        }

        /** <inheritdoc /> */
        public override void ReadPortable(IGridClientPortableReader reader) {
            base.ReadPortable(reader);

            IGridClientPortableRawReader rawReader = reader.RawReader();

            QueryId = rawReader.ReadLong();

            Operation = (GridClientCacheQueryRequestOperation)rawReader.ReadInt();
            Type = (GridClientDataQueryType)rawReader.ReadInt();

            CacheName = rawReader.ReadString();
            Clause = rawReader.ReadString();
            PageSize = rawReader.ReadInt();
            Timeout = rawReader.ReadLong();
            IncludeBackups = rawReader.ReadBoolean();
            EnableDedup = rawReader.ReadBoolean();
            ClassName = rawReader.ReadString();
            RemoteReducerClassName = rawReader.ReadString();
            RemoteTransformerClassName = rawReader.ReadString();
            ClassArguments = rawReader.ReadObjectArray<Object>();
            Arguments = rawReader.ReadObjectArray<Object>();
        }
    }
}
