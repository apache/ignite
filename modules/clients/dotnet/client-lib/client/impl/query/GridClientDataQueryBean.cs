/* @csharp.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Client.Impl.Query
{
    using System;
    using System.Collections.ObjectModel;
    using System.Collections.Generic;

    /**
     * 
     */
    internal class GridClientDataQueryBean<T> : IGridClientDataQuery<T> {
        /**<summary>Root queries projection.</summary>*/
        private GridClientDataQueriesImpl rootPrj;

        /**
         * 
         */
        public GridClientDataQueryBean(GridClientDataQueriesImpl rootPrj) {
            this.rootPrj = rootPrj;
        }

        /**
         * <summary>Copy constructor</summary>
         */
        public GridClientDataQueryBean(GridClientDataQueryBean<T> src) {
            Type = src.Type;
            CacheName = src.CacheName;
            Clause = src.Clause;
            PageSize = src.PageSize;
            Timeout = src.Timeout;
            KeepAll = src.KeepAll;
            IncludeBackups = src.IncludeBackups;
            EnableDedup = src.EnableDedup;
            ClassName = src.ClassName;
            RemoteReducerClassName = src.RemoteReducerClassName;
            RemoteTransformerClassName = src.RemoteTransformerClassName;
            ClassArguments = src.ClassArguments;

            rootPrj = src.rootPrj;
        }

        /**
         * 
         */
        public GridClientDataQueryType Type {
            get;
            set;
        }

        /**
         * 
         */
        public String CacheName {
            get;
            set;
        }

        /**
         * 
         */
        public String Clause {
            get;
            set;
        }

        /**
         * 
         */
        public int PageSize {
            get;
            set;
        }

        /**
         * 
         */
        public TimeSpan Timeout {
            get;
            set;
        }

        /**
         * 
         */
        public bool KeepAll {
            get;
            set;
        }

        /**
         * 
         */
        public bool IncludeBackups {
            get;
            set;
        }

        /**
         * 
         */
        public bool EnableDedup {
            get;
            set;
        }

        /**
         * 
         */
        public String ClassName {
            get;
            set;
        }

        /**
         * 
         */
        public String RemoteReducerClassName {
            get;
            private set;
        }

        /**
         * 
         */
        public String RemoteTransformerClassName {
            get;
            private set;
        }

        /**
         * 
         */
        public Object[] ClassArguments {
            get;
            set;
        }


        /**
         * 
         */
        public void RemoteReducer(String clsName, Object[] args) {
            RemoteTransformerClassName = null;
            RemoteReducerClassName = clsName;
            ClassArguments = args;
        }

        /**
         * 
         */
        public void RemoteTransformer(String clsName, Object[] args) {
            RemoteReducerClassName = null;
            RemoteTransformerClassName = clsName;
            ClassArguments = args;
        }

        /**
         * 
         */
        public IGridClientDataQueryFuture<T> Execute(Object[] args) {
            GridClientDataQueryBean<T> cp = new GridClientDataQueryBean<T>(this);

            GridClientDataQueryFutureImpl<T> fut = new GridClientDataQueryFutureImpl<T>(rootPrj, cp);

            fut.Init(args);

            return fut;
        }
    }
}
