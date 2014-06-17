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
    class GridClientDataQueryImpl<T> : IGridClientDataQuery<T> {
        /** Remote reducer class name. */
        private String rmtRdcClsName;

        /** Remote transformer class name. */
        private String rmtTransClsName;

        /** Reducer or transformer constructor arguments. */
        private Object[] clsArgs;

        /**
         * 
         */
        int PageSize {
            get;
            set;
        }

        /**
         * 
         */
        TimeSpan Timeout {
            get;
            set;
        }

        /**
         * 
         */
        bool KeepAll {
            get;
            set;
        }

        /**
         * 
         */
        bool IncludeBackups {
            get;
            set;
        }

        /**
         * 
         */
        bool EnableDedup {
            get;
            set;
        }

        /**
         * 
         */
        void remoteReducer(String clsName, Object[] args) {

        }

        /**
         * 
         */
        void remoteTransformer(String clsName, Object[] args) {

        }

        /**
         * 
         */
        IGridClientDataQueryFuture<T> execute(Object[] args) {
            GridClientDataQueryFutureImpl<T> fut = new GridClientDataQueryFutureImpl<T>();

            return null;
        }
    }
}
