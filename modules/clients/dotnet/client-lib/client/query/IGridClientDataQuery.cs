/* @csharp.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Client
{
    using System;
    using System.Collections.ObjectModel;
    using System.Collections.Generic;

    /**
     * <summary>Data query object.</summary>
     */
    public interface IGridClientDataQuery<T> {
        /**
         * 
         */
        int PageSize { 
            get; set;
        }

        /**
         * 
         */
        TimeSpan Timeout {
            get; set;
        }

        /**
         * 
         */
        bool KeepAll {
            get; set;
        }

        /**
         * 
         */
        bool IncludeBackups {
            get; set;
        }

        /**
         * 
         */
        bool EnableDedup {
            get; set;
        }

        /**
         * 
         */
        void RemoteReducer(String clsName, Object[] args);

        /**
         * 
         */
        void RemoteTransformer(String clsName, Object[] args);

        /**
         * 
         */
        IGridClientDataQueryFuture<T> Execute(params Object[] args);
    }
}
