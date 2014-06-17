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
    class GridClientDataQueryFutureImpl<T> : IGridClientDataQueryFuture<T> {
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
        bool KeepAll {
            get;
            set;
        }
    }
}
