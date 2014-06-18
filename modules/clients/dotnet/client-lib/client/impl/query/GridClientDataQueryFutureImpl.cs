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
    using GridGain.Client.Impl.Message;
    using GridGain.Client.Util;

    /**
     * 
     */
    class GridClientDataQueryFutureImpl<T> : GridClientFuture<ICollection<T>>, IGridClientDataQueryFuture<T> {
        /**<summary>Execution projection.</summary>*/
        private GridClientDataQueriesImpl prj;

        /**<summary>Bean holding query paramters.</summary>*/
        private GridClientDataQueryBean<T> qryBean;

        public void Init() {
            GridClientCacheQueryRequest req = new GridClientCacheQueryRequest(GridClientCacheQueryRequestOperation.Execute, Guid.Empty);

            req.PageSize = qryBean.PageSize;
            req.Type = qryBean.Type;

            prj.ExecuteQuery(req, Guid.Empty);
        }

        /**
         * 
         */
        public int available() {
            return 0;
        }

        /**
         * 
         */
        public T next() {
            return default(T);
        }
    }
}
