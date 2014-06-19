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
    using System.Collections.Concurrent;
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

        /**<summary>Current future.</summary>*/
        private IGridClientFuture<GridClientDataQueryResult> currentFut;

        /**<summary>Results for iteration.</summary>*/
        private ConcurrentQueue<T> iterResults = new ConcurrentQueue<T>();

        /**<summary>Collected elements.</summary>*/
        private IList<T> collectedRes = new List<T>();

        /**
         * 
         */
        public GridClientDataQueryFutureImpl( GridClientDataQueriesImpl prj, GridClientDataQueryBean<T> qryBean) {
            this.prj = prj;
            this.qryBean = qryBean;
        }

        /**
         * Sends initial request to remote node.
         */
        public void Init(Object[] args) {
            currentFut = prj.ExecuteQuery<T>(qryBean, args);
        }

        /**
         * 
         */
        public int available() {
            lock (this) {
                if (currentFut.IsDone)
                    FetchNextPage(qryBean.PageSize);
            }

            return iterResults.Count;
        }

        /**
         * 
         */
        public T next() {
            while (true) {
                T res;

                bool success = iterResults.TryDequeue(out res);

                if (success)
                    return res;

                lock (this) {
                    if (currentFut == null)
                        break;

                    currentFut.WaitDone();

                    FetchNextPage(qryBean.PageSize);
                }
            }

            return default(T);
        }

        /**
         * Overriden method which waits for all results to be collected.
         */
        override public bool WaitDone(TimeSpan timeout) {
            lock (this) {
                while (currentFut != null) {
                    if (!currentFut.WaitDone(timeout))
                        return false;

                    FetchNextPage(-1);
                }
            }

            return true;
        }

        /**
         * Fetches next results page from server.
         */
        private void FetchNextPage(int pageSize) {
            if (currentFut.IsDone) {
                GridClientDataQueryResult res = currentFut.Result;
                long qryId = res.QueryId;

                foreach (Object el in res.Items) {
                    iterResults.Enqueue((T)el);

                    if (qryBean.KeepAll || res.Last)
                        collectedRes.Add((T)el);
                }

                if (!res.Last)
                    currentFut = prj.FetchNextPage(qryId, pageSize, res.NodeId);
                else {
                    currentFut = null;

                    Done(collectedRes);
                }
            }
        }
    }
}
