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
    using System.Collections;
    using System.Collections.ObjectModel;
    using System.Collections.Generic;
    using GridGain.Client.Balancer;
    using GridGain.Client.Impl.Message;

    using N = GridGain.Client.IGridClientNode;

    /**
     * 
     */
   internal  class GridClientDataQueriesImpl : GridClientAbstractProjection<GridClientDataQueriesImpl>, IGridClientDataQueries {
       public GridClientDataQueriesImpl(IGridClientProjectionConfig cfg, IEnumerable<N> nodes, Predicate<N> filter, IGridClientLoadBalancer balancer)
           : base(cfg, nodes, filter, balancer) {
       }

        /**
         * 
         */
        public IGridClientDataQuery<DictionaryEntry> createSqlQuery(String clsName, String clause) {
            GridClientDataQueryBean<DictionaryEntry> qry = new GridClientDataQueryBean<DictionaryEntry>();

            qry.Type = GridClientDataQueryType.Sql;

            qry.Clause = clause;
            qry.ClassName = clsName;

            return qry;
        }

        /**
         * 
         */
        public IGridClientDataQuery<IList> createSqlFieldsQuery(String clause) {
            GridClientDataQueryBean<IList> qry = new GridClientDataQueryBean<IList>();

            qry.Type = GridClientDataQueryType.SqlFields;

            qry.Clause = clause;

            return qry;
        }

        /**
         * 
         */
        public IGridClientDataQuery<DictionaryEntry> createFullTextQuery(String clsName, String clause) {
            GridClientDataQueryBean<DictionaryEntry> qry = new GridClientDataQueryBean<DictionaryEntry>();

            qry.Type = GridClientDataQueryType.FullText;

            qry.Clause = clause;
            qry.ClassName = clsName;

            return qry;
        }

        /**
         * 
         */
        public IGridClientDataQuery<DictionaryEntry> createScanQuery(String clsName, Object[] args) {
            GridClientDataQueryBean<DictionaryEntry> qry = new GridClientDataQueryBean<DictionaryEntry>();

            qry.Type = GridClientDataQueryType.Scan;

            qry.ClassName = clsName;
            qry.ClassArguments = args;

            return qry;
        }

        /**
         * 
         */
        public IGridClientFuture rebuildIndexes(String clsName) {
            return null;
        }

        /**
         * 
         */
        public IGridClientFuture rebuildAllIndexes() {
            return null;
        }

        /**
         * 
         */
        public IGridClientDataQueryMetrics metrics() {
            return null;
        }

        /**
         * 
         */
        public void resetMetrics() {

        }

        /** <inheritdoc /> */
        public IGridClientFuture<GridClientDataQueryResult> ExecuteQuery(GridClientCacheQueryRequest req, Guid destNodeId) {
            return WithReconnectHandling((conn, nodeId) => conn.ExecuteQuery(req));
        }

        /** <inheritdoc /> */
        public IGridClientFuture<GridClientDataQueryResult> FetchNextPage(long qryId, Guid destNodeId) {
            return WithReconnectHandling((conn, nodeId) => conn.FetchNextPage(qryId, destNodeId));
        }

        override protected GridClientDataQueriesImpl CreateProjectionImpl(IEnumerable<N> nodes, Predicate<N> filter, IGridClientLoadBalancer balancer) {
            return new GridClientDataQueriesImpl(cfg, nodes, filter, balancer);
        }
    }
}