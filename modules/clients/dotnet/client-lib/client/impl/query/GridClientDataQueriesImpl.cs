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
    using GridGain.Client.Impl.Query;
    using GridGain.Client.Balancer;
    using GridGain.Client.Impl.Message;

    using N = GridGain.Client.IGridClientNode;

    /**
     * 
     */
   internal  class GridClientDataQueriesImpl : GridClientAbstractProjection<GridClientDataQueriesImpl>, IGridClientDataQueries {
       /**<summary>Cache name.</summary>*/
       private String cacheName;

       public GridClientDataQueriesImpl(IGridClientProjectionConfig cfg, IEnumerable<N> nodes, Predicate<N> filter, IGridClientLoadBalancer balancer, String cacheName)
           : base(cfg, nodes, filter, balancer) {
           this.cacheName = cacheName;
       }

        /**
         * 
         */
        public IGridClientDataQuery<DictionaryEntry> createSqlQuery(String clsName, String clause) {
            GridClientDataQueryBean<DictionaryEntry> qry = new GridClientDataQueryBean<DictionaryEntry>(this);

            qry.Type = GridClientDataQueryType.Sql;

            qry.Clause = clause;
            qry.ClassName = clsName;
            qry.CacheName = cacheName;

            return qry;
        }

        /**
         * 
         */
        public IGridClientDataQuery<IList> createSqlFieldsQuery(String clause) {
            GridClientDataQueryBean<IList> qry = new GridClientDataQueryBean<IList>(this);

            qry.Type = GridClientDataQueryType.SqlFields;

            qry.Clause = clause;
            qry.CacheName = cacheName;

            return qry;
        }

        /**
         * 
         */
        public IGridClientDataQuery<DictionaryEntry> createFullTextQuery(String clsName, String clause) {
            GridClientDataQueryBean<DictionaryEntry> qry = new GridClientDataQueryBean<DictionaryEntry>(this);

            qry.Type = GridClientDataQueryType.FullText;

            qry.Clause = clause;
            qry.ClassName = clsName;
            qry.CacheName = cacheName;

            return qry;
        }

        /**
         * 
         */
        public IGridClientDataQuery<DictionaryEntry> createScanQuery(String clsName, Object[] args) {
            GridClientDataQueryBean<DictionaryEntry> qry = new GridClientDataQueryBean<DictionaryEntry>(this);

            qry.Type = GridClientDataQueryType.Scan;

            qry.ClassName = clsName;
            qry.ClassArguments = args;
            qry.CacheName = cacheName;

            return qry;
        }

        /**
         * 
         */
        public IGridClientFuture rebuildIndexes(String clsName) {
            return WithReconnectHandling((conn, nodeId) => conn.RebuildIndexes(clsName));
        }

        /**
         * 
         */
        public IGridClientFuture rebuildAllIndexes() {
            return WithReconnectHandling((conn, nodeId) => conn.RebuildIndexes(null));
        }

        /** <inheritdoc /> */
        public IGridClientFuture<GridClientDataQueryResult> ExecuteQuery<T>(GridClientDataQueryBean<T> qry, Object[] args) {
            return WithReconnectHandling((conn, nodeId) => conn.ExecuteQuery(qry, args, nodeId));
        }

        /** <inheritdoc /> */
        public IGridClientFuture<GridClientDataQueryResult> FetchNextPage(long qryId, int pageSize, Guid destNodeId) {
            return WithReconnectHandling((conn, nodeId) => conn.FetchNextPage(qryId, pageSize, destNodeId));
        }

        /** <inheritdoc /> */
        override protected GridClientDataQueriesImpl CreateProjectionImpl(IEnumerable<N> nodes, Predicate<N> filter, IGridClientLoadBalancer balancer) {
            return new GridClientDataQueriesImpl(cfg, nodes, filter, balancer, cacheName);
        }
    }
}