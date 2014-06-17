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
    using System.Collections;
    using System.Collections.ObjectModel;
    using System.Collections.Generic;

    /**
     * <summary>
     * A queries projection of grid client. Contains various methods for cache query operations.</summary>
     */
    public interface IGridClientDataQueries {
        /**
         * 
         */
        IGridClientDataQuery<DictionaryEntry> createSqlQuery(String clsName, String clause);

        /**
         * 
         */
        IGridClientDataQuery<IList> createSqlFieldsQuery(String clause);

        /**
         * 
         */
        IGridClientDataQuery<DictionaryEntry> createFullTextQuery(String clsName, String clause);

        /**
         * 
         */
        IGridClientDataQuery<DictionaryEntry> createScanQuery(String clsName, Object[] args);

        /**
         * 
         */
        IGridClientFuture rebuildIndexes(String clsName);

        /**
         * 
         */
        IGridClientFuture rebuildAllIndexes();

        /**
         * 
         */
        IGridClientDataQueryMetrics metrics();

        /**
         * 
         */
        void resetMetrics();
    }
}
