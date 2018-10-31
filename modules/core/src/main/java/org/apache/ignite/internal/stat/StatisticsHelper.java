/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.ignite.internal.stat;

/**
 * Helper for gathering IO statistics.
 */
public class StatisticsHelper {
    /** */
    private static final ThreadLocal<StatisticsHolderQuery> CURRENT_QUERY_STATISTICS = new ThreadLocal<>();

    /**
     * Start gathering IO statistics for query. Should be used together with {@code finishGatheringQueryStatistics}
     * method.
     *
     * @param qryId Identifier of query.
     */
    public static void startGatheringQueryStatistics(String qryId) {
        StatisticsHolderQuery currQryStatisticsHolder = CURRENT_QUERY_STATISTICS.get();

        assert currQryStatisticsHolder == null : currQryStatisticsHolder;

        CURRENT_QUERY_STATISTICS.set(new StatisticsHolderQuery(qryId));
    }

    /**
     * Merge query statistics.
     *
     * @param qryStat Statistics which will be merged to current query statistics.
     */
    public static void mergeQueryStatistics(StatisticsHolderQuery qryStat) {
        assert qryStat != null;

        StatisticsHolderQuery currQryStatisticsHolder = CURRENT_QUERY_STATISTICS.get();

        assert currQryStatisticsHolder != null;

        currQryStatisticsHolder.merge(qryStat.logicalReads(), qryStat.physicalReads());
    }

    /**
     * Finish gathering IO statistics for query. Should be used together with {@code startGatheringQueryStatistics}
     * method.
     *
     * @return Gathered statistics.
     */
    public static StatisticsHolder finishGatheringQueryStatistics() {
        StatisticsHolderQuery currQryStatisticsHolder = CURRENT_QUERY_STATISTICS.get();

        assert currQryStatisticsHolder != null;

        CURRENT_QUERY_STATISTICS.remove();

        return currQryStatisticsHolder;
    }

    /**
     * Track logical read for current query.
     *
     * @param pageAddr Address of page.
     */
    static void trackLogicalReadQuery(long pageAddr) {
        StatisticsHolderQuery currQryStatisticsHolder = CURRENT_QUERY_STATISTICS.get();

        if (currQryStatisticsHolder != null)
            currQryStatisticsHolder.trackLogicalRead(pageAddr);

    }

    /**
     * Track physical and logical read for current query.
     *
     * @param pageAddr Address of page.
     */
    static void trackPhysicalAndLogicalReadQuery(long pageAddr) {
        StatisticsHolderQuery currQryStatisticsHolder = CURRENT_QUERY_STATISTICS.get();

        if (currQryStatisticsHolder != null)
            currQryStatisticsHolder.trackPhysicalAndLogicalRead(pageAddr);
    }

}
