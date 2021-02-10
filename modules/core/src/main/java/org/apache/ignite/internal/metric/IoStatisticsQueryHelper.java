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

package org.apache.ignite.internal.metric;

/**
 * Helper for gathering IO statistics.
 */
public class IoStatisticsQueryHelper {
    /** */
    private static final ThreadLocal<IoStatisticsHolderQuery> CUR_QRY_STATS = new ThreadLocal<>();

    /**
     * Start gathering IO statistics for query. Should be used together with {@code finishGatheringQueryStatistics}
     * method.
     *
     * @param qryId Identifier of query.
     */
    public static void startGatheringQueryStatistics(String qryId) {
        IoStatisticsHolderQuery currQryStatisticsHolder = CUR_QRY_STATS.get();

        assert currQryStatisticsHolder == null : currQryStatisticsHolder;

        CUR_QRY_STATS.set(new IoStatisticsHolderQuery(qryId));
    }

    /**
     * Merge query statistics.
     *
     * @param qryStat Statistics which will be merged to current query statistics.
     */
    public static void mergeQueryStatistics(IoStatisticsHolderQuery qryStat) {
        assert qryStat != null;

        IoStatisticsHolderQuery currQryStatisticsHolder = CUR_QRY_STATS.get();

        assert currQryStatisticsHolder != null;

        currQryStatisticsHolder.merge(qryStat.logicalReads(), qryStat.physicalReads());
    }

    /**
     * Finish gathering IO statistics for query. Should be used together with {@code startGatheringQueryStatistics}
     * method.
     *
     * @return Gathered statistics.
     */
    public static IoStatisticsHolder finishGatheringQueryStatistics() {
        IoStatisticsHolderQuery currQryStatisticsHolder = CUR_QRY_STATS.get();

        assert currQryStatisticsHolder != null;

        CUR_QRY_STATS.remove();

        return currQryStatisticsHolder;
    }

    /**
     * Track logical read for current query.
     *
     * @param pageAddr Address of page.
     */
    static void trackLogicalReadQuery(long pageAddr) {
        IoStatisticsHolderQuery currQryStatisticsHolder = CUR_QRY_STATS.get();

        if (currQryStatisticsHolder != null)
            currQryStatisticsHolder.trackLogicalRead(pageAddr);

    }

    /**
     * Track physical and logical read for current query.
     *
     * @param pageAddr Address of page.
     */
    static void trackPhysicalAndLogicalReadQuery(long pageAddr) {
        IoStatisticsHolderQuery currQryStatisticsHolder = CUR_QRY_STATS.get();

        if (currQryStatisticsHolder != null)
            currQryStatisticsHolder.trackPhysicalAndLogicalRead(pageAddr);
    }

}
