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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.LongAdder;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;

/**
 * Index statistics holder to gather statistics related to concrete index.
 */
public class IoStatisticsHolderIndex implements IoStatisticsHolder {
    /** Display name of hash PK index. */
    public static final String HASH_PK_IDX_NAME = "PK";

    /** */
    public static final String LOGICAL_READS_LEAF = "LOGICAL_READS_LEAF";

    /** */
    public static final String LOGICAL_READS_INNER = "LOGICAL_READS_INNER";

    /** */
    public static final String PHYSICAL_READS_LEAF = "PHYSICAL_READS_LEAF";

    /** */
    public static final String PHYSICAL_READS_INNER = "PHYSICAL_READS_INNER";

    /** */
    private LongAdder logicalReadLeafCtr = new LongAdder();

    /** */
    private LongAdder logicalReadInnerCtr = new LongAdder();

    /** */
    private LongAdder physicalReadLeafCtr = new LongAdder();

    /** */
    private LongAdder physicalReadInnerCtr = new LongAdder();

    /** */
    private final String cacheName;

    /** */
    private final String idxName;

    /**
     * @param cacheName Name of cache.
     * @param idxName Name of index.
     */
    public IoStatisticsHolderIndex(String cacheName, String idxName) {
        assert cacheName != null && idxName != null;

        this.cacheName = cacheName;
        this.idxName = idxName;
    }

    /** {@inheritDoc} */
    @Override public void trackLogicalRead(long pageAddr) {
        IndexPageType idxPageType = deriveIndexPageType(pageAddr);

        switch (idxPageType) {
            case INNER:
                logicalReadInnerCtr.increment();

                IoStatisticsQueryHelper.trackLogicalReadQuery(pageAddr);

                break;

            case LEAF:
                logicalReadLeafCtr.increment();

                IoStatisticsQueryHelper.trackLogicalReadQuery(pageAddr);

                break;
        }

    }

    /** {@inheritDoc} */
    @Override public void trackPhysicalAndLogicalRead(long pageAddr) {
        IndexPageType idxPageType = deriveIndexPageType(pageAddr);

        switch (idxPageType) {
            case INNER:
                logicalReadInnerCtr.increment();
                physicalReadInnerCtr.increment();

                IoStatisticsQueryHelper.trackPhysicalAndLogicalReadQuery(pageAddr);

                break;

            case LEAF:
                logicalReadLeafCtr.increment();
                physicalReadLeafCtr.increment();

                IoStatisticsQueryHelper.trackPhysicalAndLogicalReadQuery(pageAddr);

                break;
        }
    }

    /** {@inheritDoc} */
    @Override public long logicalReads() {
        return logicalReadLeafCtr.longValue() + logicalReadInnerCtr.longValue();
    }

    /** {@inheritDoc} */
    @Override public long physicalReads() {
        return physicalReadLeafCtr.longValue() + physicalReadInnerCtr.longValue();
    }

    /** {@inheritDoc} */
    @Override public Map<String, Long> logicalReadsMap() {
        Map<String, Long> res = new HashMap<>(3);

        res.put(LOGICAL_READS_LEAF, logicalReadLeafCtr.longValue());
        res.put(LOGICAL_READS_INNER, logicalReadInnerCtr.longValue());

        return res;
    }

    /** {@inheritDoc} */
    @Override public Map<String, Long> physicalReadsMap() {
        Map<String, Long> res = new HashMap<>(3);

        res.put(PHYSICAL_READS_LEAF, physicalReadLeafCtr.longValue());
        res.put(PHYSICAL_READS_INNER, physicalReadInnerCtr.longValue());

        return res;
    }

    /** {@inheritDoc} */
    @Override public void resetStatistics() {
        logicalReadLeafCtr.reset();
        logicalReadInnerCtr.reset();
        physicalReadLeafCtr.reset();
        physicalReadInnerCtr.reset();
    }

    /**
     * @param pageAddr Address of page.
     * @return Index page type.
     */
    private IndexPageType deriveIndexPageType(long pageAddr) {
        int pageIoType = PageIO.getType(pageAddr);
        switch (pageIoType) {
            case PageIO.T_DATA_REF_INNER:
            case PageIO.T_DATA_REF_MVCC_INNER:
            case PageIO.T_H2_REF_INNER:
            case PageIO.T_H2_MVCC_REF_INNER:
            case PageIO.T_CACHE_ID_AWARE_DATA_REF_INNER:
            case PageIO.T_CACHE_ID_DATA_REF_MVCC_INNER:
                return IndexPageType.INNER;

            case PageIO.T_DATA_REF_LEAF:
            case PageIO.T_DATA_REF_MVCC_LEAF:
            case PageIO.T_H2_REF_LEAF:
            case PageIO.T_H2_MVCC_REF_LEAF:
            case PageIO.T_CACHE_ID_AWARE_DATA_REF_LEAF:
            case PageIO.T_CACHE_ID_DATA_REF_MVCC_LEAF:
                return IndexPageType.LEAF;

            default:
                if ((PageIO.T_H2_EX_REF_LEAF_START <= pageIoType && pageIoType <= PageIO.T_H2_EX_REF_LEAF_END) ||
                    (PageIO.T_H2_EX_REF_MVCC_LEAF_START <= pageIoType && pageIoType <= PageIO.T_H2_EX_REF_MVCC_LEAF_END)
                )
                    return IndexPageType.LEAF;

                if ((PageIO.T_H2_EX_REF_INNER_START <= pageIoType && pageIoType <= PageIO.T_H2_EX_REF_INNER_END) ||
                    (PageIO.T_H2_EX_REF_MVCC_INNER_START <= pageIoType && pageIoType <= PageIO.T_H2_EX_REF_MVCC_INNER_END)
                )
                    return IndexPageType.INNER;
        }

        return IndexPageType.NOT_INDEX;
    }

    /**
     *
     */
    enum IndexPageType {
        /** */
        LEAF,

        /** */
        INNER,

        /** */
        NOT_INDEX;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "IoStatisticsHolderIndex{" + "logicalReadLeafCtr=" + logicalReadLeafCtr +
            ", logicalReadInnerCtr=" + logicalReadInnerCtr +
            ", physicalReadLeafCtr=" + physicalReadLeafCtr +
            ", physicalReadInnerCtr=" + physicalReadInnerCtr +
            ", cacheName='" + cacheName + '\'' +
            ", idxName='" + idxName + '\'' +
            '}';
    }
}
