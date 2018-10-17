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

import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.LongAdder;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;

public class StatisticsHolderFineGrained implements StatisticsHolder {
    /** */
    private Map<PageType, LongAdder> logicalReadCntrPerPageType = new EnumMap<>(PageType.class);
    /** */
    private Map<PageType, LongAdder> physicalReadCntrPerPageType = new EnumMap<>(PageType.class);

    /** Type of statistics. */
    private final StatType type;

    /** Subtype of statistic. */
    private final String subType;

    /**
     * @param type type of statistic.
     * @param subType subtype of statistic.
     */
    public StatisticsHolderFineGrained(StatType type, String subType) {
        assert type != null && subType != null;

        this.type = type;
        this.subType = subType;

        for (PageType pageType : PageType.values()) {
            logicalReadCntrPerPageType.put(pageType, new LongAdder());

            physicalReadCntrPerPageType.put(pageType, new LongAdder());
        }
    }

    /** {@inheritDoc} */
    @Override public void trackLogicalRead(long pageAddr) {
        int pageIoType = PageIO.getType(pageAddr);

        if (pageIoType > 0) { // To skip not set type.
            PageType pageType = PageType.derivePageType(pageIoType);

            logicalReadCntrPerPageType.get(pageType).increment();
            System.out.println("!!! logical " + pageType + " " + type + " " + subType + " " + logicalReadCntrPerPageType);
        }
    }

    /** {@inheritDoc} */
    @Override public void trackPhysicalAndLogicalRead(long pageAddr) {
        int pageIoType = PageIO.getType(pageAddr);

        if (pageIoType > 0) { // To skip not set type.
            PageType pageType = PageType.derivePageType(pageIoType);

            logicalReadCntrPerPageType.get(pageType).increment();

            physicalReadCntrPerPageType.get(pageType).increment();
            System.out.println("!!! physical " + pageType + " " + type + " " + subType + " " + physicalReadCntrPerPageType);
        }
    }

    /** {@inheritDoc} */
    @Override public Map<PageType, Long> logicalReadsMap() {
        Map<PageType, Long> res = new HashMap<>();

        logicalReadCntrPerPageType.forEach((type, cntr) -> {
            long val = cntr.longValue();

            if (val != 0)
                res.put(type, val);
        });

        return res;
    }

    /** {@inheritDoc} */
    @Override public Map<PageType, Long> physicalReadsMap() {
        Map<PageType, Long> res = new HashMap<>();

        physicalReadCntrPerPageType.forEach((type, cntr) -> {
            long val = cntr.longValue();

            if (val != 0)
                res.put(type, val);
        });

        return res;
    }

    /** {@inheritDoc} */
    @Override public void resetStatistics() {
        logicalReadCntrPerPageType.values().forEach(LongAdder::reset);

        physicalReadCntrPerPageType.values().forEach(LongAdder::reset);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "StatisticsHolderFineGrained{" + "logicalReadCounterPerPageType=" + logicalReadCntrPerPageType +
            ", physicalReadCounterPerPageType=" + physicalReadCntrPerPageType +
            ", type=" + type +
            ", subType='" + subType + '\'' +
            '}';
    }

}
