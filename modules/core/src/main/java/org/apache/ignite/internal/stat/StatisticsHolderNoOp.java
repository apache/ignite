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

import java.util.Collections;
import java.util.Map;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;

public class StatisticsHolderNoOp implements StatisticsHolder {
    /** {@inheritDoc} */
    @Override public void trackLogicalRead(long pageAddr) {
//ToDo: remove it
        PageType pageType = PageType.derivePageType(PageIO.getType(pageAddr));
        AggregatePageType aggregatedPageType = AggregatePageType.aggregate(pageType);
        if (aggregatedPageType == AggregatePageType.OTHER) {

            boolean skip = false;
            for (StackTraceElement element : Thread.currentThread().getStackTrace()) {
                String s = element.toString().toLowerCase();
                if (s.contains("meta") || s.contains("initdata") || s.contains("initpage") || s.contains("inittree")
                    || s.contains("init0") || s.contains("recycle")) {
                    skip = true;
                    break;
                }
            }
            if (!skip)
                System.out.println("!!!!!!!--    ---!!!!!!!");

        }

    }

    /** {@inheritDoc} */
    @Override public void trackPhysicalAndLogicalRead(long pageAddr) {
    }

    /** {@inheritDoc} */
    @Override public Map<PageType, Long> logicalReadsMap() {
        return Collections.emptyMap();
    }

    /** {@inheritDoc} */
    @Override public Map<PageType, Long> physicalReadsMap() {
        return Collections.emptyMap();
    }

    /** {@inheritDoc} */
    @Override public void resetStatistics() {
    }
}
