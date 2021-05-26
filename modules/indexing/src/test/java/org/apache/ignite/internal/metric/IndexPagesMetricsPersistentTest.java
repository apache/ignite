/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
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
 */

package org.apache.ignite.internal.metric;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.persistence.DataRegion;
import org.apache.ignite.internal.processors.cache.persistence.DataRegionMetricsImpl;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.PageMetrics;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class IndexPagesMetricsPersistentTest extends AbstractIndexPageMetricsTest {
    /** {@inheritDoc} */
    @Override boolean isPersistenceEnabled() {
        return true;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        cleanPersistenceDir();

        super.beforeTest();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override void validateIdxPagesCnt() throws IgniteCheckedException {
        DataRegion dataRegion = defaultDataRegion();
        DataRegionMetricsImpl dataRegionMetrics = dataRegion.metrics();

        long totalIdxPages = 0;

        for (IgniteInternalCache<?, ?> cache : gridCacheProcessor().caches()) {
            int grpId = cache.context().groupId();

            long idxPages = indexPageCounter.countIdxPagesInMemory(grpId);

            PageMetrics metrics = dataRegionMetrics.cacheGrpPageMetrics(grpId);

            assertThat(metrics.indexPages().value(), is(idxPages));

            totalIdxPages += idxPages;
        }

        assertThat(dataRegionMetrics.pageMetrics().indexPages().value(), is(totalIdxPages));
    }
}
