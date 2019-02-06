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
 */

package org.apache.ignite.internal.processors.cache.index;

import java.util.Set;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.h2.engine.Session;
import org.h2.util.CloseWatcher;

/**
 * Base class for all indexing tests to check H2 connection management.
 */
public class AbstractIndexingCommonTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        checkAllConnectionAreClosed();

        super.afterTestsStopped();
    }

    /**
     * Checks all H2 connection are closed.
     */
    void checkAllConnectionAreClosed() {
        Set<Object> refs = GridTestUtils.getFieldValue(CloseWatcher.class, "refs");

        if (!refs.isEmpty()) {
            for (Object o : refs) {
                if (o instanceof CloseWatcher
                    && ((CloseWatcher)o).getCloseable() instanceof Session) {
                    log.error("Session: " + ((CloseWatcher)o).getCloseable()
                        + ", open=" + !((Session)((CloseWatcher)o).getCloseable()).isClosed());
                }
            }

            // Uncomment and use heap dump to investigate the problem if the test failed.
            // GridDebug.dumpHeap("h2_conn_heap_dmp.hprof", true);

            fail("There are not closed connections. See the log above.");
        }
    }

}
