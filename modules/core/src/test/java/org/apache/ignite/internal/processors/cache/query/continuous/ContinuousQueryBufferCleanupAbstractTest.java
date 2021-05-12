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

package org.apache.ignite.internal.processors.cache.query.continuous;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.AbstractContinuousQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.continuous.GridContinuousProcessor;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.internal.processors.cache.query.continuous.CacheContinuousQueryHandler.DFLT_CONTINUOUS_QUERY_BACKUP_ACK_THRESHOLD;
import static org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi.DFLT_ACK_SND_THRESHOLD;

/**
 * Test for continuous query buffer cleanup.
 */
@RunWith(Parameterized.class)
public abstract class ContinuousQueryBufferCleanupAbstractTest extends GridCommonAbstractTest {
    /** */
    private static final int RECORDS_CNT = 10000;

    /** */
    private static final String REMOTE_ROUTINE_INFO_CLASS_NAME = "org.apache.ignite.internal.processors.continuous.GridContinuousProcessor$RemoteRoutineInfo";

    /** */
    private static final String LOCAL_ROUTINE_INFO_CLASS_NAME = "org.apache.ignite.internal.processors.continuous.GridContinuousProcessor$LocalRoutineInfo";

    /** */
    private static final String BATCH_CLASS_NAME = "org.apache.ignite.internal.processors.cache.query.continuous.CacheContinuousQueryEventBuffer$Batch";

    /** Continuous query updates counter */
    protected final AtomicInteger updCntr = new AtomicInteger();

    /** */
    @Parameterized.Parameter(value = 0)
    public int srvCnt;

    /** */
    @Parameterized.Parameter(value = 1)
    public int backupsCnt;

    /** */
    @Parameterized.Parameter(value = 2)
    public boolean useClient;

    /** */
    @Parameterized.Parameters(name = "{0} {1} {2}")
    public static List<Object[]> parameters() {
        ArrayList<Object[]> params = new ArrayList<>();

        params.add(new Object[]{1, 0, true});
        params.add(new Object[]{2, 0, true});
        params.add(new Object[]{2, 1, true});
        params.add(new Object[]{2, 1, false});

        return params;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /** Provides continuous query for the test. */
    protected abstract AbstractContinuousQuery<Integer, String> getContinuousQuery();

    /** Test starts the cluster with specified count of nodes and checks if CQ buffers were cleaned up. */
    @Test
    public void testBufferCleanup() throws Exception {
        for (int i = 0; i < srvCnt; i++)
            startGrid(i);

        Ignite qryNode = useClient ? startClientGrid() : grid(0);

        CacheConfiguration<Integer, String> cacheCfg = new CacheConfiguration<Integer, String>("testCache")
            .setBackups(backupsCnt)
            .setAffinity(new RendezvousAffinityFunction(32, null));

        IgniteCache<Integer, String> cache = qryNode.getOrCreateCache(cacheCfg);

        AbstractContinuousQuery<Integer, String> qry = getContinuousQuery();

        cache.query(qry);

        for (int i = 0; i < RECORDS_CNT; i++)
            cache.put(i, Integer.toString(i));

        GridTestUtils.waitForCondition(() -> updCntr.get() == RECORDS_CNT, 2000);

        for (int i = 0; i < srvCnt; i++)
            validateBuffer(grid(i), backupsCnt);
    }

    /** */
    private void validateBuffer(IgniteEx srv, int backupsCnt) throws ClassNotFoundException {
        GridContinuousProcessor contProc = srv.context().continuous();

        ConcurrentMap<UUID, Object> rmtInfos = GridTestUtils.getFieldValue(contProc, GridContinuousProcessor.class, "rmtInfos");

        CacheContinuousQueryHandler hnd;

        if (rmtInfos.values().isEmpty()) {
            ConcurrentMap<UUID, Object> locInfos = GridTestUtils.getFieldValue(contProc, GridContinuousProcessor.class, "locInfos");

            Object locRoutineInfo = locInfos.values().toArray()[0];

            hnd = GridTestUtils.getFieldValue(locRoutineInfo, Class.forName(LOCAL_ROUTINE_INFO_CLASS_NAME), "hnd");
        } else {
            Object rmtRoutineInfo = rmtInfos.values().toArray()[0];

            hnd = GridTestUtils.getFieldValue(rmtRoutineInfo, Class.forName(REMOTE_ROUTINE_INFO_CLASS_NAME), "hnd");
        }

        ConcurrentMap<Integer, CacheContinuousQueryEventBuffer> entryBufs = GridTestUtils.getFieldValue(hnd, CacheContinuousQueryHandler.class, "entryBufs");

        int notNullCnt = 0;

        for (CacheContinuousQueryEventBuffer evtBuf : entryBufs.values()) {
            AtomicReference<Object> curBatch = GridTestUtils.getFieldValue(evtBuf, CacheContinuousQueryEventBuffer.class, "curBatch");

            if (curBatch.get() != null) {
                CacheContinuousQueryEntry[] entries = GridTestUtils.getFieldValue(curBatch.get(), Class.forName(BATCH_CLASS_NAME), "entries");

                for (CacheContinuousQueryEntry entry : entries) {
                    if (entry != null)
                        notNullCnt++;
                }
            }
        }

        assertTrue(notNullCnt < DFLT_CONTINUOUS_QUERY_BACKUP_ACK_THRESHOLD + (1 + backupsCnt) * DFLT_ACK_SND_THRESHOLD);
    }
}
