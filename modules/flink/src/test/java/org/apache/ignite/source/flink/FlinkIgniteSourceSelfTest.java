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

package org.apache.ignite.source.flink;

import java.util.UUID;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCluster;
import org.apache.ignite.IgniteEvents;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.events.CacheEvent;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link IgniteSource}.
 */
public class FlinkIgniteSourceSelfTest extends GridCommonAbstractTest {
    /** Cache name. */
    private static final String TEST_CACHE = "testCache";

    /** Flink source context. */
    private SourceFunction.SourceContext<CacheEvent> context;

    /** Ignite instance. */
    private Ignite ignite;

    /** Cluster Group */
    private ClusterGroup clsGrp;

    /** Ignite Source instance */
    private IgniteSource igniteSrc;

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public void beforeTest() throws Exception {
        context = mock(SourceFunction.SourceContext.class);
        ignite = mock(Ignite.class);
        clsGrp = mock(ClusterGroup.class);

        IgniteEvents igniteEvents = mock(IgniteEvents.class);
        IgniteCluster igniteCluster = mock(IgniteCluster.class);
        TaskRemoteFilter taskRemoteFilter = mock(TaskRemoteFilter.class);

        when(context.getCheckpointLock()).thenReturn(new Object());
        when(ignite.events(clsGrp)).thenReturn(igniteEvents);
        when(ignite.cluster()).thenReturn(igniteCluster);

        igniteSrc = new IgniteSource(TEST_CACHE);
        igniteSrc.setIgnite(ignite);
        igniteSrc.setEvtBatchSize(1);
        igniteSrc.setEvtBufTimeout(1);
        igniteSrc.setRuntimeContext(createRuntimeContext());

        IgniteBiPredicate localListener = igniteSrc.getLocLsnr();

        when(igniteEvents.remoteListen(localListener, taskRemoteFilter, EventType.EVT_CACHE_OBJECT_PUT ))
            .thenReturn(UUID.randomUUID());

        when(igniteCluster.forCacheNodes(TEST_CACHE)).thenReturn(clsGrp);
    }

    /** {@inheritDoc} */
    @Override public void afterTest() throws Exception{
        igniteSrc.cancel();
    }

    /** Creates streaming runtime context */
    private RuntimeContext createRuntimeContext() {
        StreamingRuntimeContext runtimeContext = mock(StreamingRuntimeContext.class);

        when(runtimeContext.isCheckpointingEnabled()).thenReturn(true);

        return runtimeContext;
    }

    /**
     * Tests Ignite source start operation.
     *
     * @throws Exception If failed.
     */
    public void testIgniteSourceStart() throws Exception {
        igniteSrc.start(null, EventType.EVT_CACHE_OBJECT_PUT);

        verify(ignite.events(clsGrp), times(1));
    }

    /**
     * Tests Ignite source run operation.
     *
     * @throws Exception If failed.
     */
    public void testIgniteSourceRun() throws Exception {
        IgniteInternalFuture f = GridTestUtils.runAsync(new Runnable() {
            @Override public void run() {
                try {
                    igniteSrc.start(null, EventType.EVT_CACHE_OBJECT_PUT);

                    igniteSrc.run(context);
                }
                catch (Throwable e) {
                    igniteSrc.cancel();

                   throw new AssertionError("Unexpected failure.", e);
                }
            }
        });

        long endTime = System.currentTimeMillis() + 2000;

        GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                return f.isDone() || System.currentTimeMillis() > endTime;
            }
        }, 3000);

        igniteSrc.cancel();

        f.get(3000);
    }
}
