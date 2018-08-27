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

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCluster;
import org.apache.ignite.IgniteEvents;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.events.CacheEvent;
import org.apache.ignite.events.EventType;
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
    final String TEST_CACHE = "testCache";

    /** Flink source context. */
    SourceFunction.SourceContext<CacheEvent> context;

    /** Ignite instance. */
    Ignite ignite;

    /** Ignite events. */
    IgniteEvents igniteEvents;

    /** Ignite Cluster instance */
    IgniteCluster igniteCluster;

    /** Task Remote Filter */
    TaskRemoteFilter taskRemoteFilter;

    /** Cluster Group */
    ClusterGroup clsGrp;

    /** Ignite cache instance */
    IgniteCache<Object, Object> cache;

    /** Ignite Source instance */
    IgniteSource igniteSrc;

    /** Ignite Source Local Listener */
    IgniteSource.TaskLocalListener localListener;

    /** List of Cache Events */
    List<CacheEvent> cacheEvents;

    public void beforeTest() throws Exception{
        context = mock(SourceFunction.SourceContext.class);
        ignite = mock(Ignite.class);
        igniteEvents = mock(IgniteEvents.class);
        igniteCluster = mock(IgniteCluster.class);
        taskRemoteFilter = mock(TaskRemoteFilter.class);
        clsGrp = mock(ClusterGroup.class);
        cache = mock(IgniteCache.class);
        cacheEvents = mock(ArrayList.class);

        when(context.getCheckpointLock()).thenReturn(new Object());
        when(ignite.events(clsGrp)).thenReturn(igniteEvents);
        when(ignite.cluster()).thenReturn(igniteCluster);
        when(ignite.getOrCreateCache(TEST_CACHE)).thenReturn(cache);

        cache = ignite.getOrCreateCache(TEST_CACHE);
        igniteSrc = new IgniteSource(TEST_CACHE);
        igniteSrc.setIgnite(ignite);
        igniteSrc.setEvtBatchSize(1);
        igniteSrc.setEvtBufTimeout(1);
        igniteSrc.setRuntimeContext(createRuntimeContext());

        localListener = igniteSrc.getLocLsnr();
        when(igniteEvents.remoteListen(localListener, taskRemoteFilter, EventType.EVT_CACHE_OBJECT_PUT ))
            .thenReturn(UUID.randomUUID());
        when(igniteCluster.forCacheNodes(TEST_CACHE)).thenReturn(clsGrp);
    }

    private RuntimeContext createRuntimeContext() {
        StreamingRuntimeContext runtimeContext = mock(StreamingRuntimeContext.class);
        when(runtimeContext.isCheckpointingEnabled()).thenReturn(true);
        return runtimeContext;
    }

    public void testIgniteSourceStart() throws Exception {
        igniteSrc.start(null, EventType.EVT_CACHE_OBJECT_PUT);
        verify(ignite.events(clsGrp), times(1));
    }

    public void testIgniteSourceRun() throws Exception {

        Thread t = new Thread(new Runnable() {
            @Override public void run() {
                try {
                    igniteSrc.start(null, EventType.EVT_CACHE_OBJECT_PUT);
                }
                catch (Exception e) {
                    e.printStackTrace();
                }
                
                try {
                    igniteSrc.run(context);
                }
                catch (Exception e) {
                    e.printStackTrace();
                    fail("Stream execution process failed.");
                }
            }
        });

        t.start();
        Thread.sleep(3000);
        igniteSrc.stopped = true;
    }
}
