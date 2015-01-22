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

package org.gridgain.grid.kernal.processors.streamer;

import org.apache.ignite.cluster.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.streamer.*;
import org.apache.ignite.streamer.index.*;
import org.apache.ignite.internal.util.typedef.*;
import org.gridgain.testframework.junits.common.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Test for {@link org.apache.ignite.lifecycle.LifecycleAware} support in {@link org.apache.ignite.streamer.StreamerConfiguration}.
 */
public class GridStreamerLifecycleAwareSelfTest extends GridAbstractLifecycleAwareSelfTest {
    /**
     */
    private static class TestEventRouter extends TestLifecycleAware implements StreamerEventRouter {
        /**
         */
        TestEventRouter() {
            super(null);
        }

        /** {@inheritDoc} */
        @Nullable @Override public <T> ClusterNode route(StreamerContext ctx, String stageName, T evt) {
            return null;
        }

        /** {@inheritDoc} */
        @Nullable @Override public <T> Map<ClusterNode, Collection<T>> route(StreamerContext ctx,
            String stageName, Collection<T> evts) {
            return null;
        }
    }

    /**
     */
    private static class TestStage extends TestLifecycleAware implements StreamerStage {
        /**
         */
        TestStage() {
            super(null);
        }

        /** {@inheritDoc} */
        @Override public String name() {
            return "dummy";
        }

        /** {@inheritDoc} */
        @Nullable @Override public Map<String, Collection<?>> run(StreamerContext ctx, Collection evts) {
            return null;
        }
    }

    /**
     */
    private static class TestWindow extends TestLifecycleAware implements StreamerWindow {
        /**
         */
        TestWindow() {
            super(null);
        }

        /** {@inheritDoc} */
        @Override public String name() {
            return "dummy";
        }

        /** {@inheritDoc} */
        @Nullable @Override public StreamerIndex index() {
            return null;
        }

        /** {@inheritDoc} */
        @Nullable @Override public StreamerIndex index(@Nullable String name) {
            return null;
        }

        /** {@inheritDoc} */
        @Override public Collection<StreamerIndex> indexes() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public void reset() {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public int size() {
            return 0;
        }

        /** {@inheritDoc} */
        @Override public int evictionQueueSize() {
            return 0;
        }

        /** {@inheritDoc} */
        @Override public boolean enqueue(Object evt) {
            return false;
        }

        /** {@inheritDoc} */
        @Override public boolean enqueue(Object... evts) {
            return false;
        }

        /** {@inheritDoc} */
        @Override public boolean enqueueAll(Collection evts) {
            return false;
        }

        /** {@inheritDoc} */
        @Nullable @Override public Object dequeue() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public Collection dequeue(int cnt) {
            return null;
        }

        /** {@inheritDoc} */
        @Override public Collection dequeueAll() {
            return null;
        }

        /** {@inheritDoc} */
        @Nullable @Override public Object pollEvicted() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public Collection pollEvicted(int cnt) {
            return null;
        }

        /** {@inheritDoc} */
        @Override public Collection pollEvictedBatch() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public Collection pollEvictedAll() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public void clearEvicted() {
        }

        /** {@inheritDoc} */
        @Override public Collection snapshot(boolean includeIvicted) {
            return null;
        }

        /** {@inheritDoc} */
        @Override public Iterator iterator() {
            return null;
        }
    }

    /** {@inheritDoc} */
    @Override protected final IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        StreamerConfiguration streamerCfg = new StreamerConfiguration();

        TestEventRouter router = new TestEventRouter();

        streamerCfg.setRouter(router);

        lifecycleAwares.add(router);

        TestStage stage = new TestStage();

        streamerCfg.setStages(F.asList((StreamerStage)stage));

        lifecycleAwares.add(stage);

        TestWindow window = new TestWindow();

        streamerCfg.setWindows(F.asList((StreamerWindow)window));

        lifecycleAwares.add(window);

        cfg.setStreamerConfiguration(streamerCfg);

        return cfg;
    }
}
