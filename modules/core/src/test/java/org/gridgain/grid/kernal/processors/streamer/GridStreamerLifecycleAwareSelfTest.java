/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.streamer;

import org.apache.ignite.cluster.*;
import org.apache.ignite.configuration.*;
import org.gridgain.grid.streamer.*;
import org.gridgain.grid.streamer.index.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.testframework.junits.common.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Test for {@link org.apache.ignite.lifecycle.LifecycleAware} support in {@link GridStreamerConfiguration}.
 */
public class GridStreamerLifecycleAwareSelfTest extends GridAbstractLifecycleAwareSelfTest {
    /**
     */
    private static class TestEventRouter extends TestLifecycleAware implements GridStreamerEventRouter {
        /**
         */
        TestEventRouter() {
            super(null);
        }

        /** {@inheritDoc} */
        @Nullable @Override public <T> ClusterNode route(GridStreamerContext ctx, String stageName, T evt) {
            return null;
        }

        /** {@inheritDoc} */
        @Nullable @Override public <T> Map<ClusterNode, Collection<T>> route(GridStreamerContext ctx,
            String stageName, Collection<T> evts) {
            return null;
        }
    }

    /**
     */
    private static class TestStage extends TestLifecycleAware implements GridStreamerStage {
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
        @Nullable @Override public Map<String, Collection<?>> run(GridStreamerContext ctx, Collection evts) {
            return null;
        }
    }

    /**
     */
    private static class TestWindow extends TestLifecycleAware implements GridStreamerWindow {
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
        @Nullable @Override public GridStreamerIndex index() {
            return null;
        }

        /** {@inheritDoc} */
        @Nullable @Override public GridStreamerIndex index(@Nullable String name) {
            return null;
        }

        /** {@inheritDoc} */
        @Override public Collection<GridStreamerIndex> indexes() {
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

        GridStreamerConfiguration streamerCfg = new GridStreamerConfiguration();

        TestEventRouter router = new TestEventRouter();

        streamerCfg.setRouter(router);

        lifecycleAwares.add(router);

        TestStage stage = new TestStage();

        streamerCfg.setStages(F.asList((GridStreamerStage)stage));

        lifecycleAwares.add(stage);

        TestWindow window = new TestWindow();

        streamerCfg.setWindows(F.asList((GridStreamerWindow)window));

        lifecycleAwares.add(window);

        cfg.setStreamerConfiguration(streamerCfg);

        return cfg;
    }
}
