/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal;

import org.gridgain.grid.*;
import org.gridgain.grid.events.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.testframework.junits.common.*;

import java.util.concurrent.atomic.*;

import static org.gridgain.grid.events.GridEventType.*;

/**
 * Tests for runtime events configuration.
 */
public class GridEventStorageRuntimeConfigurationSelfTest extends GridCommonAbstractTest {
    /** */
    private int[] inclEvtTypes;

    /** {@inheritDoc} */
    @SuppressWarnings("deprecation")
    @Override protected GridConfiguration getConfiguration(String gridName) throws Exception {
        GridConfiguration cfg = super.getConfiguration(gridName);

        cfg.setIncludeEventTypes(inclEvtTypes);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testEnableWithDefaults() throws Exception {
        inclEvtTypes = null;

        try {
            Grid g = startGrid();

            final AtomicInteger cnt = new AtomicInteger();

            g.events().localListen(new GridPredicate<GridEvent>() {
                @Override public boolean apply(GridEvent evt) {
                    cnt.incrementAndGet();

                    return true;
                }
            }, EVT_TASK_STARTED);

            g.compute().run(F.noop()).get();

            assertEquals(0, cnt.get());

            g.events().enableLocal(EVT_TASK_STARTED);

            g.compute().run(F.noop()).get();

            assertEquals(1, cnt.get());
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testEnableWithIncludes() throws Exception {
        inclEvtTypes = new int[] { EVT_TASK_STARTED, EVT_TASK_FINISHED };

        try {
            Grid g = startGrid();

            final AtomicInteger cnt = new AtomicInteger();

            g.events().localListen(new GridPredicate<GridEvent>() {
                @Override public boolean apply(GridEvent evt) {
                    cnt.incrementAndGet();

                    return true;
                }
            }, EVT_TASK_STARTED, EVT_TASK_FINISHED, EVT_JOB_STARTED);

            g.compute().run(F.noop()).get();

            assertEquals(2, cnt.get());

            g.events().enableLocal(EVT_TASK_FINISHED, EVT_JOB_STARTED);

            g.compute().run(F.noop()).get();

            assertEquals(5, cnt.get());
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testDisableWithIncludes() throws Exception {
        inclEvtTypes = null;

        try {
            Grid g = startGrid();

            g.events().enableLocal(EVT_TASK_STARTED, EVT_TASK_FINISHED, EVT_JOB_STARTED);

            final AtomicInteger cnt = new AtomicInteger();

            g.events().localListen(new GridPredicate<GridEvent>() {
                @Override public boolean apply(GridEvent evt) {
                    cnt.incrementAndGet();

                    return true;
                }
            }, EVT_TASK_STARTED, EVT_TASK_FINISHED, EVT_JOB_STARTED);

            g.compute().run(F.noop()).get();

            assertEquals(3, cnt.get());

            g.events().disableLocal(EVT_TASK_STARTED, EVT_TASK_FINISHED, EVT_JOB_FAILED);

            g.compute().run(F.noop()).get();

            assertEquals(4, cnt.get());
        }
        finally {
            stopAllGrids();
        }
    }
}
