/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.client.impl;

import org.gridgain.client.*;
import org.gridgain.testframework.junits.common.*;

import java.util.*;
import java.util.concurrent.*;

import static org.gridgain.testframework.GridTestUtils.*;

/**
 * Simple unit test for GridClientComputeImpl which checks method parameters.
 * It tests only those methods that can produce assertion underneath upon incorrect arguments.
 */
public class GridClientComputeImplSelfTest extends GridCommonAbstractTest {
    /** Mocked client compute. */
    private GridClientCompute compute = allocateInstance0(GridClientComputeImpl.class);

    /**
     * @throws Exception If failed.
     */
    public void testProjection_byGridClientNode() throws Exception {
        assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return compute.projection((GridClientNode)null);
            }
        }, NullPointerException.class, "Ouch! Argument cannot be null: node");
    }

    /**
     * @throws Exception If failed.
     */
    public void testExecute() throws Exception {
        assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return compute.execute(null, null);
            }
        }, NullPointerException.class, "Ouch! Argument cannot be null: taskName");
    }

    /**
     * @throws Exception If failed.
     */
    public void testExecuteAsync() throws Exception {
        assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return compute.executeAsync(null, null);
            }
        }, NullPointerException.class, "Ouch! Argument cannot be null: taskName");
    }

    /**
     * @throws Exception If failed.
     */
    public void testAffinityExecute() throws Exception {
        assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return compute.affinityExecute(null, "cache", "key", null);
            }
        }, NullPointerException.class, "Ouch! Argument cannot be null: taskName");
    }

    /**
     * @throws Exception If failed.
     */
    public void testAffinityExecuteAsync() throws Exception {
        assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return compute.affinityExecute(null, "cache", "key", null);
            }
        }, NullPointerException.class, "Ouch! Argument cannot be null: taskName");
    }

    /**
     * @throws Exception If failed.
     */
    public void testNode() throws Exception {
        assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return compute.node(null);
            }
        }, NullPointerException.class, "Ouch! Argument cannot be null: id");
    }

    /**
     * @throws Exception If failed.
     */
    public void testNodesByIds() throws Exception {
        assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return compute.nodes((Collection<UUID>)null);
            }
        }, NullPointerException.class, "Ouch! Argument cannot be null: ids");
    }

    /**
     * @throws Exception If failed.
     */
    public void testNodesByFilter() throws Exception {
        assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return compute.nodes((GridClientPredicate<GridClientNode>)null);
            }
        }, NullPointerException.class, "Ouch! Argument cannot be null: filter");
    }

    /**
     * @throws Exception If failed.
     */
    public void testRefreshNodeById() throws Exception {
        assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return compute.refreshNode((UUID)null, false, false);
            }
        }, NullPointerException.class, "Ouch! Argument cannot be null: id");
    }

    /**
     * @throws Exception If failed.
     */
    public void testRefreshNodeByIdAsync() throws Exception {
        assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return compute.refreshNodeAsync((UUID)null, false, false);
            }
        }, NullPointerException.class, "Ouch! Argument cannot be null: id");
    }

    /**
     * @throws Exception If failed.
     */
    public void testRefreshNodeByIp() throws Exception {
        assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return compute.refreshNode((String)null, false, false);
            }
        }, NullPointerException.class, "Ouch! Argument cannot be null: ip");
    }

    /**
     * @throws Exception If failed.
     */
    public void testRefreshNodeByIpAsync() throws Exception {
        assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return compute.refreshNode((String)null, false, false);
            }
        }, NullPointerException.class, "Ouch! Argument cannot be null: ip");
    }
}
