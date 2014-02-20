// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.misc.resources;

import org.gridgain.grid.*;
import org.gridgain.grid.resources.*;

import java.util.*;

/**
 * Context class for adding data in storage.
 * Context store all data in {@link ArrayList} by default.
 * Only one context object will be initialized during task deployment.
 *
 * @author @java.author
 * @version @java.version
 */
public class GridResourcesContext {
    /** */
    @GridInstanceResource
    private Grid grid;

    /** */
    @GridLocalNodeIdResource
    private UUID nodeId;

    /** */
    private List<String> storage;

    /** */
    private String storageName;

    /** */
    private boolean started;

    /** */
    @SuppressWarnings("unused")
    @GridUserResourceOnDeployed private void deploy() {
        assert grid != null;
        assert nodeId != null;

        GridNode node = grid.localNode();

        // Get storage parameters from node attributes.
        storageName = (String)node.attribute("storage.name");

        if (storageName == null) {
            storageName = "test-storage";
        }

        storage = new ArrayList<>();

        started = true;

        System.out.println("Starting context.");
    }

    /** */
    @SuppressWarnings("unused")
    @GridUserResourceOnUndeployed private void undeploy() {
        assert grid != null;
        assert nodeId != null;

        System.out.println("Stopping context.");
        System.out.println("Storage data: " + storage);
    }

    /**
     * Add data in storage.
     *
     * @param data Data to add.
     * @return Return {@code true} if data added.
     */
    public boolean sendData(String data) {
        if (!started)
            return false;

        assert storage != null;

        storage.add("Data [storageName=" + storageName +
            ", nodeId=" + nodeId.toString() +
            ", date=" + new Date() +
            ", data=" + data +
            ']');

        return true;
    }
}
