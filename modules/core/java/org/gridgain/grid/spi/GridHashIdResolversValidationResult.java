/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi;

import java.util.*;

/**
 * Result of hash id resolvers validation.
 *
 * @author @java.author
 * @version @java.version
 */
public class GridHashIdResolversValidationResult {
    /** Offending cache name. */
    private String cacheName;

    /** Hash ID resolver class name. */
    private String rslvrCls;

    /** Offending node ID. */
    private UUID nodeId;

    /**
     * @param cacheName Offending cache name.
     * @param rslvrCls Hash ID resolver class name.
     * @param nodeId Offending node ID.
     */
    public GridHashIdResolversValidationResult(String cacheName, String rslvrCls, UUID nodeId) {
        this.cacheName = cacheName;
        this.rslvrCls = rslvrCls;
        this.nodeId = nodeId;
    }


    /**
     * @return Offending cache name.
     */
    public String cacheName() {
        return cacheName;
    }

    /**
     * @return Hash ID resolver class name.
     */
    public String resolverClass() {
        return rslvrCls;
    }

    /**
     * @return Offending node ID.
     */
    public UUID nodeId() {
        return nodeId;
    }
}
