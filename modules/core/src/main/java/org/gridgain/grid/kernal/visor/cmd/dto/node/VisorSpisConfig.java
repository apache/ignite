/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor.cmd.dto.node;

import org.gridgain.grid.util.typedef.*;

import java.io.*;
import java.util.*;

/**
 * SPIs configuration data.
 */
public class VisorSpisConfig implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Discovery SPI. */
    private final T2<String, Map<String, Object>> discoSpi;

    /** Communication SPI. */
    private final T2<String, Map<String, Object>> commSpi;

    /** Event storage SPI. */
    private final T2<String, Map<String, Object>> evtSpi;

    /** Collision SPI. */
    private final T2<String, Map<String, Object>> colSpi;

    /** Authentication SPI. */
    private final T2<String, Map<String, Object>> authSpi;

    /** Secure Session SPI. */
    private final T2<String, Map<String, Object>> sesSpi;

    /** Deployment SPI. */
    private final T2<String, Map<String, Object>> deploySpi;

    /** Checkpoint SPIs. */
    private final T2<String, Map<String, Object>>[] cpSpis;

    /** Failover SPIs. */
    private final T2<String, Map<String, Object>>[] failSpis;

    /** Load balancing SPIs. */
    private final T2<String, Map<String, Object>>[] loadBalancingSpis;

    /** Swap space SPIs. */
    private final T2<String, Map<String, Object>> swapSpaceSpis;

    /** Indexing SPIs. */
    private final T2<String, Map<String, Object>>[] indexingSpi;

    public VisorSpisConfig(T2<String, Map<String, Object>> discoSpi,
        T2<String, Map<String, Object>> commSpi,
        T2<String, Map<String, Object>> evtSpi,
        T2<String, Map<String, Object>> colSpi,
        T2<String, Map<String, Object>> authSpi,
        T2<String, Map<String, Object>> sesSpi,
        T2<String, Map<String, Object>> deploySpi,
        T2<String, Map<String, Object>>[] cpSpis,
        T2<String, Map<String, Object>>[] failSpis,
        T2<String, Map<String, Object>>[] loadBalancingSpis,
        T2<String, Map<String, Object>> swapSpaceSpis,
        T2<String, Map<String, Object>>[] indexingSpi) {
        this.discoSpi = discoSpi;
        this.commSpi = commSpi;
        this.evtSpi = evtSpi;
        this.colSpi = colSpi;
        this.authSpi = authSpi;
        this.sesSpi = sesSpi;
        this.deploySpi = deploySpi;
        this.cpSpis = cpSpis;
        this.failSpis = failSpis;
        this.loadBalancingSpis = loadBalancingSpis;
        this.swapSpaceSpis = swapSpaceSpis;
        this.indexingSpi = indexingSpi;
    }

    /**
     * @return Discovery SPI.
     */
    public T2<String, Map<String, Object>> discoverySpi() {
        return discoSpi;
    }

    /**
     * @return Communication SPI.
     */
    public T2<String, Map<String, Object>> communicationSpi() {
        return commSpi;
    }

    /**
     * @return Event storage SPI.
     */
    public T2<String, Map<String, Object>> eventStorageSpi() {
        return evtSpi;
    }

    /**
     * @return Collision SPI.
     */
    public T2<String, Map<String, Object>> collisionSpi() {
        return colSpi;
    }

    /**
     * @return Authentication SPI.
     */
    public T2<String, Map<String, Object>> authenticationSpi() {
        return authSpi;
    }

    /**
     * @return Secure Session SPI.
     */
    public T2<String, Map<String, Object>> secureSessionSpi() {
        return sesSpi;
    }

    /**
     * @return Deployment SPI.
     */
    public T2<String, Map<String, Object>> deploymentSpi() {
        return deploySpi;
    }

    /**
     * @return Checkpoint SPIs.
     */
    public T2<String, Map<String, Object>>[] checkpointSpis() {
        return cpSpis;
    }

    /**
     * @return Failover SPIs.
     */
    public T2<String, Map<String, Object>>[] failoverSpis() {
        return failSpis;
    }

    /**
     * @return Load balancing SPIs.
     */
    public T2<String, Map<String, Object>>[] loadBalancingSpis() {
        return loadBalancingSpis;
    }

    /**
     * @return Swap space SPIs.
     */
    public T2<String, Map<String, Object>> swapSpaceSpi() {
        return swapSpaceSpis;
    }

    /**
     * @return Indexing SPIs.
     */
    public T2<String, Map<String, Object>>[] indexingSpi() {
        return indexingSpi;
    }
}
