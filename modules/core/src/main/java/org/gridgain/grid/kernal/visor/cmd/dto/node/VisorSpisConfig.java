/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor.cmd.dto.node;

import org.gridgain.grid.lang.*;

import java.io.*;
import java.util.*;

/**
 * SPIs configuration data.
 */
public class VisorSpisConfig implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    private final GridBiTuple<String, Map<String, Object>> discoSpi;
    private final GridBiTuple<String, Map<String, Object>> commSpi;
    private final GridBiTuple<String, Map<String, Object>> evtSpi;
    private final GridBiTuple<String, Map<String, Object>> colSpi;
    private final GridBiTuple<String, Map<String, Object>> authSpi;
    private final GridBiTuple<String, Map<String, Object>> sesSpi;
    private final GridBiTuple<String, Map<String, Object>> deploySpi;
    private final GridBiTuple<String, Map<String, Object>>[] cpSpis;
    private final GridBiTuple<String, Map<String, Object>>[] failSpis;
    private final GridBiTuple<String, Map<String, Object>>[] loadBalancingSpis;
    private final GridBiTuple<String, Map<String, Object>> swapSpaceSpis;
    private final GridBiTuple<String, Map<String, Object>>[] indexingSpi;

    public VisorSpisConfig(GridBiTuple<String, Map<String, Object>> discoSpi,
        GridBiTuple<String, Map<String, Object>> commSpi,
        GridBiTuple<String, Map<String, Object>> evtSpi,
        GridBiTuple<String, Map<String, Object>> colSpi,
        GridBiTuple<String, Map<String, Object>> authSpi,
        GridBiTuple<String, Map<String, Object>> sesSpi,
        GridBiTuple<String, Map<String, Object>> deploySpi,
        GridBiTuple<String, Map<String, Object>>[] cpSpis,
        GridBiTuple<String, Map<String, Object>>[] failSpis,
        GridBiTuple<String, Map<String, Object>>[] loadBalancingSpis,
        GridBiTuple<String, Map<String, Object>> swapSpaceSpis,
        GridBiTuple<String, Map<String, Object>>[] indexingSpi) {
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
     * @return Disco spi.
     */
    public GridBiTuple<String, Map<String, Object>> discoverySpi() {
        return discoSpi;
    }

    /**
     * @return Communication spi.
     */
    public GridBiTuple<String, Map<String, Object>> communicationSpi() {
        return commSpi;
    }

    /**
     * @return Event spi.
     */
    public GridBiTuple<String, Map<String, Object>> eventStorageSpi() {
        return evtSpi;
    }

    /**
     * @return Column spi.
     */
    public GridBiTuple<String, Map<String, Object>> collisionSpi() {
        return colSpi;
    }

    /**
     * @return Auth spi.
     */
    public GridBiTuple<String, Map<String, Object>> authenticationSpi() {
        return authSpi;
    }

    /**
     * @return Session spi.
     */
    public GridBiTuple<String, Map<String, Object>> secureSessionSpi() {
        return sesSpi;
    }

    /**
     * @return Deploy spi.
     */
    public GridBiTuple<String, Map<String, Object>> deploymentSpi() {
        return deploySpi;
    }

    /**
     * @return Copy spis.
     */
    public GridBiTuple<String, Map<String, Object>>[] checkpointSpis() {
        return cpSpis;
    }

    /**
     * @return Fail spis.
     */
    public GridBiTuple<String, Map<String, Object>>[] failoverSpis() {
        return failSpis;
    }

    /**
     * @return Load balancing spis.
     */
    public GridBiTuple<String, Map<String, Object>>[] loadBalancingSpis() {
        return loadBalancingSpis;
    }

    /**
     * @return Swap space spis.
     */
    public GridBiTuple<String, Map<String, Object>> swapSpaceSpi() {
        return swapSpaceSpis;
    }

    /**
     * @return Indexing spi.
     */
    public GridBiTuple<String, Map<String, Object>>[] indexingSpi() {
        return indexingSpi;
    }
}
