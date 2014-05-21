/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor.cmd.dto;

import java.io.*;

/**
 * SPIs configuration data.
 */
public class VisorSpisConfig implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    private final String discoSpi;
    private final String commSpi;
    private final String evtSpi;
    private final String colSpi;
    private final String authSpi;
    private final String sesSpi;
    private final String deploySpi;
    private final String cpSpis;
    private final String failSpis;
    private final String loadBalancingSpis;
    private final String swapSpaceSpis;

    public VisorSpisConfig(String discoSpi, String commSpi, String evtSpi, String colSpi, String authSpi,
        String sesSpi, String deploySpi, String cpSpis, String failSpis, String loadBalancingSpis,
        String swapSpaceSpis) {
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
    }

    /**
     * @return Disco spi.
     */
    public String discoSpi() {
        return discoSpi;
    }

    /**
     * @return Communication spi.
     */
    public String communicationSpi() {
        return commSpi;
    }

    /**
     * @return Event spi.
     */
    public String eventSpi() {
        return evtSpi;
    }

    /**
     * @return Column spi.
     */
    public String columnSpi() {
        return colSpi;
    }

    /**
     * @return Auth spi.
     */
    public String authSpi() {
        return authSpi;
    }

    /**
     * @return Session spi.
     */
    public String sessionSpi() {
        return sesSpi;
    }

    /**
     * @return Deploy spi.
     */
    public String deploySpi() {
        return deploySpi;
    }

    /**
     * @return Copy spis.
     */
    public String cpSpis() {
        return cpSpis;
    }

    /**
     * @return Fail spis.
     */
    public String failSpis() {
        return failSpis;
    }

    /**
     * @return Load balancing spis.
     */
    public String loadBalancingSpis() {
        return loadBalancingSpis;
    }

    /**
     * @return Swap space spis.
     */
    public String swapSpaceSpis() {
        return swapSpaceSpis;
    }
}
