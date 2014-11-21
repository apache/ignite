/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor.dto.node;

import org.gridgain.grid.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.spi.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;
import java.lang.reflect.*;
import java.util.*;

import static org.gridgain.grid.kernal.visor.util.VisorTaskUtils.*;

/**
 * Data transfer object for node SPIs configuration properties.
 */
public class VisorSpisConfig implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Discovery SPI. */
    private GridBiTuple<String, Map<String, Object>> discoSpi;

    /** Communication SPI. */
    private GridBiTuple<String, Map<String, Object>> commSpi;

    /** Event storage SPI. */
    private GridBiTuple<String, Map<String, Object>> evtSpi;

    /** Collision SPI. */
    private GridBiTuple<String, Map<String, Object>> colSpi;

    /** Authentication SPI. */
    private GridBiTuple<String, Map<String, Object>> authSpi;

    /** Secure Session SPI. */
    private GridBiTuple<String, Map<String, Object>> sesSpi;

    /** Deployment SPI. */
    private GridBiTuple<String, Map<String, Object>> deploySpi;

    /** Checkpoint SPIs. */
    private GridBiTuple<String, Map<String, Object>>[] cpSpis;

    /** Failover SPIs. */
    private GridBiTuple<String, Map<String, Object>>[] failSpis;

    /** Load balancing SPIs. */
    private GridBiTuple<String, Map<String, Object>>[] loadBalancingSpis;

    /** Swap space SPIs. */
    private GridBiTuple<String, Map<String, Object>> swapSpaceSpis;

    /** Indexing SPIs. */
    private GridBiTuple<String, Map<String, Object>>[] indexingSpis;

    /**
     * Collects SPI information based on GridSpiConfiguration-annotated methods.
     * Methods with {@code Deprecated} annotation are skipped.
     *
     * @param spi SPI to collect information on.
     * @return Tuple where first component is SPI name and
     */
    private static GridBiTuple<String, Map<String, Object>> collectSpiInfo(GridSpi spi) {
        Class<? extends GridSpi> spiCls = spi.getClass();

        HashMap<String, Object> res = new HashMap<>();

        res.put("Class Name", compactClass(spi));

        for (Method mtd : spiCls.getDeclaredMethods()) {
            if (mtd.isAnnotationPresent(GridSpiConfiguration.class) && !mtd.isAnnotationPresent(Deprecated.class)) {
                String mtdName = mtd.getName();

                if (mtdName.startsWith("set")) {
                    String propName = Character.toLowerCase(mtdName.charAt(3)) + mtdName.substring(4);

                    String[] getterNames = new String[] {
                        "get" + mtdName.substring(3),
                        "is" + mtdName.substring(3),
                        "get" + mtdName.substring(3) + "Formatted"
                    };

                    try {
                        for (String getterName : getterNames) {
                            try {
                                Method getter = spiCls.getDeclaredMethod(getterName);

                                Object getRes = getter.invoke(spi);

                                res.put(propName, compactObject(getRes));

                                break;
                            }
                            catch (NoSuchMethodException ignored) {
                                // No-op.
                            }
                        }
                    }
                    catch (IllegalAccessException ignored) {
                        res.put(propName, "Error: Method Cannot Be Accessed");
                    }
                    catch (InvocationTargetException ite) {
                        res.put(propName, ("Error: Method Threw An Exception: " + ite));
                    }
                }
            }
        }

        return new GridBiTuple<String, Map<String, Object>>(spi.getName(), res);
    }

    private static GridBiTuple<String, Map<String, Object>>[] collectSpiInfo(GridSpi[] spis) {
        GridBiTuple[] res = new GridBiTuple[spis.length];

        for (int i = 0; i < spis.length; i++)
            res[i] = collectSpiInfo(spis[i]);

        return (GridBiTuple<String, Map<String, Object>>[]) res;
    }

    /**
     * @param c Grid configuration.
     * @return Data transfer object for node SPIs configuration properties.
     */
    public static VisorSpisConfig from(GridConfiguration c) {
        VisorSpisConfig cfg = new VisorSpisConfig();

        cfg.discoverySpi(collectSpiInfo(c.getDiscoverySpi()));
        cfg.communicationSpi(collectSpiInfo(c.getCommunicationSpi()));
        cfg.eventStorageSpi(collectSpiInfo(c.getEventStorageSpi()));
        cfg.collisionSpi(collectSpiInfo(c.getCollisionSpi()));
        cfg.authenticationSpi(collectSpiInfo(c.getAuthenticationSpi()));
        cfg.secureSessionSpi(collectSpiInfo(c.getSecureSessionSpi()));
        cfg.deploymentSpi(collectSpiInfo(c.getDeploymentSpi()));
        cfg.checkpointSpis(collectSpiInfo(c.getCheckpointSpi()));
        cfg.failoverSpis(collectSpiInfo(c.getFailoverSpi()));
        cfg.loadBalancingSpis(collectSpiInfo(c.getLoadBalancingSpi()));
        cfg.swapSpaceSpi(collectSpiInfo(c.getSwapSpaceSpi()));
        cfg.indexingSpis(collectSpiInfo(c.getIndexingSpi()));

        return cfg;
    }

    /**
     * @return Discovery SPI.
     */
    public GridBiTuple<String, Map<String, Object>> discoverySpi() {
        return discoSpi;
    }

    /**
     * @param discoSpi New discovery SPI.
     */
    public void discoverySpi(GridBiTuple<String, Map<String, Object>> discoSpi) {
        this.discoSpi = discoSpi;
    }

    /**
     * @return Communication SPI.
     */
    public GridBiTuple<String, Map<String, Object>> communicationSpi() {
        return commSpi;
    }

    /**
     * @param commSpi New communication SPI.
     */
    public void communicationSpi(GridBiTuple<String, Map<String, Object>> commSpi) {
        this.commSpi = commSpi;
    }

    /**
     * @return Event storage SPI.
     */
    public GridBiTuple<String, Map<String, Object>> eventStorageSpi() {
        return evtSpi;
    }

    /**
     * @param evtSpi New event storage SPI.
     */
    public void eventStorageSpi(GridBiTuple<String, Map<String, Object>> evtSpi) {
        this.evtSpi = evtSpi;
    }

    /**
     * @return Collision SPI.
     */
    public GridBiTuple<String, Map<String, Object>> collisionSpi() {
        return colSpi;
    }

    /**
     * @param colSpi New collision SPI.
     */
    public void collisionSpi(GridBiTuple<String, Map<String, Object>> colSpi) {
        this.colSpi = colSpi;
    }

    /**
     * @return Authentication SPI.
     */
    public GridBiTuple<String, Map<String, Object>> authenticationSpi() {
        return authSpi;
    }

    /**
     * @param authSpi New authentication SPI.
     */
    public void authenticationSpi(GridBiTuple<String, Map<String, Object>> authSpi) {
        this.authSpi = authSpi;
    }

    /**
     * @return Secure Session SPI.
     */
    public GridBiTuple<String, Map<String, Object>> secureSessionSpi() {
        return sesSpi;
    }

    /**
     * @param sesSpi New secure Session SPI.
     */
    public void secureSessionSpi(GridBiTuple<String, Map<String, Object>> sesSpi) {
        this.sesSpi = sesSpi;
    }

    /**
     * @return Deployment SPI.
     */
    public GridBiTuple<String, Map<String, Object>> deploymentSpi() {
        return deploySpi;
    }

    /**
     * @param deploySpi New deployment SPI.
     */
    public void deploymentSpi(GridBiTuple<String, Map<String, Object>> deploySpi) {
        this.deploySpi = deploySpi;
    }

    /**
     * @return Checkpoint SPIs.
     */
    public GridBiTuple<String, Map<String, Object>>[] checkpointSpis() {
        return cpSpis;
    }

    /**
     * @param cpSpis New checkpoint SPIs.
     */
    public void checkpointSpis(GridBiTuple<String, Map<String, Object>>[] cpSpis) {
        this.cpSpis = cpSpis;
    }

    /**
     * @return Failover SPIs.
     */
    public GridBiTuple<String, Map<String, Object>>[] failoverSpis() {
        return failSpis;
    }

    /**
     * @param failSpis New failover SPIs.
     */
    public void failoverSpis(GridBiTuple<String, Map<String, Object>>[] failSpis) {
        this.failSpis = failSpis;
    }

    /**
     * @return Load balancing SPIs.
     */
    public GridBiTuple<String, Map<String, Object>>[] loadBalancingSpis() {
        return loadBalancingSpis;
    }

    /**
     * @param loadBalancingSpis New load balancing SPIs.
     */
    public void loadBalancingSpis(GridBiTuple<String, Map<String, Object>>[] loadBalancingSpis) {
        this.loadBalancingSpis = loadBalancingSpis;
    }

    /**
     * @return Swap space SPIs.
     */
    public GridBiTuple<String, Map<String, Object>> swapSpaceSpi() {
        return swapSpaceSpis;
    }

    /**
     * @param swapSpaceSpis New swap space SPIs.
     */
    public void swapSpaceSpi(GridBiTuple<String, Map<String, Object>> swapSpaceSpis) {
        this.swapSpaceSpis = swapSpaceSpis;
    }

    /**
     * @return Indexing SPIs.
     */
    public GridBiTuple<String, Map<String, Object>>[] indexingSpis() {
        return indexingSpis;
    }

    /**
     * @param indexingSpis New indexing SPIs.
     */
    public void indexingSpis(GridBiTuple<String, Map<String, Object>>[] indexingSpis) {
        this.indexingSpis = indexingSpis;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorSpisConfig.class, this);
    }
}
