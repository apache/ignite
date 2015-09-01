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

package org.apache.ignite.internal.visor.node;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.spi.IgniteSpi;
import org.apache.ignite.spi.IgniteSpiConfiguration;

import static org.apache.ignite.internal.visor.util.VisorTaskUtils.compactClass;
import static org.apache.ignite.internal.visor.util.VisorTaskUtils.compactObject;

/**
 * Data transfer object for node SPIs configuration properties.
 */
public class VisorSpisConfiguration implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Discovery SPI. */
    private IgniteBiTuple<String, Map<String, Object>> discoSpi;

    /** Communication SPI. */
    private IgniteBiTuple<String, Map<String, Object>> commSpi;

    /** Event storage SPI. */
    private IgniteBiTuple<String, Map<String, Object>> evtSpi;

    /** Collision SPI. */
    private IgniteBiTuple<String, Map<String, Object>> colSpi;

    /** Deployment SPI. */
    private IgniteBiTuple<String, Map<String, Object>> deploySpi;

    /** Checkpoint SPIs. */
    private IgniteBiTuple<String, Map<String, Object>>[] cpSpis;

    /** Failover SPIs. */
    private IgniteBiTuple<String, Map<String, Object>>[] failSpis;

    /** Load balancing SPIs. */
    private IgniteBiTuple<String, Map<String, Object>>[] loadBalancingSpis;

    /** Swap space SPIs. */
    private IgniteBiTuple<String, Map<String, Object>> swapSpaceSpis;

    /** Indexing SPIs. */
    private IgniteBiTuple<String, Map<String, Object>>[] indexingSpis;

    /**
     * Collects SPI information based on GridSpiConfiguration-annotated methods.
     * Methods with {@code Deprecated} annotation are skipped.
     *
     * @param spi SPI to collect information on.
     * @return Tuple where first component is SPI name and map with properties as second.
     */
    private static IgniteBiTuple<String, Map<String, Object>> collectSpiInfo(IgniteSpi spi) {
        Class<? extends IgniteSpi> spiCls = spi.getClass();

        HashMap<String, Object> res = new HashMap<>();

        res.put("Class Name", compactClass(spi));

        for (Method mtd : spiCls.getDeclaredMethods()) {
            if (mtd.isAnnotationPresent(IgniteSpiConfiguration.class) && !mtd.isAnnotationPresent(Deprecated.class)) {
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

        return new IgniteBiTuple<String, Map<String, Object>>(spi.getName(), res);
    }

    /**
     * @param spis Array of spi to process.
     * @return Tuple where first component is SPI name and map with properties as second.
     */
    private static IgniteBiTuple<String, Map<String, Object>>[] collectSpiInfo(IgniteSpi[] spis) {
        IgniteBiTuple[] res = new IgniteBiTuple[spis.length];

        for (int i = 0; i < spis.length; i++)
            res[i] = collectSpiInfo(spis[i]);

        return (IgniteBiTuple<String, Map<String, Object>>[])res;
    }

    /**
     * @param c Grid configuration.
     * @return Data transfer object for node SPIs configuration properties.
     */
    public static VisorSpisConfiguration from(IgniteConfiguration c) {
        VisorSpisConfiguration cfg = new VisorSpisConfiguration();

        cfg.discoSpi = collectSpiInfo(c.getDiscoverySpi());
        cfg.commSpi = collectSpiInfo(c.getCommunicationSpi());
        cfg.evtSpi = collectSpiInfo(c.getEventStorageSpi());
        cfg.colSpi = collectSpiInfo(c.getCollisionSpi());
        cfg.deploySpi = collectSpiInfo(c.getDeploymentSpi());
        cfg.cpSpis = collectSpiInfo(c.getCheckpointSpi());
        cfg.failSpis = collectSpiInfo(c.getFailoverSpi());
        cfg.loadBalancingSpis = collectSpiInfo(c.getLoadBalancingSpi());
        cfg.swapSpaceSpis = collectSpiInfo(c.getSwapSpaceSpi());
        cfg.indexingSpis = F.asArray(collectSpiInfo(c.getIndexingSpi()));

        return cfg;
    }

    /**
     * @return Discovery SPI.
     */
    public IgniteBiTuple<String, Map<String, Object>> discoverySpi() {
        return discoSpi;
    }

    /**
     * @return Communication SPI.
     */
    public IgniteBiTuple<String, Map<String, Object>> communicationSpi() {
        return commSpi;
    }

    /**
     * @return Event storage SPI.
     */
    public IgniteBiTuple<String, Map<String, Object>> eventStorageSpi() {
        return evtSpi;
    }

    /**
     * @return Collision SPI.
     */
    public IgniteBiTuple<String, Map<String, Object>> collisionSpi() {
        return colSpi;
    }

    /**
     * @return Deployment SPI.
     */
    public IgniteBiTuple<String, Map<String, Object>> deploymentSpi() {
        return deploySpi;
    }

    /**
     * @return Checkpoint SPIs.
     */
    public IgniteBiTuple<String, Map<String, Object>>[] checkpointSpis() {
        return cpSpis;
    }

    /**
     * @return Failover SPIs.
     */
    public IgniteBiTuple<String, Map<String, Object>>[] failoverSpis() {
        return failSpis;
    }

    /**
     * @return Load balancing SPIs.
     */
    public IgniteBiTuple<String, Map<String, Object>>[] loadBalancingSpis() {
        return loadBalancingSpis;
    }

    /**
     * @return Swap space SPIs.
     */
    public IgniteBiTuple<String, Map<String, Object>> swapSpaceSpi() {
        return swapSpaceSpis;
    }

    /**
     * @return Indexing SPIs.
     */
    public IgniteBiTuple<String, Map<String, Object>>[] indexingSpis() {
        return indexingSpis;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorSpisConfiguration.class, this);
    }
}