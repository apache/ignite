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

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.visor.VisorDataTransferObject;
import org.apache.ignite.spi.IgniteSpi;
import org.apache.ignite.spi.IgniteSpiConfiguration;

import static org.apache.ignite.internal.visor.util.VisorTaskUtils.compactClass;
import static org.apache.ignite.internal.visor.util.VisorTaskUtils.compactObject;

/**
 * Data transfer object for node SPIs configuration properties.
 */
public class VisorSpisConfiguration extends VisorDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** Discovery SPI. */
    private VisorSpiDescription discoSpi;

    /** Communication SPI. */
    private VisorSpiDescription commSpi;

    /** Event storage SPI. */
    private VisorSpiDescription evtSpi;

    /** Collision SPI. */
    private VisorSpiDescription colSpi;

    /** Deployment SPI. */
    private VisorSpiDescription deploySpi;

    /** Checkpoint SPIs. */
    private VisorSpiDescription[] cpSpis;

    /** Failover SPIs. */
    private VisorSpiDescription[] failSpis;

    /** Load balancing SPIs. */
    private VisorSpiDescription[] loadBalancingSpis;

    /** Indexing SPIs. */
    private VisorSpiDescription[] indexingSpis;

    /**
     * Default constructor.
     */
    public VisorSpisConfiguration() {
        // No-op.
    }

    /**
     * Collects SPI information based on GridSpiConfiguration-annotated methods.
     * Methods with {@code Deprecated} annotation are skipped.
     *
     * @param spi SPI to collect information on.
     * @return Tuple where first component is SPI name and map with properties as second.
     */
    private static VisorSpiDescription collectSpiInfo(IgniteSpi spi) {
        Class<? extends IgniteSpi> spiCls = spi.getClass();

        HashMap<String, Object> res = new HashMap<>();

        res.put("Class Name", compactClass(spi));

        for (Method mtd : spiCls.getDeclaredMethods()) {
            if (mtd.isAnnotationPresent(IgniteSpiConfiguration.class) && !mtd.isAnnotationPresent(Deprecated.class)) {
                String mtdName = mtd.getName();

                if (mtdName.startsWith("set")) {
                    String propName = Character.toLowerCase(mtdName.charAt(3)) + mtdName.substring(4);

                    try {
                        String[] getterNames = new String[] {
                            "get" + mtdName.substring(3),
                            "is" + mtdName.substring(3),
                            "get" + mtdName.substring(3) + "Formatted"
                        };

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

        return new VisorSpiDescription(spi.getName(), res);
    }

    /**
     * @param spis Array of spi to process.
     * @return Tuple where first component is SPI name and map with properties as second.
     */
    private static VisorSpiDescription[] collectSpiInfo(IgniteSpi[] spis) {
        VisorSpiDescription[] res = new VisorSpiDescription[spis.length];

        for (int i = 0; i < spis.length; i++)
            res[i] = collectSpiInfo(spis[i]);

        return res;
    }

    /**
     * Create data transfer object for node SPIs configuration properties.
     *
     * @param c Grid configuration.
     */
    public VisorSpisConfiguration(IgniteConfiguration c) {
        discoSpi = collectSpiInfo(c.getDiscoverySpi());
        commSpi = collectSpiInfo(c.getCommunicationSpi());
        evtSpi = collectSpiInfo(c.getEventStorageSpi());
        colSpi = collectSpiInfo(c.getCollisionSpi());
        deploySpi = collectSpiInfo(c.getDeploymentSpi());
        cpSpis = collectSpiInfo(c.getCheckpointSpi());
        failSpis = collectSpiInfo(c.getFailoverSpi());
        loadBalancingSpis = collectSpiInfo(c.getLoadBalancingSpi());
        indexingSpis = F.asArray(collectSpiInfo(c.getIndexingSpi()));
    }

    /**
     * @return Discovery SPI.
     */
    public VisorSpiDescription getDiscoverySpi() {
        return discoSpi;
    }

    /**
     * @return Communication SPI.
     */
    public VisorSpiDescription getCommunicationSpi() {
        return commSpi;
    }

    /**
     * @return Event storage SPI.
     */
    public VisorSpiDescription getEventStorageSpi() {
        return evtSpi;
    }

    /**
     * @return Collision SPI.
     */
    public VisorSpiDescription getCollisionSpi() {
        return colSpi;
    }

    /**
     * @return Deployment SPI.
     */
    public VisorSpiDescription getDeploymentSpi() {
        return deploySpi;
    }

    /**
     * @return Checkpoint SPIs.
     */
    public VisorSpiDescription[] getCheckpointSpis() {
        return cpSpis;
    }

    /**
     * @return Failover SPIs.
     */
    public VisorSpiDescription[] getFailoverSpis() {
        return failSpis;
    }

    /**
     * @return Load balancing SPIs.
     */
    public VisorSpiDescription[] getLoadBalancingSpis() {
        return loadBalancingSpis;
    }

    /**
     * @return Indexing SPIs.
     */
    public VisorSpiDescription[] getIndexingSpis() {
        return indexingSpis;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        out.writeObject(discoSpi);
        out.writeObject(commSpi);
        out.writeObject(evtSpi);
        out.writeObject(colSpi);
        out.writeObject(deploySpi);
        out.writeObject(cpSpis);
        out.writeObject(failSpis);
        out.writeObject(loadBalancingSpis);
        out.writeObject(indexingSpis);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        discoSpi = (VisorSpiDescription)in.readObject();
        commSpi = (VisorSpiDescription)in.readObject();
        evtSpi = (VisorSpiDescription)in.readObject();
        colSpi = (VisorSpiDescription)in.readObject();
        deploySpi = (VisorSpiDescription)in.readObject();
        cpSpis = (VisorSpiDescription[])in.readObject();
        failSpis = (VisorSpiDescription[])in.readObject();
        loadBalancingSpis = (VisorSpiDescription[])in.readObject();
        indexingSpis = (VisorSpiDescription[])in.readObject();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorSpisConfiguration.class, this);
    }
}
