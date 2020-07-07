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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.ClientConnectorConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorDataTransferObject;

import static org.apache.ignite.internal.visor.util.VisorTaskUtils.compactArray;
import static org.apache.ignite.internal.visor.util.VisorTaskUtils.compactClass;

/**
 * Data transfer object for node configuration data.
 */
public class VisorGridConfiguration extends VisorDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** Basic. */
    private VisorBasicConfiguration basic;

    /** Metrics. */
    private VisorMetricsConfiguration metrics;

    /** SPIs. */
    private VisorSpisConfiguration spis;

    /** P2P. */
    private VisorPeerToPeerConfiguration p2p;

    /** Lifecycle. */
    private VisorLifecycleConfiguration lifecycle;

    /** Executors service configuration. */
    private VisorExecutorServiceConfiguration execSvc;

    /** Segmentation. */
    private VisorSegmentationConfiguration seg;

    /** Include properties. */
    private String inclProps;

    /** Include events types. */
    private int[] inclEvtTypes;

    /** REST configuration. */
    private VisorRestConfiguration rest;

    /** User attributes. */
    private Map<String, ?> userAttrs;

    /** IGFSs. */
    private List<VisorIgfsConfiguration> igfss;

    /** Environment. */
    private Map<String, String> env;

    /** System properties. */
    private Properties sysProps;

    /** Configuration of atomic data structures. */
    private VisorAtomicConfiguration atomic;

    /** Transactions configuration. */
    private VisorTransactionConfiguration txCfg;

    /** Memory configuration. */
    private VisorMemoryConfiguration memCfg;

    /** Persistence configuration. */
    private VisorPersistentStoreConfiguration psCfg;

    /** Cache store session listeners. */
    private String storeSesLsnrs;

    /** Warmup closure. Will be invoked before actual grid start. */
    private String warmupClos;

    /** Binary configuration. */
    private VisorBinaryConfiguration binaryCfg;

    /** List of cache key configurations. */
    private List<VisorCacheKeyConfiguration> cacheKeyCfgs;

    /** Hadoop configuration. */
    private VisorHadoopConfiguration hadoopCfg;

    /** SQL connector configuration. */
    private VisorSqlConnectorConfiguration sqlConnCfg;

    /** List of service configurations. */
    private List<VisorServiceConfiguration> srvcCfgs;

    /** Configuration of data storage. */
    private VisorDataStorageConfiguration dataStorage;

    /** Client connector configuration */
    private VisorClientConnectorConfiguration clnConnCfg;

    /** MVCC configuration. */
    private VisorMvccConfiguration mvccCfg;

    /**
     * Default constructor.
     */
    public VisorGridConfiguration() {
        // No-op.
    }

    /**
     * Create data transfer object with node configuration data.
     *
     * @param ignite Grid.
     */
    public VisorGridConfiguration(IgniteEx ignite) {
        assert ignite != null;

        IgniteConfiguration c = ignite.configuration();

        basic = new VisorBasicConfiguration(ignite, c);
        metrics = new VisorMetricsConfiguration(c);
        spis = new VisorSpisConfiguration(c);
        p2p = new VisorPeerToPeerConfiguration(c);
        lifecycle = new VisorLifecycleConfiguration(c);
        execSvc = new VisorExecutorServiceConfiguration(c);
        seg = new VisorSegmentationConfiguration(c);
        inclProps = compactArray(c.getIncludeProperties());
        inclEvtTypes = c.getIncludeEventTypes();
        rest = new VisorRestConfiguration(c);
        userAttrs = c.getUserAttributes();
        env = new HashMap<>(System.getenv());
        sysProps = IgniteSystemProperties.snapshot();
        atomic = new VisorAtomicConfiguration(c.getAtomicConfiguration());
        txCfg = new VisorTransactionConfiguration(c.getTransactionConfiguration());

        if (c.getDataStorageConfiguration() != null)
            memCfg = null;

        if (c.getDataStorageConfiguration() != null)
            psCfg = null;

        storeSesLsnrs = compactArray(c.getCacheStoreSessionListenerFactories());
        warmupClos = compactClass(c.getWarmupClosure());

        BinaryConfiguration bc = c.getBinaryConfiguration();

        if (bc != null)
            binaryCfg = new VisorBinaryConfiguration(bc);

        cacheKeyCfgs = VisorCacheKeyConfiguration.list(c.getCacheKeyConfiguration());

        ClientConnectorConfiguration ccc = c.getClientConnectorConfiguration();

        if (ccc != null)
            clnConnCfg = new VisorClientConnectorConfiguration(ccc);

        srvcCfgs = VisorServiceConfiguration.list(c.getServiceConfiguration());

        DataStorageConfiguration dsCfg = c.getDataStorageConfiguration();

        if (dsCfg != null)
            dataStorage = new VisorDataStorageConfiguration(dsCfg);

        mvccCfg = new VisorMvccConfiguration(c);
    }

    /**
     * @return Basic.
     */
    public VisorBasicConfiguration getBasic() {
        return basic;
    }

    /**
     * @return Metrics.
     */
    public VisorMetricsConfiguration getMetrics() {
        return metrics;
    }

    /**
     * @return SPIs.
     */
    public VisorSpisConfiguration getSpis() {
        return spis;
    }

    /**
     * @return P2P.
     */
    public VisorPeerToPeerConfiguration getP2p() {
        return p2p;
    }

    /**
     * @return Lifecycle.
     */
    public VisorLifecycleConfiguration getLifecycle() {
        return lifecycle;
    }

    /**
     * @return Executors service configuration.
     */
    public VisorExecutorServiceConfiguration getExecutorService() {
        return execSvc;
    }

    /**
     * @return Segmentation.
     */
    public VisorSegmentationConfiguration getSegmentation() {
        return seg;
    }

    /**
     * @return Include properties.
     */
    public String getIncludeProperties() {
        return inclProps;
    }

    /**
     * @return Include events types.
     */
    public int[] getIncludeEventTypes() {
        return inclEvtTypes;
    }

    /**
     * @return Rest.
     */
    public VisorRestConfiguration getRest() {
        return rest;
    }

    /**
     * @return User attributes.
     */
    public Map<String, ?> getUserAttributes() {
        return userAttrs;
    }

    /**
     * @return Igfss.
     */
    public List<VisorIgfsConfiguration> getIgfss() {
        return igfss;
    }

    /**
     * @return Environment.
     */
    public Map<String, String> getEnv() {
        return env;
    }

    /**
     * @return System properties.
     */
    public Properties getSystemProperties() {
        return sysProps;
    }

    /**
     * @return Configuration of atomic data structures.
     */
    public VisorAtomicConfiguration getAtomic() {
        return atomic;
    }

    /**
     * @return Transactions configuration.
     */
    public VisorTransactionConfiguration getTransaction() {
        return txCfg;
    }

    /**
     * @return Memory configuration.
     */
    public VisorMemoryConfiguration getMemoryConfiguration() {
        return memCfg;
    }

    /**
     * @return Persistent store configuration.
     */
    public VisorPersistentStoreConfiguration getPersistentStoreConfiguration() {
        return psCfg;
    }

    /**
     * @return Cache store session listener factories.
     */
    public String getCacheStoreSessionListenerFactories() {
        return storeSesLsnrs;
    }

    /**
     * @return Warmup closure to execute.
     */
    public String getWarmupClosure() {
        return warmupClos;
    }

    /**
     * @return Binary configuration.
     */
    public VisorBinaryConfiguration getBinaryConfiguration() {
        return binaryCfg;
    }

    /**
     * @return List of cache key configurations.
     */
    public List<VisorCacheKeyConfiguration> getCacheKeyConfigurations() {
        return cacheKeyCfgs;
    }

    /**
     * @return Hadoop configuration.
     */
    public VisorHadoopConfiguration getHadoopConfiguration() {
        return hadoopCfg;
    }

    /**
     * @return SQL connector configuration.
     */
    public VisorSqlConnectorConfiguration getSqlConnectorConfiguration() {
        return sqlConnCfg;
    }

    /**
     * @return Client connector configuration.
     */
    public VisorClientConnectorConfiguration getClientConnectorConfiguration() {
        return clnConnCfg;
    }

    /**
     * @return List of service configurations
     */
    public List<VisorServiceConfiguration> getServiceConfigurations() {
        return srvcCfgs;
    }

    /**
     * @return Configuration of data storage.
     */
    public VisorDataStorageConfiguration getDataStorageConfiguration() {
        return dataStorage;
    }

    /**
     * @return MVCC configuration.
     */
    public VisorMvccConfiguration getMvccConfiguration() {
        return mvccCfg;
    }

    /** {@inheritDoc} */
    @Override public byte getProtocolVersion() {
        return V4;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        out.writeObject(basic);
        out.writeObject(metrics);
        out.writeObject(spis);
        out.writeObject(p2p);
        out.writeObject(lifecycle);
        out.writeObject(execSvc);
        out.writeObject(seg);
        U.writeString(out, inclProps);
        out.writeObject(inclEvtTypes);
        out.writeObject(rest);
        U.writeMap(out, userAttrs);
        U.writeCollection(out, null);
        U.writeMap(out, env);
        out.writeObject(sysProps);
        out.writeObject(atomic);
        out.writeObject(txCfg);
        out.writeObject(memCfg);
        out.writeObject(psCfg);
        U.writeString(out, storeSesLsnrs);
        U.writeString(out, warmupClos);
        out.writeObject(binaryCfg);
        U.writeCollection(out, cacheKeyCfgs);
        out.writeObject(null);
        out.writeObject(sqlConnCfg);
        U.writeCollection(out, srvcCfgs);
        out.writeObject(dataStorage);
        out.writeObject(clnConnCfg);
        out.writeObject(mvccCfg);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        basic = (VisorBasicConfiguration)in.readObject();
        metrics = (VisorMetricsConfiguration)in.readObject();
        spis = (VisorSpisConfiguration)in.readObject();
        p2p = (VisorPeerToPeerConfiguration)in.readObject();
        lifecycle = (VisorLifecycleConfiguration)in.readObject();
        execSvc = (VisorExecutorServiceConfiguration)in.readObject();
        seg = (VisorSegmentationConfiguration)in.readObject();
        inclProps = U.readString(in);
        inclEvtTypes = (int[])in.readObject();
        rest = (VisorRestConfiguration)in.readObject();
        userAttrs = U.readMap(in);
        igfss = U.readList(in);
        env = U.readMap(in);
        sysProps = (Properties)in.readObject();
        atomic = (VisorAtomicConfiguration)in.readObject();
        txCfg = (VisorTransactionConfiguration)in.readObject();
        memCfg = (VisorMemoryConfiguration)in.readObject();
        psCfg = (VisorPersistentStoreConfiguration)in.readObject();
        storeSesLsnrs = U.readString(in);
        warmupClos = U.readString(in);
        binaryCfg = (VisorBinaryConfiguration)in.readObject();
        cacheKeyCfgs = U.readList(in);
        hadoopCfg = (VisorHadoopConfiguration)in.readObject();
        sqlConnCfg = (VisorSqlConnectorConfiguration) in.readObject();
        srvcCfgs = U.readList(in);

        if (protoVer > V1)
            dataStorage = (VisorDataStorageConfiguration)in.readObject();

        if (protoVer > V2)
            clnConnCfg = (VisorClientConnectorConfiguration)in.readObject();

        if (protoVer > V3)
            mvccCfg = (VisorMvccConfiguration)in.readObject();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorGridConfiguration.class, this);
    }
}
