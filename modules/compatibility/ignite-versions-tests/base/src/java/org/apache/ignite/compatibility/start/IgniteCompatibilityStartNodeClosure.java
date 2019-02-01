/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.compatibility.start;

import java.util.Arrays;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.binary.BinaryBasicIdMapper;
import org.apache.ignite.binary.BinaryBasicNameMapper;
import org.apache.ignite.compatibility.Person;
import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.MemoryConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.configuration.PersistentStoreConfiguration;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.lang.IgniteOutClosure;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheRebalanceMode.SYNC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * Closure for preconfiguring Ignite and create an instance of it.
 */
public class IgniteCompatibilityStartNodeClosure implements IgniteOutClosure<Ignite> {
    /** */
    public static final String PARTITIONED_CACHE_NAME = "partitioned";

    /** */
    public static final String PARTITIONED_TX_CACHE_NAME = "partitioned_tx";

    /** */
    public static final String USE_SIMPLE_NAME_MAPPER_PROP = "gridgain.compatibility.use.binary.simple.name.mapper";

    /** Property for enforcing 2k page size. */
    public static final String USE_2K_PAGE_SIZE = "compatibility.tests.use.2k.page.size";

    /** */
    public static final String[] TEST_PROPS = {
        USE_SIMPLE_NAME_MAPPER_PROP,
        USE_2K_PAGE_SIZE
    };

    /** Is client mode. */
    private boolean clientMode;

    /** Node name. */
    private String nodeName;

    /** Working directory. */
    private String workDir;

    /** Enable storage. */
    protected boolean enableStorage;

    /** Configure closure. */
    private IgniteOutClosure<IgniteConfiguration> configureClosure;

    /**
     * Sets client mode flag.
     *
     * @param clientMode Client mode flag.
     * @return This for chaining.
     */
    public IgniteCompatibilityStartNodeClosure clientMode(boolean clientMode) {
        this.clientMode = clientMode;

        return this;
    }

    /**
     * @return Client mode flag.
     */
    public boolean clientMode() {
        return clientMode;
    }

    /**
     * Sets node name.
     *
     * @param nodeName Node name.
     * @return This for chaining.
     */
    public IgniteCompatibilityStartNodeClosure nodeName(String nodeName) {
        this.nodeName = nodeName;

        return this;
    }

    /**
     * @return node name.
     */
    public String getNodeName() {
        return nodeName;
    }

    /**
     * Sets working directory.
     *
     * @param workDir Node work directory.
     */
    public void workDirectory(String workDir) {
        this.workDir = workDir;
    }

    /**
     * @return Node work directory.
     */
    public String workDirectory() {
        return  workDir;
    }

    /**
     * Sets enable storage flag value.
     *
     * @param enableStorage Flag to enable disk storage.
     * @return This closure for chaining.
     */
    public IgniteCompatibilityStartNodeClosure enableStorage(boolean enableStorage) {
        this.enableStorage = enableStorage;

        return this;
    }

    /**
     * Sets preconfiguration parameters.
     *
     * @param configureClojure Configure clojure.
     * @return This closure for chaining.
     */
    public IgniteCompatibilityStartNodeClosure preConfigure(IgniteOutClosure<IgniteConfiguration> configureClojure) {
        this.configureClosure = configureClojure;

        return this;
    }

    /**
     * @return Ignite instance.
     */
    @Override public final Ignite apply() {
        IgniteConfiguration cfg;

        if (configureClosure != null)
            cfg = configureClosure.apply();
        else
            cfg = new IgniteConfiguration();

        setDefaults(cfg);

        return Ignition.start(cfg);
    }

    /**
     * Sets default values for node configuration.
     *
     * @param cfg Ignite configuration.
     */
    protected void setDefaults(IgniteConfiguration cfg) {
        TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder();

        ipFinder.setAddresses(Arrays.asList("127.0.0.1:27500..27530"));

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();
        discoSpi.setLocalPort(27500);
        discoSpi.setLocalPortRange(30);

        discoSpi.setIpFinder(ipFinder);

        cfg.setClientMode(clientMode);

        if (nodeName != null)
            cfg.setIgniteInstanceName(nodeName);

        cfg.setLocalHost("127.0.0.1");
        cfg.setPeerClassLoadingEnabled(true);
        cfg.setIncludeEventTypes(EventType.EVTS_TASK_EXECUTION);
        cfg.setDiscoverySpi(discoSpi);

        MemoryConfiguration memCfg = cfg.getMemoryConfiguration();

        if (memCfg == null) {
            memCfg = new MemoryConfiguration();
        }

        memCfg.setDefaultMemoryPolicySize(512L * 1024 * 1024);

        if (Boolean.getBoolean(USE_2K_PAGE_SIZE))
            memCfg.setPageSize(2048);

        cfg.setMemoryConfiguration(memCfg);

        TcpCommunicationSpi commSpi = new TcpCommunicationSpi();
        commSpi.setSharedMemoryPort(-1);

        cfg.setCommunicationSpi(commSpi);

        boolean useSimpleNameMapper = Boolean.valueOf(System.getProperty(USE_SIMPLE_NAME_MAPPER_PROP));

        cfg.setMarshaller(new BinaryMarshaller());

        if (useSimpleNameMapper) {
            BinaryConfiguration bcfg = new BinaryConfiguration();

            bcfg.setNameMapper(new BinaryBasicNameMapper(true));
            bcfg.setIdMapper(new BinaryBasicIdMapper(true));

            cfg.setBinaryConfiguration(bcfg);
        }

        cfg.setMetricsLogFrequency(0);

        if (workDir != null)
            cfg.setWorkDirectory(workDir);

        if (enableStorage)
            cfg.setPersistentStoreConfiguration(new PersistentStoreConfiguration());

        System.out.println("Configured node [" +
            "useSimpleNameMapper=" + useSimpleNameMapper +
            ", storageEnabled=" + (cfg.getPersistentStoreConfiguration() != null) + "]");
        System.out.println("Config [" + cfg + "]");
    }

    /**
     * @return Cache config.
     */
    public static CacheConfiguration partitionedCacheConfiguration() {
        return partitionedCacheConfiguration(true);
    }

    /**
     * @param idx If {@code true} indexing is enabled.
     * @return Cache config.
     */
    @SuppressWarnings("unchecked")
    public static CacheConfiguration partitionedCacheConfiguration(boolean idx) {
        CacheConfiguration ccfg = new CacheConfiguration();

        ccfg.setName(PARTITIONED_CACHE_NAME);
        ccfg.setCacheMode(PARTITIONED);
        ccfg.setAtomicityMode(ATOMIC);
        ccfg.setNearConfiguration(null);
        ccfg.setBackups(1);
        ccfg.setRebalanceMode(SYNC);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);

        if (idx)
            ccfg.setIndexedTypes(Integer.class, Person.class);

        return ccfg;
    }

    /**
     * @return Tx cache config.
     */
    @SuppressWarnings("unchecked")
    public static CacheConfiguration partitionedTxCacheConfiguration() {
        CacheConfiguration ccfg = new CacheConfiguration(partitionedCacheConfiguration());

        ccfg.setName(PARTITIONED_TX_CACHE_NAME);
        ccfg.setAtomicityMode(TRANSACTIONAL);
        ccfg.setNearConfiguration(new NearCacheConfiguration());

        return ccfg;
    }
}
