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
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.internal.S;

import static org.apache.ignite.internal.visor.util.VisorTaskUtils.compactArray;

/**
 * Data transfer object for node configuration data.
 */
public class VisorGridConfiguration implements Serializable {
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

    /** Igfss. */
    private Iterable<VisorIgfsConfiguration> igfss;

    /** Environment. */
    private Map<String, String> env;

    /** System properties. */
    private Properties sysProps;

    /** Configuration of atomic data structures. */
    private VisorAtomicConfiguration atomic;

    /** Transactions configuration. */
    private VisorTransactionConfiguration txCfg;

    /**
     * @param ignite Grid.
     * @return Fill data transfer object with node configuration data.
     */
    public VisorGridConfiguration from(IgniteEx ignite) {
        assert ignite != null;

        IgniteConfiguration c = ignite.configuration();

        basic = VisorBasicConfiguration.from(ignite, c);
        metrics = VisorMetricsConfiguration.from(c);
        spis = VisorSpisConfiguration.from(c);
        p2p = VisorPeerToPeerConfiguration.from(c);
        lifecycle = VisorLifecycleConfiguration.from(c);
        execSvc = VisorExecutorServiceConfiguration.from(c);
        seg = VisorSegmentationConfiguration.from(c);
        inclProps = compactArray(c.getIncludeProperties());
        inclEvtTypes = c.getIncludeEventTypes();
        rest = VisorRestConfiguration.from(c);
        userAttrs = c.getUserAttributes();
        igfss = VisorIgfsConfiguration.list(c.getFileSystemConfiguration());
        env = new HashMap<>(System.getenv());
        sysProps = IgniteSystemProperties.snapshot();
        atomic = VisorAtomicConfiguration.from(c.getAtomicConfiguration());
        txCfg = VisorTransactionConfiguration.from(c.getTransactionConfiguration());

        return this;
    }

    /**
     * @return Basic.
     */
    public VisorBasicConfiguration basic() {
        return basic;
    }

    /**
     * @return Metrics.
     */
    public VisorMetricsConfiguration metrics() {
        return metrics;
    }

    /**
     * @return SPIs.
     */
    public VisorSpisConfiguration spis() {
        return spis;
    }

    /**
     * @return P2P.
     */
    public VisorPeerToPeerConfiguration p2p() {
        return p2p;
    }

    /**
     * @return Lifecycle.
     */
    public VisorLifecycleConfiguration lifecycle() {
        return lifecycle;
    }

    /**
     * @return Executors service configuration.
     */
    public VisorExecutorServiceConfiguration executeService() {
        return execSvc;
    }

    /**
     * @return Segmentation.
     */
    public VisorSegmentationConfiguration segmentation() {
        return seg;
    }

    /**
     * @return Include properties.
     */
    public String includeProperties() {
        return inclProps;
    }

    /**
     * @return Include events types.
     */
    public int[] includeEventTypes() {
        return inclEvtTypes;
    }

    /**
     * @return Rest.
     */
    public VisorRestConfiguration rest() {
        return rest;
    }

    /**
     * @return User attributes.
     */
    public Map<String, ?> userAttributes() {
        return userAttrs;
    }

    /**
     * @return Igfss.
     */
    public Iterable<VisorIgfsConfiguration> igfss() {
        return igfss;
    }

    /**
     * @return Environment.
     */
    public Map<String, String> env() {
        return env;
    }

    /**
     * @return System properties.
     */
    public Properties systemProperties() {
        return sysProps;
    }

    /**
     * @return Configuration of atomic data structures.
     */
    public VisorAtomicConfiguration atomic() {
        return atomic;
    }

    /**
     * @return Transactions configuration.
     */
    public VisorTransactionConfiguration transaction() {
        return txCfg;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorGridConfiguration.class, this);
    }
}