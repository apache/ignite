/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor.cmd.dto.node;

import org.gridgain.grid.product.*;

import java.io.*;
import java.util.*;

/**
 /**
 * Node configuration data.
 */
public class VisorNodeConfig implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** License. */
    private final GridProductLicense license;

    /** Basic. */
    private final VisorNodeBasicConfig basic;

    /** Metrics. */
    private final VisorNodeMetricsConfig metrics;

    /** SPIs. */
    private final VisorNodeSpisConfig spis;

    /** P2P. */
    private final VisorNodePeerToPeerConfig p2p;

    /** Email. */
    private final VisorNodeEmailConfig email;

    /** Lifecycle. */
    private final VisorNodeLifecycleConfig lifecycle;

    /** Executors service configuration. */
    private final VisorNodeExecServiceConfig execSvc;

    /** Segmentation. */
    private final VisorNodeSegmentationConfig seg;

    /** Include properties. */
    private final String inclProps;

    /** Include events types. */
    private final int[] inclEvtTypes;

    private final VisorNodeRestConfig rest;

    /** User attributes. */
    private final Map<String, ?> userAttrs;

    /** Caches. */
    private final Iterable<VisorNodeCacheConfig> caches;

    /** Ggfss. */
    private final Iterable<VisorNodeGgfsConfig> ggfss;

    /** Streamers. */
    private final Iterable<VisorStreamerConfig> streamers;

//    /** Sender hub configuration */
//    private final VisorNodeCacheDrSenderConfig drSenderHub;
//
//    /** Receiver hub configuration */
//    private final VisorNodeCacheDrReceiverConfig drReceiverHub;

    /** Environment. */
    private final Map<String, String> env;

    /** System properties. */
    private final Properties sysProps;

    public VisorNodeConfig(GridProductLicense license, VisorNodeBasicConfig basic, VisorNodeMetricsConfig metrics,
        VisorNodeSpisConfig spis, VisorNodePeerToPeerConfig p2p, VisorNodeEmailConfig email,
        VisorNodeLifecycleConfig lifecycle,
        VisorNodeExecServiceConfig execSvc, VisorNodeSegmentationConfig seg, String inclProps, int[] inclEvtTypes,
        VisorNodeRestConfig rest, Map<String, ?> userAttrs, Iterable<VisorNodeCacheConfig> caches,
        Iterable<VisorNodeGgfsConfig> ggfss,
        Iterable<VisorStreamerConfig> streamers,
//        VisorNodeCacheDrSenderConfig drSenderHub, VisorNodeCacheDrReceiverConfig drReceiverHub,
        Map<String, String> env,
        Properties sysProps) {
        this.license = license;
        this.basic = basic;
        this.metrics = metrics;
        this.spis = spis;
        this.p2p = p2p;
        this.email = email;
        this.lifecycle = lifecycle;
        this.execSvc = execSvc;
        this.seg = seg;
        this.inclProps = inclProps;
        this.inclEvtTypes = inclEvtTypes;
        this.rest = rest;
        this.userAttrs = userAttrs;
        this.caches = caches;
        this.ggfss = ggfss;
        this.streamers = streamers;
//        this.drSenderHub = drSenderHub;
//        this.drReceiverHub = drReceiverHub;
        this.env = env;
        this.sysProps = sysProps;
    }

    /**
     * @return License.
     */
    public GridProductLicense license() {
        return license;
    }

    /**
     * @return Basic.
     */
    public VisorNodeBasicConfig basic() {
        return basic;
    }

    /**
     * @return Metric.
     */
    public VisorNodeMetricsConfig metrics() {
        return metrics;
    }

    /**
     * @return Spis.
     */
    public VisorNodeSpisConfig spis() {
        return spis;
    }

    /**
     * @return P2P.
     */
    public VisorNodePeerToPeerConfig p2p() {
        return p2p;
    }

    /**
     * @return Email.
     */
    public VisorNodeEmailConfig email() {
        return email;
    }

    /**
     * @return Lifecycle.
     */
    public VisorNodeLifecycleConfig lifecycle() {
        return lifecycle;
    }

    /**
     * @return Executors.
     */
    public VisorNodeExecServiceConfig executorService() {
        return execSvc;
    }

    /**
     * @return Segmentation.
     */
    public VisorNodeSegmentationConfig seg() {
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
    public int[] inclEventTypes() {
        return inclEvtTypes;
    }

    /**
     * @return Rest.
     */
    public VisorNodeRestConfig rest() {
        return rest;
    }

    /**
     * @return User attributes.
     */
    public Map<String, ?> userAttributes() {
        return userAttrs;
    }

    /**
     * @return Caches.
     */
    public Iterable<VisorNodeCacheConfig> caches() {
        return caches;
    }

    /**
     * @return Ggfss.
     */
    public Iterable<VisorNodeGgfsConfig> ggfss() {
        return ggfss;
    }

    /**
     * @return Streamers.
     */
    public Iterable<VisorStreamerConfig> streamers() {
        return streamers;
    }

    /**
     * @return Sender hub configuration
     */
//    public VisorNodeCacheDrSenderConfig drSenderHub() {
//        return drSenderHub;
//    }

    /**
     * @return Receiver hub configuration
     */
//    public VisorNodeCacheDrReceiverConfig drReceiverHub() {
//        return drReceiverHub;
//    }

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
}
