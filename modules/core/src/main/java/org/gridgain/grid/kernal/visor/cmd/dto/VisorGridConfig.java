/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor.cmd.dto;

import org.gridgain.grid.kernal.visor.cmd.dto.cache.*;
import org.gridgain.grid.kernal.visor.cmd.dto.node.*;
import org.gridgain.grid.product.*;

import java.io.*;
import java.util.*;

/**
 /**
 * Node configuration data.
 */
public class VisorGridConfig implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** License. */
    private final GridProductLicense license;

    /** Basic. */
    private final VisorBasicConfig basic;

    /** Metrics. */
    private final VisorMetricsConfig metrics;

    /** SPIs. */
    private final VisorSpisConfig spis;

    /** P2P. */
    private final VisorPeerToPeerConfig p2p;

    /** Email. */
    private final VisorEmailConfig email;

    /** Lifecycle. */
    private final VisorLifecycleConfig lifecycle;

    /** Executors service configuration. */
    private final VisorExecServiceConfig execSvc;

    /** Segmentation. */
    private final VisorSegmentationConfig seg;

    /** Include properties. */
    private final String inclProps;

    /** Include events types. */
    private final int[] inclEvtTypes;

    private final VisorRestConfig rest;

    /** User attributes. */
    private final Map<String, ?> userAttrs;

    /** Caches. */
    private final Iterable<VisorCacheConfig> caches;

    /** Ggfss. */
    private final Iterable<VisorGgfsConfig> ggfss;

    /** Streamers. */
    private final Iterable<VisorStreamerConfig> streamers;

    /** Sender hub configuration */
    private final VisorDrSenderHubConfig drSenderHub;

    /** Receiver hub configuration */
    private final VisorDrReceiverHubConfig drReceiverHub;

    /** Environment. */
    private final Map<String, String> env;

    /** System properties. */
    private final Properties sysProps;

    public VisorGridConfig(GridProductLicense license, VisorBasicConfig basic, VisorMetricsConfig metrics,
        VisorSpisConfig spis, VisorPeerToPeerConfig p2p, VisorEmailConfig email,
        VisorLifecycleConfig lifecycle,
        VisorExecServiceConfig execSvc, VisorSegmentationConfig seg, String inclProps, int[] inclEvtTypes,
        VisorRestConfig rest, Map<String, ?> userAttrs, Iterable<VisorCacheConfig> caches,
        Iterable<VisorGgfsConfig> ggfss,
        Iterable<VisorStreamerConfig> streamers,
        VisorDrSenderHubConfig drSenderHub, VisorDrReceiverHubConfig drReceiverHub,
        Map<String, String> env, Properties sysProps) {
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
        this.drSenderHub = drSenderHub;
        this.drReceiverHub = drReceiverHub;
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
    public VisorBasicConfig basic() {
        return basic;
    }

    /**
     * @return Metric.
     */
    public VisorMetricsConfig metrics() {
        return metrics;
    }

    /**
     * @return Spis.
     */
    public VisorSpisConfig spis() {
        return spis;
    }

    /**
     * @return P2P.
     */
    public VisorPeerToPeerConfig p2p() {
        return p2p;
    }

    /**
     * @return Email.
     */
    public VisorEmailConfig email() {
        return email;
    }

    /**
     * @return Lifecycle.
     */
    public VisorLifecycleConfig lifecycle() {
        return lifecycle;
    }

    /**
     * @return Executors.
     */
    public VisorExecServiceConfig executorService() {
        return execSvc;
    }

    /**
     * @return Segmentation.
     */
    public VisorSegmentationConfig seg() {
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
    public VisorRestConfig rest() {
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
    public Iterable<VisorCacheConfig> caches() {
        return caches;
    }

    /**
     * @return Ggfss.
     */
    public Iterable<VisorGgfsConfig> ggfss() {
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
    public VisorDrSenderHubConfig drSenderHub() {
        return drSenderHub;
    }

    /**
     * @return Receiver hub configuration
     */
    public VisorDrReceiverHubConfig drReceiverHub() {
        return drReceiverHub;
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
}
