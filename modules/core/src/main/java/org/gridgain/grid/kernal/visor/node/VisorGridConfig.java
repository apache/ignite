/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor.node;

import org.gridgain.grid.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.visor.cache.*;
import org.gridgain.grid.kernal.visor.streamer.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;
import java.util.*;

import static java.lang.System.*;
import static org.gridgain.grid.kernal.visor.util.VisorTaskUtils.*;

/**
 * Data transfer object for node configuration data.
 */
public class VisorGridConfig implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

//    /** License. */
//    private VisorLicense license;

    /** Basic. */
    private VisorBasicConfig basic;

    /** Metrics. */
    private VisorMetricsConfig metrics;

    /** SPIs. */
    private VisorSpisConfig spis;

    /** P2P. */
    private VisorPeerToPeerConfig p2p;

    /** Email. */
    private VisorEmailConfig email;

    /** Lifecycle. */
    private VisorLifecycleConfig lifecycle;

    /** Executors service configuration. */
    private VisorExecutorServiceConfig execSvc;

    /** Segmentation. */
    private VisorSegmentationConfig seg;

    /** Include properties. */
    private String inclProps;

    /** Include events types. */
    private int[] inclEvtTypes;

    private VisorRestConfig rest;

    /** User attributes. */
    private Map<String, ?> userAttrs;

    /** Caches. */
    private Iterable<VisorCacheConfig> caches;

    /** Ggfss. */
    private Iterable<VisorGgfsConfig> ggfss;

    /** Streamers. */
    private Iterable<VisorStreamerConfig> streamers;

//    /** Sender hub configuration */
//    private VisorDrSenderHubConfig drSenderHub;
//
//    /** Receiver hub configuration */
//    private VisorDrReceiverHubConfig drReceiverHub;

    /** Environment. */
    private Map<String, String> env;

    /** System properties. */
    private Properties sysProps;

    /**
     * @param g Grid.
     * @return Data transfer object for node configuration data.
     */
    public static VisorGridConfig from(GridEx g) {
        assert g != null;

        GridConfiguration c = g.configuration();

        VisorGridConfig cfg = new VisorGridConfig();

//        cfg.license(VisorLicense.from(g));
        cfg.basic(VisorBasicConfig.from(g, c));
        cfg.metrics(VisorMetricsConfig.from(c));
        cfg.spis(VisorSpisConfig.from(c));
        cfg.p2p(VisorPeerToPeerConfig.from(c));
        cfg.email(VisorEmailConfig.from(c));
        cfg.lifecycle(VisorLifecycleConfig.from(c));
        cfg.executeService(VisorExecutorServiceConfig.from(c));
        cfg.segmentation(VisorSegmentationConfig.from(c));
        cfg.includeProperties(compactArray(c.getIncludeProperties()));
        cfg.includeEventTypes(c.getIncludeEventTypes());
        cfg.rest(VisorRestConfig.from(c));
        cfg.userAttributes(c.getUserAttributes());
        cfg.caches(VisorCacheConfig.list(c.getCacheConfiguration()));
        cfg.ggfss(VisorGgfsConfig.list(c.getGgfsConfiguration()));
        cfg.streamers(VisorStreamerConfig.list(c.getStreamerConfiguration()));
//        cfg.drSenderHub(VisorDrSenderHubConfig.from(c.getDrSenderHubConfiguration()));
//        cfg.drReceiverHub(VisorDrReceiverHubConfig.from(c.getDrReceiverHubConfiguration()));
        cfg.env(new HashMap<>(getenv()));
        cfg.systemProperties(getProperties());

        return cfg;
    }

//    /**
//     * @return License.
//     */
//    @Nullable public VisorLicense license() {
//        return license;
//    }
//
//    /**
//     * @param license New license.
//     */
//    public void license(@Nullable VisorLicense license) {
//        this.license = license;
//    }

    /**
     * @return Basic.
     */
    public VisorBasicConfig basic() {
        return basic;
    }

    /**
     * @param basic New basic.
     */
    public void basic(VisorBasicConfig basic) {
        this.basic = basic;
    }

    /**
     * @return Metrics.
     */
    public VisorMetricsConfig metrics() {
        return metrics;
    }

    /**
     * @param metrics New metrics.
     */
    public void metrics(VisorMetricsConfig metrics) {
        this.metrics = metrics;
    }

    /**
     * @return SPIs.
     */
    public VisorSpisConfig spis() {
        return spis;
    }

    /**
     * @param spis New SPIs.
     */
    public void spis(VisorSpisConfig spis) {
        this.spis = spis;
    }

    /**
     * @return P2P.
     */
    public VisorPeerToPeerConfig p2p() {
        return p2p;
    }

    /**
     * @param p2P New p2p.
     */
    public void p2p(VisorPeerToPeerConfig p2P) {
        p2p = p2P;
    }

    /**
     * @return Email.
     */
    public VisorEmailConfig email() {
        return email;
    }

    /**
     * @param email New email.
     */
    public void email(VisorEmailConfig email) {
        this.email = email;
    }

    /**
     * @return Lifecycle.
     */
    public VisorLifecycleConfig lifecycle() {
        return lifecycle;
    }

    /**
     * @param lifecycle New lifecycle.
     */
    public void lifecycle(VisorLifecycleConfig lifecycle) {
        this.lifecycle = lifecycle;
    }

    /**
     * @return Executors service configuration.
     */
    public VisorExecutorServiceConfig executeService() {
        return execSvc;
    }

    /**
     * @param execSvc New executors service configuration.
     */
    public void executeService(VisorExecutorServiceConfig execSvc) {
        this.execSvc = execSvc;
    }

    /**
     * @return Segmentation.
     */
    public VisorSegmentationConfig segmentation() {
        return seg;
    }

    /**
     * @param seg New segmentation.
     */
    public void segmentation(VisorSegmentationConfig seg) {
        this.seg = seg;
    }

    /**
     * @return Include properties.
     */
    public String includeProperties() {
        return inclProps;
    }

    /**
     * @param inclProps New include properties.
     */
    public void includeProperties(String inclProps) {
        this.inclProps = inclProps;
    }

    /**
     * @return Include events types.
     */
    public int[] includeEventTypes() {
        return inclEvtTypes;
    }

    /**
     * @param inclEvtTypes New include events types.
     */
    public void includeEventTypes(int[] inclEvtTypes) {
        this.inclEvtTypes = inclEvtTypes;
    }

    /**
     * @return Rest.
     */
    public VisorRestConfig rest() {
        return rest;
    }

    /**
     * @param rest New rest.
     */
    public void rest(VisorRestConfig rest) {
        this.rest = rest;
    }

    /**
     * @return User attributes.
     */
    public Map<String, ?> userAttributes() {
        return userAttrs;
    }

    /**
     * @param userAttrs New user attributes.
     */
    public void userAttributes(Map<String, ?> userAttrs) {
        this.userAttrs = userAttrs;
    }

    /**
     * @return Caches.
     */
    public Iterable<VisorCacheConfig> caches() {
        return caches;
    }

    /**
     * @param caches New caches.
     */
    public void caches(Iterable<VisorCacheConfig> caches) {
        this.caches = caches;
    }

    /**
     * @return Ggfss.
     */
    public Iterable<VisorGgfsConfig> ggfss() {
        return ggfss;
    }

    /**
     * @param ggfss New ggfss.
     */
    public void ggfss(Iterable<VisorGgfsConfig> ggfss) {
        this.ggfss = ggfss;
    }

    /**
     * @return Streamers.
     */
    public Iterable<VisorStreamerConfig> streamers() {
        return streamers;
    }

    /**
     * @param streamers New streamers.
     */
    public void streamers(Iterable<VisorStreamerConfig> streamers) {
        this.streamers = streamers;
    }

//    /**
//     * @return Sender hub configuration
//     */
//    @Nullable public VisorDrSenderHubConfig drSenderHub() {
//        return drSenderHub;
//    }
//
//    /**
//     * @param drSndHub New sender hub configuration
//     */
//    public void drSenderHub(@Nullable VisorDrSenderHubConfig drSndHub) {
//        drSenderHub = drSndHub;
//    }
//
//    /**
//     * @return Receiver hub configuration
//     */
//    @Nullable public VisorDrReceiverHubConfig drReceiverHub() {
//        return drReceiverHub;
//    }
//
//    /**
//     * @param drReceiverHub New receiver hub configuration
//     */
//    public void drReceiverHub(@Nullable VisorDrReceiverHubConfig drReceiverHub) {
//        this.drReceiverHub = drReceiverHub;
//    }

    /**
     * @return Environment.
     */
    public Map<String, String> env() {
        return env;
    }

    /**
     * @param env New environment.
     */
    public void env(Map<String, String> env) {
        this.env = env;
    }

    /**
     * @return System properties.
     */
    public Properties systemProperties() {
        return sysProps;
    }

    /**
     * @param sysProps New system properties.
     */
    public void systemProperties(Properties sysProps) {
        this.sysProps = sysProps;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorGridConfig.class, this);
    }
}
